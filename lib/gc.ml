let src = Logs.Src.create "gc"

module Log = (val Logs.src_log src : Logs.LOG)
module Set = Set.Make (Int)

(* This garbage collector is a ‘mark-and-sweep’ garbage collector whose
   allocation is protected by a global lock between all writers.
   - [get_free_cell] is the first attempt to allocate a memory area, accessed
     with the protection of our mutex [free_locker]. This function consists of
     reusing unused cells.
   - If there are no unused areas, we ‘sweep’, i.e. we look at the cells that
     have been collected and try [get_free_cell] again.
   - If we still don't have any available cells, we ‘really allocate’ by moving
     the boundary of our segment ([brk]).
   - If we cannot move [brk], we end up in the worst case scenario: having to
     extend (if possible) our segment (which consists of creating a new segment,
     copying the old one and then working on the new one).

   Sweep

   Rowex is based on a fairly simple memory model, where data is created by a
   given task (identified by a number, which simply increments, allowing tasks
   to be ordered from oldest to most recent). According to the Rowex design,
   once allocated, this cell is reachable by the task that allocated it, as
   well as all tasks prior to the one that created the cell.

   Thus, if a cell is marked as free, we must "wait" until the oldest active
   task is more recent than the one that marked the cell. If this is the case,
   then no active task can now reach this cell, and we can therefore consider
   it to be truly free.

   A collected cell is therefore associated with the identifier of the task
   that requested the GC to collect it. Then, during a "sweep" phase, all
   collected cells are scanned and those that meet this predicate are swept:
   [cell.uid < oldest_active_task_uid]. These cells are then added to our [free]
   table (protected by a mutex) to be available when a task wishes to allocate.

   Collect

   The collection occurs when a task wants to delete a cell. This operation is
   very simple and consists of adding the cell to be collected to an (atomic)
   queue. We keep track of the task that wanted to delete this cell, and as
   long as there are tasks that predate it, we keep the cell in our queue. It
   is during a sweep and when the predicate is satisfied that we can consider
   the cell to be truly free.

   Extension of our [brk]

   It may happen that there are no free cells. In this case, we move the
   boundary of our segment in order to allocate a new cell: we move the [brk].
   The format of our rowex file consists of having our [brk] at offset [0], the
   root of our tree at offset [size_of_word], and then our rowex (i.e. at
   offset [size_of_word * 2]).

   Extension of our data-segment

   The worst case scenario is when the [brk] can no longer be moved. In this
   case, we need to extend our segment. The extension is not part of the GC,
   but the latter creates a situation where such an extension can be made.

   When a task cannot allocate, it will set up a "trap" for all tasks (that
   want to write). It will then wait for all these tasks to "release"
   (terminate) or fall into our trap. We can do this thanks to [Clatch]
   (Counted down latch).

   In other words, the extension takes effect once all tasks have finished or
   fallen into the trap:
   - if a new task attempts to appear, it is not counted by our [clatch] (since
     it was created before it appeared) and will wait for the extension
   - if a task ends, it is counted down from the [clatch]
   - it is possible that a task may continue to allocate even if another task
     has set up our trap. This is not a problem because if this task continues,
     it will eventually either finish or want to allocate again (and may have
     fallen into our trap). But in any case, the task that set the trap will
     still be waiting and the extension will still not have started. We say that
     tasks "converge" towards our trap.

   Readers are a special case because they do not allocate. It does not matter
   whether they are working on the new version or the old one. However, they
   are important in determining whether cells can be "swept" or not (which is
   why they are counted when we want to know "the oldest active task").

   Once the writing tasks have fallen into our trap, we can start creating a
   new segment, copy the old one onto the new one (knowing that no tasks can
   write to it at the same time) and rework this new segment.
*)

module Clatch = struct
  type t = {
      mutex : Miou.Mutex.t
    ; condition : Miou.Condition.t
    ; mutable count : int
  }

  let create n =
    {
      mutex = Miou.Mutex.create ()
    ; condition = Miou.Condition.create ()
    ; count = n
    }

  let await t =
    Miou.Mutex.protect t.mutex @@ fun () ->
    while t.count > 0 do
      Miou.Condition.wait t.condition t.mutex
    done

  let count_down t =
    Miou.Mutex.protect t.mutex @@ fun () ->
    t.count <- t.count - 1;
    Miou.Condition.broadcast t.condition
end

type cell = { addr : int; len : int; uid : int }

type 'mem t = {
    queue_locker : Miou.Mutex.t
  ; active_processes : (int * [ `Wr | `Rd ]) Queue.t
  ; released_processes : Set.t ref
  ; clatch : Clatch.t option ref
  ; free_locker : Miou.Mutex.t
  ; free : (int, Set.t) Hashtbl.t
  ; free_cells : int Atomic.t
  ; older_active_process : int Atomic.t
  ; swept_older : int Atomic.t
  ; collected : cell Miou.Queue.t
  ; in_sync : 'mem Miou.Computation.t option ref
  ; extend_and_copy : int -> 'mem -> 'mem * int
  ; mutable memory : 'mem
  ; memory_from_t : 'mem Atomic.t
}

(* NOTE(dinosaure): The use of [ref] here is important because these are values
   that must be shared between tasks. In particular, we need to make a copy of
   [t] for each new task (see [with_memory]). We therefore ensure that we copy
   the pointer to these values rather than the values themselves (and thus,
   they are shared between all our tasks). *)

type uid = int

let make ~extend_and_copy memory_from_t =
  let free_locker = Miou.Mutex.create () in
  let free = Hashtbl.create 0x7ff in
  let free_cells = Atomic.make 0 in
  let older_active_process = Atomic.make 0 in
  let swept_older = Atomic.make min_int in
  let collected = Miou.Queue.create () in
  let queue_locker = Miou.Mutex.create () in
  let active_processes = Queue.create () in
  let released_processes = ref Set.empty in
  let clatch = ref None in
  let in_sync = ref None in
  {
    free_locker
  ; free
  ; free_cells
  ; older_active_process
  ; swept_older
  ; collected
  ; queue_locker
  ; active_processes
  ; released_processes
  ; clatch
  ; in_sync
  ; extend_and_copy
  ; memory = Atomic.get memory_from_t
  ; memory_from_t
  }

let atomic_memory { memory_from_t; _ } = Atomic.get memory_from_t
let memory { memory; _ } = memory
let with_memory t memory = { t with memory }
let null = 0

let unsafe_add_free_cell t writer ~addr ~len =
  Log.debug (fun m ->
      m "[%016x] add a new free cell %016x (%d byte(s))" writer addr len);
  let () =
    try
      let cells = Hashtbl.find t.free len in
      Hashtbl.replace t.free len (Set.add addr cells)
    with Not_found -> Hashtbl.add t.free len (Set.singleton addr)
  in
  ignore (Atomic.fetch_and_add t.free_cells 1)

let get_free_cell writer ~len =
  if Atomic.get writer.free_cells > 0 then
    Miou.Mutex.protect writer.free_locker @@ fun () ->
    match Set.to_list (Hashtbl.find writer.free len) with
    | [ cell ] ->
        ignore (Atomic.fetch_and_add writer.free_cells (-1));
        Hashtbl.remove writer.free len;
        Some cell
    | cell :: cells ->
        ignore (Atomic.fetch_and_add writer.free_cells (-1));
        Hashtbl.replace writer.free len (Set.of_list cells);
        Some cell
    | [] ->
        Hashtbl.remove writer.free len;
        None
    | exception Not_found -> None
  else None

let can_we_sweep_it ~older uid' = older = null || uid' < older

let gen_counter = Atomic.make 1
let gen () = Atomic.fetch_and_add gen_counter 1
let gen_upper_bound () = Atomic.get gen_counter

let collect t writer addr ~len ~uid =
  let addr = Rowex.Addr.unsafe_to_int addr in
  Log.debug (fun m ->
      m "[%016x] collects %016x (%d byte(s)) made by %016x" writer addr len uid);
  (* NOTE(dinosaure): Any process alive when the cell is collected may still hold
     a pointer to it (whatever its uid: a process more recent than the collector
     may have started to walk the tree before the cell was unpublished). So we
     stamp the cell with an upper bound of the uid of every alive process: the
     cell becomes really free (see [can_we_sweep_it]) only once the oldest active
     process is more recent than this stamp - processes appearing after the
     collection can not reach the cell anymore, it was unpublished from the tree
     beforehand. *)
  Miou.Queue.enqueue t.collected { addr; len; uid = gen_upper_bound () }

let sweep t writer =
  let really_sweep () =
    Log.debug (fun m -> m "sweep: %016x start" writer);
    let older =
      Miou.Mutex.protect t.queue_locker @@ fun () ->
      match Queue.peek t.active_processes with
      | older, _ -> older
      | exception Queue.Empty -> null
    in
    let collected = Miou.Queue.(to_list (transfer t.collected)) in
    let free, keep =
      List.fold_left
        (fun (free, keep) ({ addr; len; uid } as cell) ->
          if can_we_sweep_it ~older uid then ((addr, len) :: free, keep)
          else (free, cell :: keep))
        ([], []) collected
    in
    Log.debug (fun m -> m "sweep: keep %d cell(s)" (List.length keep));
    Log.debug (fun m -> m "sweep: free %d cell(s)" (List.length free));
    List.iter (Miou.Queue.enqueue t.collected) keep;
    Miou.Mutex.protect t.free_locker @@ fun () ->
    List.iter (fun (addr, len) -> unsafe_add_free_cell t writer ~addr ~len) free
  in
  let older = Atomic.get t.older_active_process in
  if Atomic.get t.swept_older <> older then begin
    Atomic.set t.swept_older older;
    if Miou.Queue.length t.collected > 0 then really_sweep ()
  end

exception Retry_after_extension

let unsafe_count_active_writers t =
  let rw = !(t.released_processes) in
  let fn acc (uid, k) =
    match k with `Wr when not (Set.mem uid rw) -> acc + 1 | _ -> acc
  in
  Queue.fold fn 0 t.active_processes

let size_of_word = Sys.word_size / 8

external string_unsafe_get_uint32 : string -> int -> int32
  = "%caml_string_get32"

module type S = sig
  type memory

  val length : memory -> int
  val atomic_fetch_add_leuintnat : memory -> int -> int -> int
  val atomic_set_leuintnat : memory -> int -> int -> unit
  val set_int32 : memory -> int -> int32 -> unit
  val set_uint8 : memory -> int -> int -> unit
end

module Make (C : S) = struct
  type memory = C.memory

  (* XXX(dinosaure): An important note is that the [t.memory] used is always
     the correct one (even if it is not atomic) for readers and writers. It may
     happen that [t.memory] for readers continues to refer to the old memory
     area (the reader takes the memory area, a writer appears and attempts to
     extend the memory area: in this case, our reader still refers to the old
     area), but we always refer to the latest memory area (regardless of
     extensions) for writers. *)

  let rec blitv payloads memory dst_off =
    match payloads with
    | hd :: tl ->
        let len = String.length hd in
        let len0 = len land 3 in
        let len1 = len asr 2 in
        for i = 0 to len1 - 1 do
          let i = i * 4 in
          let v = string_unsafe_get_uint32 hd i in
          C.set_int32 memory (dst_off + i) v
        done;
        for i = 0 to len0 - 1 do
          let i = (len1 * 4) + i in
          C.set_uint8 memory (dst_off + i) (Char.code hd.[i])
        done;
        blitv tl memory (dst_off + len)
    | [] -> ()

  let really_alloc t writer ~kind len payloads =
    let len = (len + (size_of_word - 1)) / size_of_word * size_of_word in
    Log.debug (fun m -> m "[%016x] try to allocate %d byte(s)" writer len);
    let old_brk = C.atomic_fetch_add_leuintnat t.memory 0 len in
    if old_brk + len <= C.length t.memory then begin
      let addr = old_brk in
      Log.debug (fun m ->
          m "brk: %016x (owner: [%016x]) => %016x" addr writer (old_brk + len));
      blitv payloads t.memory addr;
      if kind = `Node then
        C.atomic_set_leuintnat t.memory (addr + Rowex._header_owner) writer;
      Rowex.Addr.of_int_to_rdwr addr
    end
    else begin
      C.atomic_set_leuintnat t.memory 0 old_brk;
      (* NOTE(dinosaure): we must replace [brk] to be sure that a next usage
         of our rowex file will not fail with a SIGSEGV (because the current
         [brk] farther than expected. *)
      (* NOTE(dinosaure): the idea here is to "trap" our writers in this part of
         the code. if, by mistake, one has finished in the meantime, it will
         "count_down" (see [release_writer]) itself but will not participate in
         the extension. a writer will create the "clatch" and wait for all the
         others to fall into the trap as well. then, we will create an ivar and
         our first writer will perform the extension while the others wait for
         the result of this extension. *)
      Log.debug (fun m -> m "start to extend our rowex file");
      Miou.Mutex.lock t.queue_locker;
      match !(t.clatch) with
      | None ->
          let active_writers = unsafe_count_active_writers t in
          assert (active_writers >= 1);
          let clatch = Clatch.create (active_writers - 1) in
          let in_sync = Miou.Computation.create () in
          t.clatch := Some clatch;
          t.in_sync := Some in_sync;
          Log.debug (fun m ->
              m "lucky you are %016x, start to wait %d writer(s)" writer
                active_writers);
          Miou.Mutex.unlock t.queue_locker;
          Clatch.await clatch;
          t.clatch := None;
          let new_memory, new_size = t.extend_and_copy writer t.memory in
          Log.debug (fun m -> m "new memory (new size: %d byte(s))" new_size);
          t.memory <- new_memory;
          Atomic.set t.memory_from_t new_memory;
          assert (Miou.Computation.try_return in_sync new_memory);
          (* TODO(dinosaure): should we clean-up [t.in_sync]? I would like to
             say yes but I'm not sure. *)
          raise Retry_after_extension
      | Some clatch ->
          Log.debug (fun m -> m "%016x trapped" writer);
          Miou.Mutex.unlock t.queue_locker;
          let in_sync = Option.get !(t.in_sync) in
          Clatch.count_down clatch;
          Clatch.await clatch;
          let new_memory = Miou.Computation.await_exn in_sync in
          t.memory <- new_memory;
          raise Retry_after_extension
    end

  let alloc t ~writer ~kind len payloads =
    (* The subtlety here is that if one writer falls into the file extension
       case, another writer may want to allocate at the same time. There are
       then two possible scenarios:
       - the case where this new writer attempts to allocate smaller data and
         finds an empty cell; in this case, we continue to work on the "old"
         memory area and the extension only takes effect when ALL writers have
         fallen into the trap
       - the case where this new writer does not find a free cell and therefore
         falls into the trap.

       In other words, we can continue to work on the old memory area (before
       its extension) even if a writer requests the extension at the same time.
       We will reach a point where all writers will be unable to allocate and
       will fall into the trap of the first writer (and all allocations, even
       those made while our first writer waits for all the others to fall into
       the trap, will be effective during the extension). *)
    match get_free_cell t ~len with
    | Some addr ->
        blitv payloads t.memory addr;
        if kind = `Node then
          C.atomic_set_leuintnat t.memory (addr + Rowex._header_owner) writer;
        Rowex.Addr.of_int_to_rdwr addr
    | None -> begin
        ignore (sweep t writer);
        match get_free_cell t ~len with
        | None -> begin
            try really_alloc t writer ~kind len payloads
            with Retry_after_extension ->
              Log.debug (fun m -> m "retry an allocation");
              really_alloc t writer ~kind len payloads
          end
        | Some addr ->
            Log.debug (fun m ->
                m "re-use(2) %016x (owner: [%016x])" addr writer);
            blitv payloads t.memory addr;
            if kind = `Node then
              C.atomic_set_leuintnat t.memory
                (addr + Rowex._header_owner)
                writer;
            Rowex.Addr.of_int_to_rdwr addr
      end

  (* the goal here is to update [t.older_active_writer] to the one we get from
   [t.active_writers]. We **really try** to be synchrone between the last
   [t.active_writers] and [t.older_active_writer]. *)
  let rec update_older_active_process ?(backoff = Miou.Backoff.default) ?older t
      =
    let older =
      match older with
      | Some older -> older
      | None -> begin
          Miou.Mutex.protect t.queue_locker @@ fun () ->
          match Queue.peek t.active_processes with
          | older, _ -> older
          | exception Queue.Empty -> 0
        end
    in
    let seen = Atomic.get t.older_active_process in
    if
      seen <> older
      && not (Atomic.compare_and_set t.older_active_process seen older)
    then update_older_active_process ~backoff:(Miou.Backoff.once backoff) t

  let gen = gen

  let add_process t kind =
    let uid = gen () in
    Log.debug (fun m -> m "new writer %016x" uid);
    let set = Atomic.compare_and_set t.older_active_process 0 uid in
    if not set then begin
      let older, _ =
        Miou.Mutex.protect t.queue_locker @@ fun () ->
        (* Here, if a writer attempts to extend the area, and our new writer is
           not counted in our trap, but we should have our [in_sync] notifying
           everyone that the extension has finished and that [t.memory] is
           indeed the new memory area. We therefore wait for the extension to
           finish here before launching our new writer. *)
        let () =
          match (kind, !(t.clatch)) with
          | `Wr, Some _ ->
              ignore (Miou.Computation.await_exn (Option.get !(t.in_sync)))
          | _ -> ()
        in
        Queue.push (uid, kind) t.active_processes;
        (* here, we take the previous writer before the apparition of our new one. *)
        Queue.peek t.active_processes
      in
      update_older_active_process ~older t;
      uid
    end
    else
      Miou.Mutex.protect t.queue_locker @@ fun () ->
      Queue.push (uid, kind) t.active_processes;
      uid

  let rec unsafe_clean_released_processes t =
    let rw = !(t.released_processes) in
    if Set.is_empty rw = false then
      match Queue.peek t.active_processes with
      | older, _ ->
          if Set.mem older rw then begin
            t.released_processes := Set.remove older rw;
            ignore (Queue.pop t.active_processes);
            unsafe_clean_released_processes t
          end
      | exception Queue.Empty -> ()

  let release_process t kind ~uid =
    let older =
      Miou.Mutex.protect t.queue_locker @@ fun () ->
      Log.debug (fun m -> m "release writer %016x" uid);
      (* here, if our writer has finished but another writer tries to extend the
         file, we count down to prevent the other writer from waiting for us
         indefinitely! *)
      let () =
        match (kind, !(t.clatch)) with
        | `Wr, Some clatch ->
            Log.debug (fun m -> m "writer %016x unlock our extension" uid);
            Clatch.count_down clatch
        | _ -> ()
      in
      match Queue.peek t.active_processes with
      | older, _ ->
          if uid = older then begin
            assert (fst (Queue.pop t.active_processes) = uid);
            (* here, we possibly have few writers ahead our writer [uid]. they
               must have ended before us. *)
            Log.debug (fun m -> m "clean possible released writers");
            unsafe_clean_released_processes t;
            let older = Queue.peek_opt t.active_processes in
            let older = Option.map fst older in
            Option.value ~default:0 older
          end
          else begin
            Log.debug (fun m ->
                m "it exists an older active writer (%016x) than %016x" older
                  uid);
            let rw = !(t.released_processes) in
            let rw = Set.add uid rw in
            t.released_processes := rw;
            older
          end
      | exception Queue.Empty ->
          Log.err (fun m -> m "we missed writer %016x" uid);
          assert false
    in
    update_older_active_process ~older t

  let collect = collect

  let delete t (addr : 'a Rowex.Addr.t) len =
    Miou.Mutex.protect t.free_locker @@ fun () ->
    unsafe_add_free_cell t 0 ~addr:(Rowex.Addr.unsafe_to_int addr) ~len

  let unsafe_delete t (addr : 'a Rowex.Addr.t) len =
    unsafe_add_free_cell t 0 ~addr:(Rowex.Addr.unsafe_to_int addr) ~len
end
