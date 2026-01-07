let src = Logs.Src.create "part"

module Log = (val Logs.src_log src : Logs.LOG)

type memory =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

type cell = { addr : int; len : int; uid : int }

module Set = Set.Make (Int)

module C = struct
  external persist : memory -> int -> int -> unit = "caml_persist" [@@noalloc]

  external atomic_get_uint8 : memory -> int -> int = "caml_atomic_get_uint8"
  [@@noalloc]

  external atomic_set_uint8 : memory -> int -> int -> unit
    = "caml_atomic_set_uint8"
  [@@noalloc]

  external atomic_get_leuintnat : memory -> int -> int
    = "caml_atomic_get_leuintnat"
  [@@noalloc]

  external atomic_set_leuintnat : memory -> int -> int -> unit
    = "caml_atomic_set_leuintnat"
  [@@noalloc]

  external atomic_get_leuint16 : memory -> int -> int
    = "caml_atomic_get_leuint16"
  [@@noalloc]

  external atomic_set_leuint16 : memory -> int -> int -> unit
    = "caml_atomic_set_leuint16"
  [@@noalloc]

  external atomic_get_leuint31 : memory -> int -> int
    = "caml_atomic_get_leuint31"
  [@@noalloc]

  external atomic_set_leuint31 : memory -> int -> int -> unit
    = "caml_atomic_set_leuint31"
  [@@noalloc]

  external atomic_get_leuint64 : memory -> int -> (int64[@unboxed])
    = "bytecode_compilation_not_supported" "caml_atomic_get_leuint64"
  [@@noalloc]

  external atomic_set_leuint64 : memory -> int -> (int64[@unboxed]) -> unit
    = "bytecode_compilation_not_supported" "caml_atomic_set_leuint64"
  [@@noalloc]

  external atomic_get_leuint128 : memory -> int -> bytes -> unit
    = "caml_atomic_get_leuint128"
  [@@noalloc]

  external atomic_fetch_add_leuint16 : memory -> int -> int -> int
    = "caml_atomic_fetch_add_leuint16"
  [@@noalloc]

  external atomic_fetch_add_leuintnat : memory -> int -> int -> int
    = "caml_atomic_fetch_add_leuintnat"
  [@@noalloc]

  external atomic_fetch_sub_leuintnat : memory -> int -> int -> int
    = "caml_atomic_fetch_sub_leuintnat"
  [@@noalloc]

  external atomic_fetch_sub_leuint16 : memory -> int -> int -> int
    = "caml_atomic_fetch_sub_leuint16"
  [@@noalloc]

  external atomic_fetch_or_leuintnat : memory -> int -> int -> int
    = "caml_atomic_fetch_or_leuintnat"
  [@@noalloc]

  external pause_intrinsic : unit -> unit = "caml_pause_intrinsic" [@@noalloc]

  external atomic_compare_exchange_strong :
    memory -> int -> int Atomic.t -> int -> bool
    = "caml_atomic_compare_exchange_strong_leuintnat"
  [@@noalloc]

  external atomic_compare_exchange_weak :
    memory -> int -> int Atomic.t -> int -> bool
    = "caml_atomic_compare_exchange_weak_leuintnat"
  [@@noalloc]

  external get_ocaml_string : memory -> int -> string = "caml_get_ocaml_string"

  external get_ocaml_string_length : memory -> int -> int
    = "caml_get_ocaml_string_length"
  [@@noalloc]

  external get_leint31 : memory -> int -> int = "caml_get_leint31" [@@noalloc]
  external get_leintnat : memory -> int -> int = "caml_get_leintnat" [@@noalloc]

  external set_n48_key : memory -> int -> int -> int -> unit
    = "caml_set_n48_key"
  [@@noalloc]

  external movnt64 : memory -> int -> int -> unit = "caml_movnt64" [@@noalloc]
  external msync : memory -> unit = "caml_msync" [@@noalloc]
end

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

(* we have 3 locks:
   1) [queue_locker] for [active_writers], [released_writers] and [clatch].
      it operates when we create/remove a writer and when one writer try to
      extend the rowex file
   2) [free_locker] for [free]. it operates when we wants to get a new free cell
      or when we would like to add free cells. we prefetch [free_cells] (which
      is an atomic, so it's safe to use it across domains) to see if we need to
      lock/get a new free cell/unlock (this last operation has a cost). finally,
      [collected] is a safe shareable queue across domains. the idea is to share
      collected cell and do the [sweep] computation (to know if a collected
      cell can be a free cell) without [free_locker]. after this computation,
      we lock and add all free cells into [free]
   3) [extend_locker] for [extend_result]. it operates when we would like to
      extend the rowex file: so it's a particular situation where all writers
      are trapped into a certain branch of our code and one of them do the
      extensions and the others are waiting

  A member of [t] is the memory we want to work on. This memory is accessible
  atomically in the event that one (and only one) writer wants to extend the
  file (and, in this case, modify the memory with its new, larger version).

  Thus, writers can modify the [t.memory] field. Writers also have a
  [writer.memory] field that is not atomic because they change it themselves
  (and no one else can change it).

  There remains the case of readers, which is a field that is neither atomic
  nor mutable. Readers can still refer to the old version of the file even if
  it has been updated, but this is not a problem, and when a new reader appears,
  it will take the [t.memory] (which has just been modified by one of the
  writers).

  In short, logically:
  - writers have their own "memory" field and do not need anyone else to modify
    it except themselves in the event of an extension
  - the main value [t] has a memory field that one of the writers can change if
    there has been an extension, but [t] only uses it to give memory to the
    readers
  - readers do not need to update themselves; if they have the old version, the
    user should create new ones
*)
type t = {
    filepath : string
  ; memory : memory Atomic.t
  ; root : Rowex.rdwr Rowex.Addr.t
  ; queue_locker : Miou.Mutex.t
  ; active_processes : (int * [ `Wr | `Rd ]) Queue.t
  ; released_processes : Set.t ref
  ; clatch : Clatch.t option ref
  ; free_locker : Miou.Mutex.t
  ; free : (int, Set.t) Hashtbl.t
  ; free_cells : int Atomic.t
  ; older_active_process : int Atomic.t
  ; collected : cell Miou.Queue.t
  ; extend_locker : Miou.Mutex.t
  ; extend_result : memory Miou.Computation.t option ref
}

type reader = { memory : memory; root : Rowex.ro Rowex.Addr.t }

type writer = {
    filepath : string
  ; free_locker : Miou.Mutex.t
  ; free : (int, Set.t) Hashtbl.t
  ; free_cells : int Atomic.t
  ; older_active_process : int Atomic.t
  ; queue_locker : Miou.Mutex.t
  ; clatch : Clatch.t option ref
  ; active_processes : (int * [ `Wr | `Rd ]) Queue.t
  ; released_processes : Set.t ref
  ; collected : cell Miou.Queue.t
  ; extend_locker : Miou.Mutex.t
  ; extend_result : memory Miou.Computation.t option ref
  ; mutable memory : memory
  ; memory_from_t : memory Atomic.t
  ; uid : int
  ; root : Rowex.rdwr Rowex.Addr.t
}

let size_of_word = Sys.word_size / 8

module System = struct
  let load_memory ?len filepath =
    let fd = Unix.openfile filepath Unix.[ O_RDWR; O_DSYNC ] 0o644 in
    let finally () = Unix.close fd in
    Fun.protect ~finally @@ fun () ->
    let open Unix in
    let open Bigarray in
    let len = match len with Some len -> len | None -> (fstat fd).st_size in
    let memory = Unix.map_file fd ~pos:0L char c_layout true [| len |] in
    Bigarray.array1_of_genarray memory

  let into_new_file ?(mode = 0o644) ?size filepath src =
    let fd =
      Unix.openfile filepath Unix.[ O_RDWR; O_CREAT; O_DSYNC; O_APPEND ] mode
    in
    (* 64KiB *)
    let tmp = Bytes.create 0x10000 in
    let rec go written =
      match Unix.read src tmp 0 (Bytes.length tmp) with
      | 0 -> written
      | len ->
          let str = Bytes.unsafe_to_string tmp in
          let len = Unix.write_substring fd str 0 len in
          go (written + len)
    in
    let finally () = Unix.close fd in
    Fun.protect ~finally @@ fun () ->
    Log.debug (fun m -> m "copy our rowex file into %s" filepath);
    match (go 0, size) with
    | _written, None -> ()
    | written, Some size ->
        Log.debug (fun m ->
            m "%d byte(s) written (size: %d byte(s))" written size);
        if written < size then Unix.ftruncate fd size

  let prng = Stdlib.Domain.DLS.new_key Random.State.make_self_init

  let temp =
    Stdlib.Domain.DLS.new_key ~split_from_parent:Fun.id @@ fun () ->
    match Sys.getenv "PART_TMP" with
    | value when Sys.file_exists value && Sys.is_directory value -> value
    | _ | (exception _) -> "/tmp"

  let generate_filepath pattern =
    let g = Domain.DLS.get prng in
    let v = Random.State.bits g land 0xffffff in
    let filename = Fmt.str pattern (Fmt.str "%06x" v) in
    Filename.concat (Stdlib.Domain.DLS.get temp) filename

  let copy_into_larger_filepath (writer : writer) =
    let new_filepath =
      let rec go retries =
        if retries >= 10 then failwith "Impossible to create a new rowex file";
        let v = generate_filepath "rowex-%s.idx" in
        if Sys.file_exists v then go (succ retries) else v
      in
      go 0
    in
    (* we add 1MiB *)
    let new_size = Bigarray.Array1.dim writer.memory + 1048576 in
    let fd = Unix.openfile writer.filepath Unix.[ O_RDONLY ] 0o644 in
    let finally () = Unix.close fd in
    Fun.protect ~finally @@ fun () ->
    (* please note that this part cannot be used if there is only one active
       writer (see the worst branch of [really_alloc]). The advantage is that it
       truly synchronizes writes so that our entire file can then be copied
       cleanly to another destination. readers can continue to run normally. *)
    C.msync writer.memory;
    into_new_file ~size:new_size new_filepath fd;
    (new_filepath, new_size)
end

external bigarray_unsafe_set_uint8 : memory -> int -> int -> unit
  = "%caml_ba_set_1"

external bigarray_unsafe_set_uint32 : memory -> int -> int32 -> unit
  = "%caml_bigstring_set32"

external string_unsafe_get_uint32 : string -> int -> int32
  = "%caml_string_get32"

let rec blitv payloads memory dst_off =
  match payloads with
  | hd :: tl ->
      let len = String.length hd in
      let len0 = len land 3 in
      let len1 = len asr 2 in
      for i = 0 to len1 - 1 do
        let i = i * 4 in
        let v = string_unsafe_get_uint32 hd i in
        bigarray_unsafe_set_uint32 memory (dst_off + i) v
      done;
      for i = 0 to len0 - 1 do
        let i = (len1 * 4) + i in
        bigarray_unsafe_set_uint8 memory (dst_off + i) (Char.code hd.[i])
      done;
      blitv tl memory (dst_off + len)
  | [] -> ()

let size_of_node = function
  | 0 -> Rowex._sizeof_n4
  | 1 -> Rowex._sizeof_n16
  | 2 -> Rowex._sizeof_n48
  | 3 -> Rowex._sizeof_n256
  | _ -> assert false

module Garbage_collector = struct
  let src = Logs.Src.create "part.gc"

  module Log = (val Logs.src_log src : Logs.LOG)

  let gen =
    let v = Atomic.make 1 in
    fun () -> Atomic.fetch_and_add v 1

  let unsafe_add_free_cell writer ~addr ~len =
    Log.debug (fun m -> m "Add a new free cell %016x (%d byte(s))" addr len);
    let () =
      try
        let cells = Hashtbl.find writer.free len in
        Hashtbl.replace writer.free len (Set.add addr cells)
      with Not_found -> Hashtbl.add writer.free len (Set.singleton addr)
    in
    ignore (Atomic.fetch_and_add writer.free_cells 1)

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

  let can_we_sweep_it writer uid' =
    let older_active_process =
      Atomic.get (Sys.opaque_identity writer.older_active_process)
    in
    older_active_process = 0 || uid' < older_active_process

  let collect writer addr ~len ~uid =
    let addr = Rowex.Addr.unsafe_to_int addr in
    Log.debug (fun m ->
        m "[%016x] collect %016x (%d byte(s)) made by %016x & owned by %016x"
          writer.uid addr len uid writer.uid);
    Miou.Queue.enqueue writer.collected { addr; len; uid = writer.uid }

  let sweep writer =
    let really_sweep () =
      Log.debug (fun m -> m "sweep: %016x start" writer.uid);
      let collected = Miou.Queue.(to_list (transfer writer.collected)) in
      let free, keep =
        List.fold_left
          (fun (free, keep) ({ addr; len; uid } as cell) ->
            if can_we_sweep_it writer uid then ((addr, len) :: free, keep)
            else (free, cell :: keep))
          ([], []) collected
      in
      Log.debug (fun m -> m "sweep: keep %d cell(s)" (List.length keep));
      Log.debug (fun m -> m "sweep: free %d cell(s)" (List.length free));
      List.iter (Miou.Queue.enqueue writer.collected) keep;
      Miou.Mutex.protect writer.free_locker @@ fun () ->
      List.iter (fun (addr, len) -> unsafe_add_free_cell writer ~addr ~len) free
    in
    if Miou.Queue.length writer.collected > 0 then really_sweep ()

  exception Retry_after_extension

  let unsafe_count_active_writers writer =
    let rw = !(writer.released_processes) in
    let fn acc (uid, k) =
      match k with `Wr when not (Set.mem uid rw) -> acc + 1 | _ -> acc
    in
    Queue.fold fn 0 writer.active_processes

  let really_alloc writer ~kind len payloads =
    let memory = writer.memory in
    let len = (len + (size_of_word - 1)) / size_of_word * size_of_word in
    Log.debug (fun m -> m "try to allocate %d byte(s)" len);
    let old_brk = C.atomic_fetch_add_leuintnat memory 0 len in
    if old_brk + len <= Bigarray.Array1.dim memory then begin
      let addr = old_brk in
      Log.debug (fun m -> m "brk: %016x => %016x" addr (old_brk + len));
      blitv payloads memory addr;
      if kind = `Node then
        C.atomic_set_leuintnat memory (addr + Rowex._header_owner) writer.uid;
      Rowex.Addr.of_int_to_rdwr addr
    end
    else begin
      C.atomic_set_leuintnat memory 0 old_brk;
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
      Miou.Mutex.lock writer.queue_locker;
      match !(writer.clatch) with
      | None ->
          let active_writers = unsafe_count_active_writers writer in
          assert (active_writers >= 1);
          let clatch = Clatch.create (active_writers - 1) in
          writer.clatch := Some clatch;
          Miou.Mutex.unlock writer.queue_locker;
          Log.debug (fun m ->
              m "lucky you are %016x, start to wait %d writer(s)" writer.uid
                active_writers);
          Clatch.await clatch;
          writer.clatch := None;
          let result = Miou.Computation.create () in
          Miou.Mutex.protect writer.extend_locker (fun () ->
              writer.extend_result := Some result);
          let new_filepath, _new_size =
            System.copy_into_larger_filepath writer
          in
          Unix.rename new_filepath writer.filepath;
          let memory = System.load_memory writer.filepath in
          writer.memory <- memory;
          Atomic.set writer.memory_from_t memory;
          assert (Miou.Computation.try_return result memory);
          Miou.Mutex.protect writer.extend_locker (fun () ->
              writer.extend_result := None);
          raise Retry_after_extension
      | Some clatch ->
          Miou.Mutex.unlock writer.queue_locker;
          Log.debug (fun m -> m "writer %016x trapped" writer.uid);
          Clatch.count_down clatch;
          Clatch.await clatch;
          let rec gimme_extend_ivar () =
            let result =
              Miou.Mutex.protect writer.extend_locker @@ fun () ->
              !(writer.extend_result)
            in
            if Option.is_none result then gimme_extend_ivar ()
            else Option.get result
          in
          let result = gimme_extend_ivar () in
          let memory = Miou.Computation.await_exn result in
          writer.memory <- memory;
          raise Retry_after_extension
    end

  let alloc writer ~kind len payloads =
    match get_free_cell writer ~len with
    | Some addr ->
        let memory = writer.memory in
        blitv payloads memory addr;
        if kind = `Node then
          C.atomic_set_leuintnat memory (addr + Rowex._header_owner) writer.uid;
        Rowex.Addr.of_int_to_rdwr addr
    | None -> begin
        ignore (sweep writer);
        match get_free_cell writer ~len with
        | None -> (
            try really_alloc writer ~kind len payloads
            with Retry_after_extension ->
              Log.debug (fun m -> m "retry an allocation");
              really_alloc writer ~kind len payloads)
        | Some addr ->
            let memory = writer.memory in
            blitv payloads memory addr;
            if kind = `Node then
              C.atomic_set_leuintnat memory
                (addr + Rowex._header_owner)
                writer.uid;
            Rowex.Addr.of_int_to_rdwr addr
      end
end

(* NOTE(dinosaure): I think it's the same implementation than [unsafe_add_free_cell]... *)
let unsafe_add_free_cell (rowex : t) ~addr ~len =
  Log.debug (fun m -> m "Add a new free cell %016x (%d byte(s))" addr len);
  let () =
    try
      let cells = Hashtbl.find rowex.free len in
      Hashtbl.replace rowex.free len (Set.add addr cells)
    with Not_found -> Hashtbl.add rowex.free len (Set.singleton addr)
  in
  ignore (Atomic.fetch_and_add rowex.free_cells 1)

let scan (rowex : t) =
  let memory = Atomic.get rowex.memory in
  let brk = C.atomic_get_leuintnat memory 0 in
  if brk > Bigarray.Array1.dim memory then
    Fmt.invalid_arg "The given ROWEX file is smaller than it says";
  let cur = ref (C.atomic_get_leuintnat memory size_of_word) in
  let collected = ref 0 in
  Log.debug (fun m -> m "scan: start (brk: %016x)" brk);
  while !cur < brk do
    Log.debug (fun m -> m "scan: %016x" !cur);
    let hdr = C.atomic_get_leuintnat memory !cur in
    match hdr lsr Rowex._bits_kind with
    | (0 | 1 | 2 | 3) as v ->
        let len = size_of_node v in
        if hdr land 1 = 1 then begin
          unsafe_add_free_cell rowex ~addr:!cur ~len;
          incr collected
        end;
        cur := !cur + len
    | 5 ->
        let len_w =
          if Sys.word_size == 64 then hdr land 0xfffffffffffffff
          else hdr land 0xfffffff
        in
        let len = len_w * size_of_word in
        cur := !cur + len
    | _ -> Fmt.failwith "Invalid ROWEX file, bad cell at %016x" !cur
  done;
  Log.debug (fun m -> m "%d cell(s) collected" !collected)

module Reader = struct
  type memory = reader
  type 'a t = 'a

  let bind x f = f x
  let return x = x

  open Rowex

  let get : type k v. memory -> 'a rd Addr.t -> (k, v) value -> v t =
   fun { memory; _ } addr v ->
    Log.debug (fun m ->
        m "get        %016x : %a" (Addr.unsafe_to_int addr) pp_value v);
    match v with
    | OCaml_string -> C.get_ocaml_string memory (Addr.unsafe_to_int addr)
    | OCaml_string_length ->
        C.get_ocaml_string_length memory (Addr.unsafe_to_int addr)
    | LEInt31 -> C.get_leint31 memory (Addr.unsafe_to_int addr)
    | LEInt -> C.get_leintnat memory (Addr.unsafe_to_int addr)
    | _ -> assert false

  let atomic_get : type v. memory -> 'a rd Addr.t -> (atomic, v) value -> v t =
   fun { memory; _ } addr k ->
    Log.debug (fun m ->
        m "atomic_get %016x : %a" (Addr.unsafe_to_int addr) pp_value k);
    match k with
    | Int8 -> C.atomic_get_uint8 memory (Addr.unsafe_to_int addr)
    | LEInt -> C.atomic_get_leuintnat memory (Addr.unsafe_to_int addr)
    | LEInt16 -> C.atomic_get_leuint16 memory (Addr.unsafe_to_int addr)
    | LEInt31 -> C.atomic_get_leuint31 memory (Addr.unsafe_to_int addr)
    | LEInt64 -> C.atomic_get_leuint64 memory (Addr.unsafe_to_int addr)
    | LEInt128 ->
        let res = Bytes.create 16 in
        C.atomic_get_leuint128 memory (Addr.unsafe_to_int addr) res;
        Bytes.unsafe_to_string res
    | Addr_rd ->
        Addr.of_int_to_rdonly
          (C.atomic_get_leuintnat memory (Addr.unsafe_to_int addr))
    | Addr_rdwr ->
        Addr.of_int_to_rdwr
          (C.atomic_get_leuintnat memory (Addr.unsafe_to_int addr))

  let atomic_set : type v.
      memory -> 'a wr Addr.t -> (atomic, v) value -> v -> unit t =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<atomic_set>)"

  let fetch_add : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<fetch_add>)"

  let fetch_sub : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<fetch_sub>)"

  let fetch_or : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<fetch_or>)"

  let compare_exchange : type a.
         memory
      -> ?weak:bool
      -> rdwr Addr.t
      -> (atomic, a) value
      -> a Atomic.t
      -> a
      -> bool t =
   fun _ ?weak:_ _ _ _ _ ->
    Fmt.failwith "Invalid reader operation (<compare_exchange>)"

  let persist : memory -> 'c wr Addr.t -> len:int -> unit t =
   fun _ _ ~len:_ -> Fmt.failwith "Invalid reader operation (<persist>)"

  let set_n48_key : memory -> 'c wr Addr.t -> int -> int -> unit t =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<set_n48_key>)"

  let movnt64 : memory -> dst:'c wr Addr.t -> int -> unit t =
   fun _ ~dst:_ _ -> Fmt.failwith "Invalid reader operation (<movnt64)"

  let allocate :
         memory
      -> kind:[ `Leaf | `Node ]
      -> ?len:int
      -> string list
      -> rdwr Addr.t t =
   fun _ ~kind:_ ?len:_ _ -> Fmt.failwith "Invalid reader operation (allocate)"

  let delete : memory -> 'a Addr.t -> int -> unit t =
   fun _ _ _ -> Fmt.failwith "Invalid reader operation (<delete>)"

  let collect : memory -> 'a Addr.t -> len:int -> uid:int -> unit t =
   fun _ _ ~len:_ ~uid:_ -> Fmt.failwith "Invalid reader operation (<collect>)"

  let pause_intrinsic () = C.pause_intrinsic ()
end

module Writer = struct
  type memory = writer
  type 'a t = 'a

  let bind x f = f x
  let return x = x

  open Rowex

  let to_reader writer : reader =
    { memory = writer.memory; root = Addr.to_rdonly writer.root }

  let get : type k v. memory -> 'a rd Addr.t -> (k, v) value -> v t =
   fun t addr k -> Reader.get (to_reader t) addr k

  let atomic_get : type v. memory -> 'a rd Addr.t -> (atomic, v) value -> v t =
   fun t addr k -> Reader.atomic_get (to_reader t) addr k

  let atomic_set : type v.
      memory -> 'a wr Addr.t -> (atomic, v) value -> v -> unit t =
   fun { memory; _ } addr k v ->
    Log.debug (fun m ->
        m "atomic_set %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    match k with
    | Int8 -> C.atomic_set_uint8 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_set_leuintnat memory (Addr.unsafe_to_int addr) v
    | LEInt16 -> C.atomic_set_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt31 -> C.atomic_set_leuint31 memory (Addr.unsafe_to_int addr) v
    | LEInt64 -> C.atomic_set_leuint64 memory (Addr.unsafe_to_int addr) v
    | Addr_rd ->
        C.atomic_set_leuintnat memory (Addr.unsafe_to_int addr)
          (Addr.unsafe_to_int v)
    | Addr_rdwr ->
        C.atomic_set_leuintnat memory (Addr.unsafe_to_int addr)
          (Addr.unsafe_to_int v)
    | _ -> assert false

  let fetch_add : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun { memory; _ } addr k v ->
    Log.debug (fun m ->
        m "fetch_add  %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    match k with
    | LEInt16 -> C.atomic_fetch_add_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_fetch_add_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let fetch_sub : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun { memory; _ } addr k v ->
    Log.debug (fun m ->
        m "fetch_sub  %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    match k with
    | LEInt16 -> C.atomic_fetch_sub_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_fetch_sub_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let fetch_or : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t =
   fun { memory; _ } addr k v ->
    Log.debug (fun m ->
        m "fetch_or   %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    match k with
    | LEInt -> C.atomic_fetch_or_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let compare_exchange : type a.
         memory
      -> ?weak:bool
      -> rdwr Addr.t
      -> (atomic, a) value
      -> a Atomic.t
      -> a
      -> bool t =
   fun { memory; _ } ?(weak = false) addr k expected desired ->
    Log.debug (fun m ->
        m "compare_exchange weak:%b %016x (%a : %a) (%a : %a)" weak
          (Addr.unsafe_to_int addr)
          (pp_of_value ~prefer_hex:true k)
          (Atomic.get expected) pp_value k
          (pp_of_value ~prefer_hex:true k)
          desired pp_value k);
    match (k, weak) with
    | LEInt, true ->
        C.atomic_compare_exchange_weak memory (Addr.unsafe_to_int addr) expected
          desired
    | LEInt, false ->
        C.atomic_compare_exchange_strong memory (Addr.unsafe_to_int addr)
          expected desired
    | _ -> assert false

  let persist { memory; _ } (addr : 'c wr Addr.t) ~len =
    Log.debug (fun m -> m "persist    %016x (%d)" (Addr.unsafe_to_int addr) len);
    C.persist memory (Addr.unsafe_to_int addr) len

  let set_n48_key { memory; _ } (addr : 'c wr Addr.t) k c =
    C.set_n48_key memory (Addr.unsafe_to_int addr) k c

  let movnt64 { memory; _ } ~(dst : 'c wr Addr.t) src =
    C.movnt64 memory (Addr.unsafe_to_int dst) src

  let allocate t ~kind ?len payloads =
    let len =
      match len with
      | Some len -> len
      | None -> List.fold_left (fun a str -> a + String.length str) 0 payloads
    in
    Log.debug (fun m -> m "alloctate %3d" len);
    Garbage_collector.alloc t ~kind len payloads

  let delete t (addr : 'a Addr.t) len =
    Log.debug (fun m -> m "delete     %016x %d" (Addr.unsafe_to_int addr) len);
    Miou.Mutex.protect t.free_locker @@ fun () ->
    Garbage_collector.unsafe_add_free_cell t ~addr:(Addr.unsafe_to_int addr)
      ~len

  let collect t addr ~len ~uid =
    Log.debug (fun m ->
        m "collect    %016x %d %d" (Addr.unsafe_to_int addr) len uid);
    Garbage_collector.collect t addr ~len ~uid

  let pause_intrinsic () = C.pause_intrinsic ()
end

module Rowex_rd = Rowex.Make (Reader)
module Rowex_wr = Rowex.Make (Writer)

let lookup (t : reader) = Rowex_rd.lookup t t.root
let exists (t : reader) = Rowex_rd.exists t t.root
let remove (t : writer) = Rowex_wr.remove t t.root
let insert (t : writer) = Rowex_wr.insert t t.root

let make ~filepath memory =
  C.atomic_set_leuintnat memory 0 (size_of_word * 2);
  let clatch = ref None
  and extend_result = ref None
  and released_processes = ref Set.empty in
  let t : t =
    {
      filepath
    ; memory = Atomic.make memory
    ; root = Rowex.Addr.null
    ; queue_locker = Miou.Mutex.create ()
    ; active_processes = Queue.create ()
    ; released_processes
    ; clatch
    ; free_locker = Miou.Mutex.create ()
    ; free = Hashtbl.create 0x100
    ; free_cells = Atomic.make 0
    ; older_active_process = Atomic.make 0
    ; collected = Miou.Queue.create ()
    ; extend_locker = Miou.Mutex.create ()
    ; extend_result
    }
  in
  let writer : writer =
    {
      filepath
    ; free_locker = t.free_locker
    ; free = t.free
    ; free_cells = t.free_cells
    ; older_active_process = t.older_active_process
    ; queue_locker = t.queue_locker
    ; active_processes = t.active_processes
    ; released_processes
    ; clatch
    ; collected = t.collected
    ; extend_locker = t.extend_locker
    ; extend_result
    ; memory
    ; memory_from_t = t.memory
    ; uid = Garbage_collector.gen ()
    ; root = t.root
    }
  in
  let root = Rowex_wr.make writer in
  C.atomic_set_leuintnat memory size_of_word (Rowex.Addr.unsafe_to_int root);
  { t with root }

let load ~filepath memory =
  let root = C.atomic_get_leuintnat memory size_of_word in
  let t : t =
    {
      filepath
    ; memory = Atomic.make memory
    ; root = Rowex.Addr.of_int_to_rdwr root
    ; free_locker = Miou.Mutex.create ()
    ; free = Hashtbl.create 0x100
    ; free_cells = Atomic.make 0
    ; older_active_process = Atomic.make 0
    ; collected = Miou.Queue.create ()
    ; queue_locker = Miou.Mutex.create ()
    ; active_processes = Queue.create ()
    ; released_processes = ref Set.empty
    ; clatch = ref None
    ; extend_locker = Miou.Mutex.create ()
    ; extend_result = ref None
    }
  in
  scan t;
  t

let from_system ?(size = 10485760) filepath =
  if Sys.file_exists filepath then load ~filepath (System.load_memory filepath)
  else
    let open Unix in
    let open Bigarray in
    let fd = Unix.openfile filepath Unix.[ O_RDWR; O_DSYNC; O_CREAT ] 0o644 in
    Unix.ftruncate fd size;
    let len = (fstat fd).st_size in
    let memory = Unix.map_file fd ~pos:0L char c_layout true [| len |] in
    Unix.close fd;
    let memory = Bigarray.array1_of_genarray memory in
    make ~filepath memory

(* This part is how we handle processes. *)

(* the goal here is to update [t.older_active_writer] to the one we get from
   [t.active_writers]. We **really try** to be synchrone between the last
   [t.active_writers] and [t.older_active_writer]. *)
let rec update_older_active_process ?(backoff = Miou.Backoff.default) ?older
    (t : t) =
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

let add_process (t : t) kind ~uid =
  Log.debug (fun m -> m "new writer %016x" uid);
  let set = Atomic.compare_and_set t.older_active_process 0 uid in
  if not set then begin
    let older, _ =
      Miou.Mutex.protect t.queue_locker @@ fun () ->
      Queue.push (uid, kind) t.active_processes;
      (* here, we take the previous writer before the apparition of our new one. *)
      Queue.peek t.active_processes
    in
    update_older_active_process ~older t
  end
  else
    Miou.Mutex.protect t.queue_locker @@ fun () ->
    Queue.push (uid, kind) t.active_processes

let rec unsafe_clean_released_processes (t : t) =
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

let release_process (t : t) kind ~uid =
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
              m "it exists an older active writer (%016x) than %016x" older uid);
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

let reader (t : t) fn =
  let uid = Garbage_collector.gen () in
  add_process t `Rd ~uid;
  let reader =
    { memory = Atomic.get t.memory; root = Rowex.Addr.to_rdonly t.root }
  in
  let res = try Ok (fn reader) with exn -> Error exn in
  release_process t `Rd ~uid;
  match res with Ok value -> value | Error exn -> raise exn

let writer (t : t) fn =
  let writer : writer =
    {
      filepath = t.filepath
    ; free_locker = t.free_locker
    ; free = t.free
    ; free_cells = t.free_cells
    ; older_active_process = t.older_active_process
    ; queue_locker = t.queue_locker
    ; active_processes = t.active_processes
    ; released_processes = t.released_processes
    ; clatch = t.clatch
    ; collected = t.collected
    ; extend_locker = t.extend_locker
    ; extend_result = t.extend_result
    ; memory = Atomic.get t.memory
    ; memory_from_t = t.memory
    ; uid = Garbage_collector.gen ()
    ; root = t.root
    }
  in
  add_process t `Wr ~uid:writer.uid;
  let res =
    try Ok (fn writer)
    with exn ->
      Log.err (fun m ->
          m "%016x terminated with an exception: %S" writer.uid
            (Printexc.to_string exn));
      Error exn
  in
  release_process t `Wr ~uid:writer.uid;
  res
