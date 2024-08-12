let src = Logs.Src.create "part"

module Log = (val Logs.src_log src : Logs.LOG)

type memory =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

type cell = { addr : int; len : int; uid : int }

module Set = Set.Make (Int)

type t = {
    memory : memory Atomic.t
  ; root : Rowex.rdwr Rowex.Addr.t
  ; free : (int, Set.t) Hashtbl.t
  ; free_cells : int Atomic.t
  ; free_locker : Miou.Mutex.t
  ; queue_locker : Miou.Mutex.t
  ; active_writers : int Queue.t
  ; mutable released_writers : Set.t
  ; older_active_writer : int Atomic.t
  ; collected : cell Miou.Queue.t
}

type reader = { memory : memory Atomic.t; root : Rowex.ro Rowex.Addr.t }

type writer = {
    free : (int, Set.t) Hashtbl.t
  ; free_cells : int Atomic.t
  ; free_locker : Miou.Mutex.t
  ; queue_locker : Miou.Mutex.t
  ; memory : memory Atomic.t
  ; root : Rowex.rdwr Rowex.Addr.t
  ; active_writers : int Queue.t
  ; released_writers : Set.t
  ; older_active_writer : int Atomic.t
  ; collected : cell Miou.Queue.t
  ; uid : int
}

let size_of_word = Sys.word_size / 8

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

  (*
  external msync : memory -> unit = "caml_msync" [@@noalloc]
*)

  external set_n48_key : memory -> int -> int -> int -> unit
    = "caml_set_n48_key"
  [@@noalloc]

  external movnt64 : memory -> int -> int -> unit = "caml_movnt64" [@@noalloc]
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
    let older_active_writer =
      Atomic.get (Sys.opaque_identity writer.older_active_writer)
    in
    older_active_writer = 0 || uid' < older_active_writer

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

  (*
  let load_memory t =
    let fd = Unix.openfile t.filepath Unix.[ O_RDWR; O_DSYNC ] 0o644 in
    let finally () = Unix.close fd in
    Fun.protect ~finally @@ fun () ->
    let memory =
      Unix.map_file fd ~pos:0L Bigarray.char Bigarray.c_layout true [| t.size |]
    in
    let memory = Bigarray.array1_of_genarray memory in
    t.memory <- memory

  let create_file ?(mode = 0o644) ?size filepath fn =
    let fd =
      Unix.openfile filepath
        Unix.[ O_RDWR; O_CREAT; O_DSYNC; O_APPEND ]
        (* O_DIRECT? *) mode
    in
    let rec fill acc =
      match fn () with
      | Some str ->
          let len = Unix.write_substring fd str 0 (String.length str) in
          fill (acc + len)
      | None -> acc
    in
    let finally () = Unix.close fd in
    Fun.protect ~finally @@ fun () ->
    match (fill 0, size) with
    | _size, None -> ()
    | a, Some b -> if a < b then Unix.ftruncate fd b

  let generate_filepath g pattern =
    let v = Random.State.bits g land 0xffffff in
    Fmt.str pattern (Fmt.str "%06x" v)

  let _copy_into_larger_filepath t =
    let new_filepath =
      let rec go retries =
        if retries >= 10 then failwith "Impossible to create a new rowex file";
        let v =
          Filename.(concat (dirname t.filepath))
            (generate_filepath t.g "rowex-%s.idx")
        in
        if Sys.file_exists v then go (succ retries) else v
      in
      go 0
    in
    let new_size = t.size + 1048576 in
    let fd = Unix.openfile t.filepath Unix.[ O_RDONLY ] 0o644 in
    let buf = Bytes.create 0x1000 in
    let rec copy () =
      match Unix.read fd buf 0 (Bytes.length buf) with
      | 0 -> None
      | len -> Some (Bytes.sub_string buf 0 len)
      | exception Unix.(Unix_error (EINTR, _, _)) -> copy ()
    in
    let finally () = Unix.close fd in
    Fun.protect ~finally @@ fun () ->
    C.msync t.memory;
    create_file ~size:new_size new_filepath copy;
    (new_filepath, new_size)
  *)

  let really_alloc writer ~kind len payloads =
    let memory = Atomic.get writer.memory in
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
    else raise Out_of_memory
  (* (
     Log.debug (fun m -> m "prepare next index");
     let new_filepath, new_size = copy_into_larger_filepath t in
     waiting_pending_readers_and_lock t;
     t.filepath <- new_filepath;
     t.size <- new_size;
     load_memory t;
     assert (Atomic.compare_and_set t.lock true false);
     Miou.Condition.broadcast (snd t.readers_locker);
     assert (brk + len <= t.size);
     blitv payloads t.memory brk;
     Log.debug (fun m -> m "copy at %016x:" brk);
     Log.debug (fun m ->
         m "@[<hov>%a@]"
           (Hxd_string.pp Hxd.default)
           (String.concat "" payloads));
     C.atomic_set_leuintnat t.memory 0 (brk + len);
     Log.debug (fun m ->
         m "atomic_set %016x (%a : %a)" 0 (Rowex.pp_of_value LEInt) (brk + len)
           Rowex.pp_value LEInt);
     Atomic.set t.brk (brk + len);
     Rowex.Addr.of_int_to_rdwr brk) *)

  let alloc writer ~kind len payloads =
    match get_free_cell writer ~len with
    | Some addr ->
        let memory = Atomic.get writer.memory in
        blitv payloads memory addr;
        if kind = `Node then
          C.atomic_set_leuintnat memory (addr + Rowex._header_owner) writer.uid;
        Rowex.Addr.of_int_to_rdwr addr
    | None -> (
        ignore (sweep writer);
        match get_free_cell writer ~len with
        | None -> really_alloc writer ~kind len payloads
        | Some addr ->
            let memory = Atomic.get writer.memory in
            blitv payloads memory addr;
            if kind = `Node then
              C.atomic_set_leuintnat memory
                (addr + Rowex._header_owner)
                writer.uid;
            Rowex.Addr.of_int_to_rdwr addr)
end

let add_free_cell (rowex : t) ~addr ~len =
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
  let cur = ref (C.atomic_get_leuintnat memory size_of_word) in
  let collected = ref 0 in
  Log.debug (fun m -> m "scan: start");
  while !cur < brk do
    Log.debug (fun m -> m "scan: %016x" !cur);
    let hdr = C.atomic_get_leuintnat memory !cur in
    match hdr lsr Rowex._bits_kind with
    | (0 | 1 | 2 | 3) as v ->
        let len = size_of_node v in
        if hdr land 1 = 1 then begin
          add_free_cell rowex ~addr:!cur ~len;
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
    let memory = Atomic.get memory in
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
    let memory = Atomic.get memory in
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

  let atomic_set :
      type v. memory -> 'a wr Addr.t -> (atomic, v) value -> v -> unit t =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<atomic_set>)"

  let fetch_add : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<fetch_add>)"

  let fetch_sub : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<fetch_sub>)"

  let fetch_or : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t =
   fun _ _ _ _ -> Fmt.failwith "Invalid reader operation (<fetch_or>)"

  let compare_exchange :
      type a.
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

  let pause_intrinsic () =
    Miou.yield ();
    C.pause_intrinsic ()
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

  let atomic_set :
      type v. memory -> 'a wr Addr.t -> (atomic, v) value -> v -> unit t =
   fun { memory; _ } addr k v ->
    Log.debug (fun m ->
        m "atomic_set %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    let memory = Atomic.get memory in
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
    let memory = Atomic.get memory in
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
    let memory = Atomic.get memory in
    match k with
    | LEInt16 -> C.atomic_fetch_sub_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_fetch_sub_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let fetch_or : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t =
   fun { memory; _ } addr k v ->
    Log.debug (fun m ->
        m "fetch_or   %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    let memory = Atomic.get memory in
    match k with
    | LEInt -> C.atomic_fetch_or_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let compare_exchange :
      type a.
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
    let memory = Atomic.get memory in
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
    let memory = Atomic.get memory in
    C.persist memory (Addr.unsafe_to_int addr) len

  let set_n48_key { memory; _ } (addr : 'c wr Addr.t) k c =
    let memory = Atomic.get memory in
    C.set_n48_key memory (Addr.unsafe_to_int addr) k c

  let movnt64 { memory; _ } ~(dst : 'c wr Addr.t) src =
    let memory = Atomic.get memory in
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

let make memory =
  C.atomic_set_leuintnat memory 0 (size_of_word * 2);
  let t : t =
    {
      memory = Atomic.make memory
    ; root = Rowex.Addr.null
    ; free = Hashtbl.create 0x100
    ; free_cells = Atomic.make 0
    ; free_locker = Miou.Mutex.create ()
    ; queue_locker = Miou.Mutex.create ()
    ; active_writers = Queue.create ()
    ; released_writers = Set.empty
    ; older_active_writer = Atomic.make 0
    ; collected = Miou.Queue.create ()
    }
  in
  let writer =
    {
      memory = t.memory
    ; root = t.root
    ; free = t.free
    ; free_cells = t.free_cells
    ; free_locker = t.free_locker
    ; queue_locker = t.queue_locker
    ; active_writers = t.active_writers
    ; released_writers = t.released_writers
    ; collected = t.collected
    ; older_active_writer = t.older_active_writer
    ; uid = Garbage_collector.gen ()
    }
  in
  let root = Rowex_wr.make writer in
  C.atomic_set_leuintnat memory size_of_word (Rowex.Addr.unsafe_to_int root);
  { t with root }

let load memory =
  let root = C.atomic_get_leuintnat memory size_of_word in
  let t =
    {
      memory = Atomic.make memory
    ; root = Rowex.Addr.of_int_to_rdwr root
    ; free = Hashtbl.create 0x100
    ; free_cells = Atomic.make 0
    ; free_locker = Miou.Mutex.create ()
    ; queue_locker = Miou.Mutex.create ()
    ; active_writers = Queue.create ()
    ; released_writers = Set.empty
    ; older_active_writer = Atomic.make 0
    ; collected = Miou.Queue.create ()
    }
  in
  scan t;
  t

let from_system ~filepath =
  if Sys.file_exists filepath then begin
    let fd = Unix.openfile filepath Unix.[ O_RDWR; O_DSYNC ] 0o644 in
    let stat = Unix.fstat fd in
    let memory =
      Unix.map_file fd ~pos:0L Bigarray.char Bigarray.c_layout true
        [| stat.Unix.st_size |]
    in
    Unix.close fd;
    let memory = Bigarray.array1_of_genarray memory in
    load memory
  end
  else
    let fd = Unix.openfile filepath Unix.[ O_RDWR; O_DSYNC; O_CREAT ] 0o644 in
    Unix.ftruncate fd 31457280 (* 30M *);
    let stat = Unix.fstat fd in
    let memory =
      Unix.map_file fd ~pos:0L Bigarray.char Bigarray.c_layout true
        [| stat.Unix.st_size |]
    in
    Unix.close fd;
    let memory = Bigarray.array1_of_genarray memory in
    make memory

let reader (rowex : t) =
  { memory = rowex.memory; root = Rowex.Addr.to_rdonly rowex.root }

let rec update_older_activer_writer ?(backoff = Miou.Backoff.default) ?older
    (t : t) =
  let older =
    match older with
    | Some older -> older
    | None -> (
        Miou.Mutex.protect t.queue_locker @@ fun () ->
        match Queue.peek t.active_writers with
        | older -> older
        | exception Queue.Empty -> 0)
  in
  let seen = Atomic.get t.older_active_writer in
  if
    seen <> older
    && not (Atomic.compare_and_set t.older_active_writer seen older)
  then update_older_activer_writer ~backoff:(Miou.Backoff.once backoff) t

let add_writer (t : t) ~uid =
  Log.debug (fun m -> m "new writer %016x" uid);
  let set = Atomic.compare_and_set t.older_active_writer 0 uid in
  if not set then begin
    let older =
      Miou.Mutex.protect t.queue_locker @@ fun () ->
      Queue.push uid t.active_writers;
      Queue.peek t.active_writers
    in
    update_older_activer_writer ~older t
  end
  else
    Miou.Mutex.protect t.queue_locker @@ fun () ->
    Queue.push uid t.active_writers

let rec clean_released_writers (t : t) =
  if Set.is_empty t.released_writers = false then
    match Queue.peek t.active_writers with
    | older ->
        if Set.mem older t.released_writers then begin
          t.released_writers <- Set.remove older t.released_writers;
          ignore (Queue.pop t.active_writers);
          clean_released_writers t
        end
    | exception Queue.Empty -> ()

let release_writer (t : t) ~uid =
  let older =
    Miou.Mutex.protect t.queue_locker @@ fun () ->
    Log.debug (fun m -> m "release writer %016x" uid);
    match Queue.peek t.active_writers with
    | older ->
        if uid = older then begin
          assert (Queue.pop t.active_writers = uid);
          Log.debug (fun m -> m "clean possible released writers");
          clean_released_writers t;
          Option.value ~default:0 (Queue.peek_opt t.active_writers)
        end
        else begin
          Log.debug (fun m ->
              m "it exists an older active writer (%016x) than %016x" older uid);
          t.released_writers <- Set.add uid t.released_writers;
          older
        end
    | exception Queue.Empty ->
        Log.err (fun m -> m "we missed writer %016x" uid);
        assert false
  in
  update_older_activer_writer ~older t

let writer (t : t) fn =
  let writer : writer =
    {
      memory = t.memory
    ; root = t.root
    ; free = t.free
    ; free_cells = t.free_cells
    ; free_locker = t.free_locker
    ; queue_locker = t.queue_locker
    ; active_writers = t.active_writers
    ; released_writers = t.released_writers
    ; collected = t.collected
    ; older_active_writer = t.older_active_writer
    ; uid = Garbage_collector.gen ()
    }
  in
  add_writer t ~uid:writer.uid;
  let res =
    try Ok (fn writer)
    with exn ->
      Log.err (fun m ->
          m "%016x terminated with an exception: %S" writer.uid
            (Printexc.to_string exn));
      Error exn
  in
  release_writer t ~uid:writer.uid;
  res
