let src = Logs.Src.create "part"

module Log = (val Logs.src_log src : Logs.LOG)

module C = struct
  type memory = Bstr.t

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

module W = struct
  type memory = { filepath : string; memory : Bstr.t; uid : Gc.uid }

  let length t = Bstr.length t.memory

  let atomic_fetch_add_leuintnat t off v =
    C.atomic_fetch_add_leuintnat t.memory off v

  let atomic_set_leuintnat t off v = C.atomic_set_leuintnat t.memory off v
  let set_int32 t off v = Bstr.set_int32_ne t.memory off v
  let set_uint8 t off v = Bstr.set_uint8 t.memory off v
end

module Garbage_collector = Gc.Make (W)

type memory = Bstr.t
type writer = { gc : W.memory Gc.t; root : Rowex.rdwr Rowex.Addr.t }
type reader = { memory : Bstr.t; root : Rowex.ro Rowex.Addr.t }
type uid = Gc.uid

let size_of_word = Sys.word_size / 8

module System = struct
  let src = Logs.Src.create "part.system"

  module Log = (val Logs.src_log src : Logs.LOG)

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

  let copy_into_larger_filepath uid { W.filepath; memory; _ } =
    let new_filepath =
      let rec go retries =
        if retries >= 10 then failwith "Impossible to create a new rowex file";
        let v = generate_filepath "rowex-%s.idx" in
        if Sys.file_exists v then go (succ retries) else v
      in
      go 0
    in
    (* we add 1MiB *)
    let new_size = Bstr.length memory + 1048576 in
    let fd = Unix.openfile filepath Unix.[ O_RDONLY ] 0o644 in
    let finally () = Unix.close fd in
    Fun.protect ~finally @@ fun () ->
    (* please note that this part cannot be used if there is only one active
       writer (see the worst branch of [really_alloc]). The advantage is that it
       truly synchronizes writes so that our entire file can then be copied
       cleanly to another destination. readers can continue to run normally. *)
    C.msync memory;
    Log.debug (fun m ->
        m "copy %s (%d byte(s)) into %s" filepath (Bstr.length memory)
          new_filepath);
    into_new_file ~size:new_size new_filepath fd;
    Log.debug (fun m -> m "rename %s into %s" new_filepath filepath);
    Unix.rename new_filepath filepath;
    let memory = load_memory filepath in
    Log.debug (fun m ->
        m "reload %s (%d byte(s))" filepath (Bstr.length memory));
    ({ W.filepath; memory; uid }, new_size)
end

type t = {
    filepath : string
  ; gc : W.memory Gc.t
  ; root : Rowex.rdwr Rowex.Addr.t
}

let size_of_node = function
  | 0 -> Rowex._sizeof_n4
  | 1 -> Rowex._sizeof_n16
  | 2 -> Rowex._sizeof_n48
  | 3 -> Rowex._sizeof_n256
  | _ -> assert false

let scan (rowex : t) =
  let { W.memory; _ } = Gc.memory rowex.gc in
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
          let addr = Rowex.Addr.of_int_to_rdwr !cur in
          Garbage_collector.unsafe_delete rowex.gc addr len;
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

  let to_reader (writer : memory) =
    {
      memory = (Gc.memory writer.gc).memory
    ; root = Rowex.Addr.to_rdonly writer.root
    }

  let get : type k v. memory -> 'a rd Addr.t -> (k, v) value -> v t =
   fun t addr k -> Reader.get (to_reader t) addr k

  let atomic_get : type v. memory -> 'a rd Addr.t -> (atomic, v) value -> v t =
   fun t addr k -> Reader.atomic_get (to_reader t) addr k

  let atomic_set : type v.
      memory -> 'a wr Addr.t -> (atomic, v) value -> v -> unit t =
   fun t addr k v ->
    let ({ memory; _ } : reader) = to_reader t in
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
   fun t addr k v ->
    let ({ memory; _ } : reader) = to_reader t in
    Log.debug (fun m ->
        m "fetch_add  %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    match k with
    | LEInt16 -> C.atomic_fetch_add_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_fetch_add_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let fetch_sub : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun t addr k v ->
    let ({ memory; _ } : reader) = to_reader t in
    Log.debug (fun m ->
        m "fetch_sub  %016x (%a : %a)" (Addr.unsafe_to_int addr) (pp_of_value k)
          v pp_value k);
    match k with
    | LEInt16 -> C.atomic_fetch_sub_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_fetch_sub_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let fetch_or : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t =
   fun t addr k v ->
    let ({ memory; _ } : reader) = to_reader t in
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
   fun t ?(weak = false) addr k expected desired ->
    let ({ memory; _ } : reader) = to_reader t in
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

  let persist t (addr : 'c wr Addr.t) ~len =
    Log.debug (fun m -> m "persist    %016x (%d)" (Addr.unsafe_to_int addr) len);
    let ({ memory; _ } : reader) = to_reader t in
    C.persist memory (Addr.unsafe_to_int addr) len

  let set_n48_key t (addr : 'c wr Addr.t) k c =
    let ({ memory; _ } : reader) = to_reader t in
    C.set_n48_key memory (Addr.unsafe_to_int addr) k c

  let movnt64 t ~(dst : 'c wr Addr.t) src =
    let ({ memory; _ } : reader) = to_reader t in
    C.movnt64 memory (Addr.unsafe_to_int dst) src

  let allocate (t : memory) ~kind ?len payloads =
    let len =
      match len with
      | Some len -> len
      | None -> List.fold_left (fun a str -> a + String.length str) 0 payloads
    in
    let { W.uid; _ } = Gc.memory t.gc in
    Log.debug (fun m -> m "[%016x] alloctate %3d" (uid :> int) len);
    Garbage_collector.alloc t.gc ~writer:uid ~kind len payloads

  let delete (t : memory) (addr : 'a Addr.t) len =
    Log.debug (fun m -> m "delete     %016x %d" (Addr.unsafe_to_int addr) len);
    Garbage_collector.delete t.gc addr len

  let collect (t : memory) addr ~len ~uid =
    Log.debug (fun m ->
        m "collect    %016x %d %d" (Addr.unsafe_to_int addr) len uid);
    let { W.uid = current; _ } = Gc.memory t.gc in
    Garbage_collector.collect t.gc current addr ~len ~uid

  let pause_intrinsic () =
    C.pause_intrinsic ();
    Miou.yield ()
end

module Rowex_rd = Rowex.Make (Reader)
module Rowex_wr = Rowex.Make (Writer)

let lookup (t : reader) = Rowex_rd.lookup t t.root
let exists (t : reader) = Rowex_rd.exists t t.root
let remove (t : writer) = Rowex_wr.remove t t.root
let insert (t : writer) = Rowex_wr.insert t t.root

let make ~filepath memory =
  C.atomic_set_leuintnat memory 0 (size_of_word * 2);
  let w = Atomic.make { W.filepath; memory; uid = Gc.null } in
  let extend_and_copy = System.copy_into_larger_filepath in
  let gc = Gc.make ~extend_and_copy w in
  let writer = { gc; root = Rowex.Addr.null } in
  let root = Rowex_wr.make writer in
  C.atomic_set_leuintnat memory size_of_word (Rowex.Addr.unsafe_to_int root);
  { filepath; gc; root }

let load ~filepath memory =
  let root = C.atomic_get_leuintnat memory size_of_word in
  let root = Rowex.Addr.of_int_to_rdwr root in
  let w = Atomic.make { W.filepath; memory; uid = Gc.null } in
  let extend_and_copy = System.copy_into_larger_filepath in
  let gc = Gc.make ~extend_and_copy w in
  let t = { filepath; gc; root } in
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

let reader (t : t) fn =
  let uid = Garbage_collector.add_process t.gc `Rd in
  let root = Rowex.Addr.to_rdonly t.root in
  let reader = { memory = (Gc.atomic_memory t.gc).W.memory; root } in
  let res = try Ok (fn ~uid reader) with exn -> Error exn in
  Garbage_collector.release_process t.gc `Rd ~uid;
  match res with Ok value -> value | Error exn -> raise exn

let writer (t : t) fn =
  let uid = Garbage_collector.add_process t.gc `Wr in
  let memory = (Gc.atomic_memory t.gc).W.memory in
  let w = { W.filepath = t.filepath; memory; uid } in
  let gc = Gc.with_memory t.gc w in
  let writer = { gc; root = t.root } in
  let res =
    try Ok (fn ~uid writer)
    with exn ->
      Log.err (fun m ->
          m "%016x terminated with an exception: %S"
            (uid :> int)
            (Printexc.to_string exn));
      Error exn
  in
  Garbage_collector.release_process t.gc `Wr ~uid;
  res
