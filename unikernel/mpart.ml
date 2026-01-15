let src = Logs.Src.create "mpart"
let tag = Logs.Tag.def "task" Fmt.int

module Log = (val Logs.src_log src : Logs.LOG)

type 'fd memory = {
    blk : 'fd
  ; wr : 'fd Cachet_wr.t
  ; rd : 'fd Cachet.t
  ; atomic_read : 'fd -> src_off:int -> ?dst_off:int -> Bstr.t -> unit
  ; writev : 'fd Cachet_wr.writev
  ; tmp : Bstr.t
  ; pagesize : int
}

(* XXX(dinosaure): we don't handle 32-bit architecture for unikernels. *)
let () = assert (Sys.word_size = 64)

(* A few notes about our unikernel and rowex:
  - We only support 64-bit architecture. We could support 32-bit architecture,
    as rowex was designed to run on such an architecture, but Solo5 only works
    in 64-bit, so let's avoid complicating the implementation.
  - When we talk about atomic access, we mean that the operation is
    "indivisible" (and "irreducible"): that is, when we do an
    [atomic_get_leuintnat], there is no opportunity for the scheduler (here,
    Miou) to attempt to execute other tasks. In this case, [Mkernel] offers
    [Block.atomic_{read,write}] for this purpose.
  - This mainly means that our [Cachet] cache system must use [atomic_read]
  - Writing is also atomic but not effective (it consists of populating our
    write pipeline). However, [persist] (and [commit]) are also atomic (even
    though they perform reads/writes, these are atomic and do not allow the
    scheduler to execute other tasks).
  - Finally, there is a special case for [movnt64] and [set_n48_key] (which is
    based on [movnt64]). These operations must be written without taking into
    account the cache on our block device. We therefore load the pages onto a
    pre-allocated continuous memory [t.tmp] to perform the desired operation
    ([movnt64] or [set_n48_key]) and then write the result to the block device
    (we also invalidate our cache on this area). *)

let int32_to_int =
  let mask = (0xffff lsl 16) lor 0xffff in
  fun n -> Int32.to_int n land mask

let int63_to_int = Int64.to_int

module C = struct
  let persist t off len = Cachet_wr.persist t.wr ~off ~len
  let atomic_get_uint8 t off = Cachet_wr.get_uint8 t.wr off
  let atomic_set_uint8 t off v = Cachet_wr.set_uint8 t.wr off v

  let atomic_get_leuintnat t off =
    int63_to_int (Cachet_wr.get_int64_le t.wr off)

  let atomic_set_leuintnat t off v =
    Cachet_wr.set_int64_le t.wr off (Int64.of_int v)

  let atomic_get_leuint16 t off = Cachet_wr.get_uint16_le t.wr off
  let atomic_set_leuint16 t off v = Cachet_wr.set_uint16_le t.wr off v
  let atomic_get_leuint31 t off = int32_to_int (Cachet_wr.get_int32_le t.wr off)

  let atomic_set_leuint31 t off v =
    Cachet_wr.set_int32_le t.wr off (Int32.of_int v)

  let atomic_get_leuint64 t off = Cachet_wr.get_int64_le t.wr off
  let atomic_set_leuint64 t off v = Cachet_wr.set_int64_le t.wr off v

  let atomic_get_leuint128 t off buf =
    let str = Cachet_wr.get_int128 t.wr off in
    (* TODO(dinosaure): [Cachet_wr] should provides [get_int128_into]. *)
    Bytes.blit_string str 0 buf 0 16

  let atomic_fetch_add_leuint16 t off v =
    let v' = Cachet_wr.get_uint16_le t.wr off in
    Cachet_wr.set_uint16_le t.wr off (v' + v);
    v'

  let atomic_fetch_sub_leuint16 t off v =
    let v' = Cachet_wr.get_uint16_le t.wr off in
    Cachet_wr.set_uint16_le t.wr off (v' - v);
    v'

  let atomic_fetch_add_leuintnat t off v =
    let v' = Cachet_wr.get_int64_le t.wr off in
    let v' = int63_to_int v' in
    Cachet_wr.set_int64_le t.wr off (Int64.of_int (v' + v));
    v'

  let atomic_fetch_sub_leuintnat t off v =
    let v' = Cachet_wr.get_int64_le t.wr off in
    let v' = int63_to_int v' in
    Cachet_wr.set_int64_le t.wr off (Int64.of_int (v' - v));
    v'

  let pause_intrinsic () = Miou.yield ()

  let atomic_compare_exchange_strong t off expected desired =
    let seen = int63_to_int (Cachet_wr.get_int64_le t.wr off) in
    let set = Atomic.compare_and_set expected seen desired in
    if set then Cachet_wr.set_int64_le t.wr off (Int64.of_int desired);
    set

  let atomic_compare_exchange_weak = atomic_compare_exchange_strong
  let get_leint31 t off = int32_to_int (Cachet_wr.get_int32_le t.wr off)
  let get_leintnat t off = int63_to_int (Cachet_wr.get_int64_le t.wr off)

  let atomic_fetch_or_leuintnat t off v =
    let v' = Cachet_wr.get_int64_le t.wr off in
    let v' = int63_to_int v' in
    Cachet_wr.set_int64_le t.wr off (Int64.of_int (v' lor v));
    v'

  let get_ocaml_string_length t off =
    (* XXX(dinosaure): we assume [Sys.word_size = 64]. *)
    let ln = Cachet_wr.get_int64_le t.wr off in
    let ln = Int64.logand ln 0xfffffffffffffffL in
    Log.debug (fun m -> m "load length of string at %016x (%016Lx)" off ln);
    let ln = int63_to_int ln in
    let pd = Cachet_wr.get_int64_le t.wr (off + ((ln - 2) * 8)) in
    let pd = int63_to_int pd in
    ((ln - 2) * 8) - (pd lsr 56) - 1

  let get_ocaml_string t off =
    let len = get_ocaml_string_length t off in
    Log.debug (fun m -> m "load string at %016x (len: %d)" off len);
    let buf = Bytes.create len in
    let len0 = len land 3 in
    let len1 = len asr 2 in
    for i = 0 to len1 - 1 do
      let i = i * 4 in
      let v = Cachet_wr.get_int32_ne t.wr (off + 8 + i) in
      Bytes.set_int32_le buf i v
    done;
    for i = 0 to len0 - 1 do
      let i = (len1 * 4) + i in
      let v = Cachet_wr.get_uint8 t.wr (off + 8 + i) in
      Bytes.set_uint8 buf i v
    done;
    Bytes.unsafe_to_string buf

  let repeat n fn =
    for i = 0 to n - 1 do
      fn i
    done

  let unsafe_and_uncacheable_operation t logical_address len ~fn:op =
    let p0 = logical_address lsr t.pagesize in
    let p1 = (logical_address + len) lsr t.pagesize in
    let number_of_pages = p1 - p0 + 1 in
    let physical_address = p0 lsl t.pagesize in
    let fn idx =
      let src_off = physical_address + (idx * (1 lsl t.pagesize)) in
      let dst_off = idx * (1 lsl t.pagesize) in
      t.atomic_read t.blk ~src_off ~dst_off t.tmp
    in
    repeat number_of_pages fn;
    op t.tmp (logical_address - physical_address);
    let fn idx =
      let off = idx * (1 lsl t.pagesize) in
      Bstr.sub t.tmp ~off ~len:(1 lsl t.pagesize)
    in
    let pages = List.init number_of_pages fn in
    t.writev t.blk ~pos:physical_address pages;
    Cachet.invalidate t.rd ~off:logical_address ~len

  external set_n48_key : Bstr.t -> int -> int -> int -> unit
    = "unimplemented" "caml_set_n48_key"

  let set_n48_key t off k v =
    let fn bstr roff = set_n48_key bstr roff k v in
    unsafe_and_uncacheable_operation t off 348 ~fn

  let movnt64 t off v =
    let fn bstr off = Bstr.set_int64_ne bstr off (Int64.of_int v) in
    unsafe_and_uncacheable_operation t off 8 ~fn
end

module W = struct
  type nonrec memory = { memory : Mkernel.Block.t memory; uid : Gc.uid }

  let length t = Mkernel.Block.length t.memory.blk

  let atomic_fetch_add_leuintnat t off v =
    C.atomic_fetch_add_leuintnat t.memory off v

  let atomic_set_leuintnat t off v = C.atomic_set_leuintnat t.memory off v
  let set_int32 t off v = Cachet_wr.set_int32_ne t.memory.wr off v
  let set_uint8 t off v = Cachet_wr.set_uint8 t.memory.wr off v
end

module Garbage_collector = Gc.Make (W)

type reader = {
    memory : Mkernel.Block.t memory
  ; root : Rowex.ro Rowex.Addr.t
  ; tags : Logs.Tag.set
}

type writer = {
    gc : W.memory Gc.t
  ; root : Rowex.rdwr Rowex.Addr.t
  ; tags : Logs.Tag.set
}

type t = writer

module Reader = struct
  type memory = reader
  type 'a t = 'a

  let bind x f = f x
  let return x = x

  open Rowex

  let get : type k v. memory -> 'a rd Addr.t -> (k, v) value -> v t =
   fun { memory; tags; _ } addr v ->
    Log.debug (fun m ->
        m ~tags "get        %016x : %a" (Addr.unsafe_to_int addr) pp_value v);
    match v with
    | OCaml_string -> C.get_ocaml_string memory (Addr.unsafe_to_int addr)
    | OCaml_string_length ->
        C.get_ocaml_string_length memory (Addr.unsafe_to_int addr)
    | LEInt31 -> C.get_leint31 memory (Addr.unsafe_to_int addr)
    | LEInt -> C.get_leintnat memory (Addr.unsafe_to_int addr)
    | _ -> assert false

  let atomic_get : type v. memory -> 'a rd Addr.t -> (atomic, v) value -> v t =
   fun { memory; tags; _ } addr k ->
    Log.debug (fun m ->
        m ~tags "atomic_get %016x : %a" (Addr.unsafe_to_int addr) pp_value k);
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
    ; tags = writer.tags
    }

  let get : type k v. memory -> 'a rd Addr.t -> (k, v) value -> v t =
   fun t addr k -> Reader.get (to_reader t) addr k

  let atomic_get : type v. memory -> 'a rd Addr.t -> (atomic, v) value -> v t =
   fun t addr k -> Reader.atomic_get (to_reader t) addr k

  let atomic_set : type v.
      memory -> 'a wr Addr.t -> (atomic, v) value -> v -> unit t =
   fun t addr k v ->
    let ({ memory; tags; _ } : reader) = to_reader t in
    Log.debug (fun m ->
        m ~tags "atomic_set %016x (%a : %a)" (Addr.unsafe_to_int addr)
          (pp_of_value k) v pp_value k);
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
    let ({ memory; tags; _ } : reader) = to_reader t in
    Log.debug (fun m ->
        m ~tags "fetch_add  %016x (%a : %a)" (Addr.unsafe_to_int addr)
          (pp_of_value k) v pp_value k);
    match k with
    | LEInt16 -> C.atomic_fetch_add_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_fetch_add_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let fetch_sub : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t
      =
   fun t addr k v ->
    let ({ memory; tags; _ } : reader) = to_reader t in
    Log.debug (fun m ->
        m ~tags "fetch_sub  %016x (%a : %a)" (Addr.unsafe_to_int addr)
          (pp_of_value k) v pp_value k);
    match k with
    | LEInt16 -> C.atomic_fetch_sub_leuint16 memory (Addr.unsafe_to_int addr) v
    | LEInt -> C.atomic_fetch_sub_leuintnat memory (Addr.unsafe_to_int addr) v
    | _ -> assert false

  let fetch_or : memory -> 'a wr Addr.t -> (atomic, int) value -> int -> int t =
   fun t addr k v ->
    let ({ memory; tags; _ } : reader) = to_reader t in
    Log.debug (fun m ->
        m ~tags "fetch_or   %016x (%a : %a)" (Addr.unsafe_to_int addr)
          (pp_of_value k) v pp_value k);
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
    let ({ memory; tags; _ } : reader) = to_reader t in
    Log.debug (fun m ->
        m ~tags "compare_exchange weak:%b %016x (%a : %a) (%a : %a)" weak
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
    Log.debug (fun m ->
        m ~tags:t.tags "persist    %016x (%d)" (Addr.unsafe_to_int addr) len);
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
    Log.debug (fun m -> m ~tags:t.tags "alloctate %3d" len);
    let { W.uid = writer; memory; _ } = Gc.memory t.gc in
    let addr = Garbage_collector.alloc t.gc ~writer ~kind len payloads in
    C.persist memory (addr :> int) len;
    addr

  let delete (t : memory) (addr : 'a Addr.t) len =
    Log.debug (fun m ->
        m ~tags:t.tags "delete     %016x %d" (Addr.unsafe_to_int addr) len);
    Garbage_collector.delete t.gc addr len

  let collect (t : memory) addr ~len ~uid =
    Log.debug (fun m ->
        m ~tags:t.tags "collect    %016x %d %d" (Addr.unsafe_to_int addr) len
          uid);
    let { W.uid = current; _ } = Gc.memory t.gc in
    Garbage_collector.collect t.gc current addr ~len ~uid

  let pause_intrinsic () =
    Log.debug (fun m -> m "yield");
    C.pause_intrinsic ()
end

module Rowex_rd = Rowex.Make (Reader)
module Rowex_wr = Rowex.Make (Writer)

let lookup (t : reader) = Rowex_rd.lookup t t.root
let exists (t : reader) = Rowex_rd.exists t t.root
let remove (t : writer) = Rowex_wr.remove t t.root
let insert (t : writer) = Rowex_wr.insert t t.root

let unsafe_ctz n =
  let t = ref 1 in
  let r = ref 0 in
  while n land !t = 0 do
    t := !t lsl 1;
    incr r
  done;
  !r

let make ?(tags = Logs.Tag.empty) blk =
  let pagesize = Mkernel.Block.pagesize blk in
  let map blk ~pos len =
    let bstr = Bstr.create len in
    Mkernel.Block.atomic_read blk ~src_off:pos bstr;
    bstr
  in
  let writev blk ~pos bstrs =
    let rec go pos = function
      | [] -> ()
      | bstr :: rest ->
          Mkernel.Block.atomic_write blk ~src_off:0 ~dst_off:pos bstr;
          go (pos + pagesize) rest
    in
    go pos bstrs
  in
  let number_of_pages = Mkernel.Block.length blk / pagesize in
  let wr = Cachet_wr.make ~pagesize ~map ~writev ~number_of_pages blk in
  let rd = Cachet_wr.cache wr in
  let tmp = Bstr.create (pagesize * 2) in
  let pagesize = unsafe_ctz pagesize in
  let atomic_read = Mkernel.Block.atomic_read in
  let m = { blk; wr; rd; atomic_read; writev; tmp; pagesize } in
  let w = { W.memory = m; uid = Gc.null } in
  let extend_and_copy _ _ = raise Out_of_memory in
  let gc = Gc.make ~extend_and_copy (Atomic.make w) in
  let writer = { gc; root = Rowex.Addr.null; tags } in
  let brk = C.atomic_get_leuintnat m 0 in
  if brk == 0 then begin
    C.atomic_set_leuintnat m 0 16;
    let root = Rowex_wr.make writer in
    Log.debug (fun m -> m "ROWEX tree root: %016x" (root :> int));
    C.atomic_set_leuintnat m 8 (Rowex.Addr.unsafe_to_int root);
    Cachet_wr.commit wr;
    { gc; root; tags }
  end
  else
    let root = C.atomic_get_leuintnat m 8 in
    let root = Rowex.Addr.of_int_to_rdwr root in
    { gc; root; tags }

(* TODO(dinosaure): scan and fill our GC with unreachable nodes. *)

let reader t fn =
  let uid = Garbage_collector.add_process t.gc `Rd in
  let tags = Logs.Tag.add tag (uid :> int) t.tags in
  let root = Rowex.Addr.to_rdonly t.root in
  let reader = { memory = (Gc.atomic_memory t.gc).W.memory; root; tags } in
  let res = try Ok (fn reader) with exn -> Error exn in
  Garbage_collector.release_process t.gc `Rd ~uid;
  match res with Ok value -> value | Error exn -> raise exn

let writer t fn =
  let uid = Garbage_collector.add_process t.gc `Wr in
  let tags = Logs.Tag.add tag (uid :> int) t.tags in
  let memory = (Gc.atomic_memory t.gc).W.memory in
  let w = { W.memory; uid } in
  let gc = Gc.with_memory t.gc w in
  let writer = { gc; root = t.root; tags } in
  let res = try Ok (fn writer) with exn -> Error exn in
  Garbage_collector.release_process t.gc `Wr ~uid;
  match res with Ok value -> value | Error exn -> raise exn
