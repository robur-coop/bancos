let src = Logs.Src.create "mpart"

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

  let atomic_set_leuint128 t off v = Cachet_wr.set_int128 t.wr off v

  let atomic_fetch_add_leuint16 t off v =
    let v' = Cachet_wr.get_uint16_le t.wr off in
    Cachet_wr.set_uint16_le t.wr off (v' + v)

  let atomic_fetch_sub_leuint16 t off v =
    let v' = Cachet_wr.get_uint16_le t.wr off in
    Cachet_wr.set_uint16_le t.wr off (v' - v)

  let atomic_fetch_add_leuintnat t off v =
    let v' = int63_to_int (Cachet_wr.get_int64_le t.wr off) in
    Cachet_wr.set_int64_le t.wr off (Int64.of_int (v + v'))

  let atomic_fetch_sub_leuintnat t off v =
    let v' = int63_to_int (Cachet_wr.get_int64_le t.wr off) in
    Cachet_wr.set_int64_le t.wr off (Int64.of_int (v - v'))

  let pause_intrinsic () = Miou.yield ()

  let atomic_compare_exchange_strong t off expected desired =
    let seen = int63_to_int (Cachet_wr.get_int64_le t.wr off) in
    Atomic.compare_and_set expected seen desired

  let atomic_compare_exchange_weak = atomic_compare_exchange_strong
  let get_leint31 t off = int32_to_int (Cachet_wr.get_int32_le t.wr off)
  let get_leintnat t off = int63_to_int (Cachet_wr.get_int64_le t.wr off)

  let atomic_fetch_or_leuintnat t off v =
    let v' = int63_to_int (Cachet_wr.get_int64_le t.wr off) in
    Cachet_wr.set_int64_le t.wr off (Int64.of_int (v lor v'))

  let get_ocaml_string_length t off =
    (* XXX(dinosaure): we assume [Sys.word_size = 64]. *)
    let ln = int63_to_int (Cachet_wr.get_int64_le t.wr off) in
    let pd = Cachet_wr.get_int64_le t.wr (off + ((ln - 2) * 8)) in
    let pd = int63_to_int pd in
    ((ln - 2) * 8) - (pd lsr 56) - 1

  let get_ocaml_string t off =
    let len = get_ocaml_string_length t off in
    let buf = Bytes.create len in
    Cachet.blit_to_bytes t.rd ~src_off:(off + 8) buf ~dst_off:0 ~len;
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
    let fn bstr off = Bstr.set_int64_ne bstr off v in
    unsafe_and_uncacheable_operation t off 8 ~fn
end

type reader = { memory : Mkernel.Block.t memory; root : Rowex.ro Rowex.Addr.t }

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

module Rowex_rd = Rowex.Make (Reader)
