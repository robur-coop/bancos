let () = Printexc.record_backtrace true

exception Duplicate

let ( .![] ) = String.unsafe_get
(* XXX(dinosaure): see [art.ml] about this unsafe access. *)

external ( <= ) : 'a -> 'a -> bool = "%lessequal"

let ( <= ) (x : int) y = x <= y [@@inline]
let min (a : int) b = if a <= b then a else b [@@inline]

type key = string

let key : string -> key =
 fun key ->
  if String.contains key '\000' then invalid_arg "Invalid key";
  key

external unsafe_key : string -> key = "%identity"

let src = Logs.Src.create "rowex"

module Log = (val Logs.src_log src : Logs.LOG)

module String = struct
  include Stdlib.String

  external unsafe_get_uint32 : string -> int -> int32 = "%caml_string_get32"
end

external bytes_set16 : bytes -> int -> int -> unit = "%caml_bytes_set16u"
external bytes_set32 : bytes -> int -> int32 -> unit = "%caml_bytes_set32u"
external bytes_set64 : bytes -> int -> int64 -> unit = "%caml_bytes_set64u"
external string_get16 : string -> int -> int = "%caml_string_get16u"

let const x _ = x
let size_of_word = Sys.word_size / 8

external bswap64 : int64 -> int64 = "%bswap_int64"
external bswap32 : int32 -> int32 = "%bswap_int32"

(* XXX(dinosaure): [Int64.of_int]/[Int32.of_int] interprets the bit-sign, so
   [Addr.null = 0x4000000000000000] becomes [0xc000000000000000] when we want to
   serialize it. [uint64_of_uint]/[uint32_of_uint] uses [Unsigned_long_val] but
   it's a shame to add these C functions. The compiler should add such
   primitives. *)

external uint64_of_uint : int -> (int64[@unboxed])
  = "bytecode_compilation_not_supported" "caml_uint64_of_uint"
[@@noalloc]

external uint32_of_uint : int -> (int32[@unboxed])
  = "bytecode_compilation_not_supported" "caml_uint32_of_uint"
[@@noalloc]

let leintnat_to_string v =
  if Sys.word_size = 64 && Sys.big_endian then (
    let v = bswap64 (uint64_of_uint v) in
    let res = Bytes.create 8 in
    bytes_set64 res 0 v;
    Bytes.unsafe_to_string res)
  else if Sys.word_size = 64 then (
    let v = uint64_of_uint v in
    let res = Bytes.create 8 in
    bytes_set64 res 0 v;
    Bytes.unsafe_to_string res)
  else if Sys.word_size = 32 && Sys.big_endian then (
    let v = bswap32 (uint32_of_uint v) in
    let res = Bytes.create 4 in
    bytes_set32 res 0 v;
    Bytes.unsafe_to_string res)
  else if Sys.word_size = 32 then (
    let v = uint32_of_uint v in
    let res = Bytes.create 4 in
    bytes_set32 res 0 v;
    Bytes.unsafe_to_string res)
  else assert false
[@@inline]

let leint31_to_string v =
  if Sys.big_endian then (
    let res = Bytes.create 4 in
    bytes_set32 res 0 (bswap32 (Int32.of_int v));
    Bytes.unsafe_to_string res)
  else
    let res = Bytes.create 4 in
    bytes_set32 res 0 (Int32.of_int v);
    Bytes.unsafe_to_string res
[@@inline]

(* XXX(dinosaure): same as the GC bit for OCaml, a native integer with the least
   significant bit to 1 is a leaf, otherwise, it is a node. Such layout is
   possible when **any** addresses are an even absolute number! To ensure that,
   the only value which can introduce an odd shift is a C string (something
   which terminates by a '\000') but we ensure that a C string has a "pad" of
   '\000' such as OCaml does.

   We use an native [int] here which depends on the architecture of the host
   system. We have multiple reason to do this choice:
   + Use an [int64] implies to use a boxed value which has an impact on
     performance (due to the indirection)
   + Use an [int64] implies a real performance regression on a 32-bits
     architecture (due to the impossibility to store it into a register)
   + A incompatibility exists if we move from a 32-bits to a 64-bits
     architecture - but it seems more clever to use a static tool which can
     upgrade (or downgrade) the format from one to another
   + A limitation exists about how many objects we can store - however, it seems
     fair to keep this limitation at our layer when it is applied at the whole
     system

   According to the layout, we are able to encode/{i serialise} an address into
   [Sys.word_size - 2] bits (GC bit plus ART-type bit). Then, the [NULL] address
   can be encoded to an impossible address (for us but it still is value for
   OCaml): [1 lsl (Sys.word_size - 2)]. This is our way to encode/{i serialise}
   this type:

   {[
      type 'a tree =
        | Leaf of 'a
        | Node of 'a t
      and 'a t = 'a tree option
   ]} *)

type 'a rd = < rd : unit ; .. > as 'a
type 'a wr = < wr : unit ; .. > as 'a
type ro = < rd : unit >
type wo = < wr : unit >
type rdwr = < rd : unit ; wr : unit >

module rec Leaf : sig
  type t [@@immediate]

  val prj : t -> 'a Addr.t
  val inj : 'a Addr.t -> t
end = struct
  type t = int

  let prj x = x lsr 1 [@@inline always]
  let inj x = (x lsl 1) lor 1 [@@inline always]
end

and Addr : sig
  type 'a t = private int

  val length : int
  val null : rdwr t
  val is_null : 'a t -> bool
  external of_int_to_rdonly : int -> ro t = "%identity"
  external of_int_to_wronly : int -> wo t = "%identity"
  external of_int_to_rdwr : int -> rdwr t = "%identity"
  external to_wronly : 'a wr t -> wo t = "%identity"
  external to_rdonly : 'a rd t -> ro t = "%identity"
  external unsafe_to_leaf : 'a t -> Leaf.t = "%identity"
  external unsafe_of_leaf : Leaf.t -> 'a t = "%identity"
  external unsafe_to_int : _ t -> int = "%identity"
  external unsafe_to_rdwr : _ t -> rdwr t = "%identity"
  val ( + ) : 'a t -> int -> 'a t
end = struct
  type 'a t = int

  let length = Sys.word_size / 8
  let null = 1 lsl (Sys.word_size - 2)
  let is_null x = x = null [@@inline always]

  external of_int_to_rdonly : int -> ro t = "%identity"
  external of_int_to_wronly : int -> wo t = "%identity"
  external of_int_to_rdwr : int -> rdwr t = "%identity"
  external to_wronly : 'a wr t -> wo t = "%identity"
  external to_rdonly : 'a rd t -> ro t = "%identity"
  external unsafe_to_leaf : 'a t -> Leaf.t = "%identity"
  external unsafe_of_leaf : Leaf.t -> 'a t = "%identity"
  external unsafe_to_int : _ t -> int = "%identity"
  external unsafe_to_rdwr : _ t -> rdwr t = "%identity"

  let ( + ) addr v = addr + v [@@inline always]
end

module A = Addr

let string_of_null_addr = leintnat_to_string (Addr.null :> int)

type ('c, 'a) value =
  | Int8 : (atomic, int) value
  | LEInt : (atomic, int) value
  | LEInt16 : (atomic, int) value
  | LEInt31 : (atomic, int) value
  | LEInt64 : (atomic, int64) value
  | LEInt128 : (atomic, string) value
  (* XXX(dinosaure): a Int128 does not exist in OCaml, so we load it into a
       simple (little-endian) [string]. However, the access to the value must
       be atomic and be saved into a string then. *)
  | Addr_rd : (atomic, ro Addr.t) value
  | Addr_rdwr : (atomic, rdwr Addr.t) value
  | OCaml_string : (non_atomic, string) value
  | OCaml_string_length : (non_atomic, int) value

and atomic = Atomic
and non_atomic = Non_atomic

module type S = sig
  type 'a t
  type memory

  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val return : 'a -> 'a t
  val atomic_get : memory -> 'a rd Addr.t -> (atomic, 'v) value -> 'v t
  val atomic_set : memory -> 'a wr Addr.t -> (atomic, 'v) value -> 'v -> unit t
  val persist : memory -> 'a wr Addr.t -> len:int -> unit t
  val movnt64 : memory -> dst:'a wr Addr.t -> int -> unit t
  val set_n48_key : memory -> 'a wr Addr.t -> int -> int -> unit t
  val fetch_add : memory -> rdwr Addr.t -> (atomic, int) value -> int -> int t
  val fetch_or : memory -> rdwr Addr.t -> (atomic, int) value -> int -> int t
  val fetch_sub : memory -> rdwr Addr.t -> (atomic, int) value -> int -> int t

  val compare_exchange :
       memory
    -> ?weak:bool
    -> rdwr Addr.t
    -> (atomic, 'a) value
    -> 'a Atomic.t
    -> 'a
    -> bool t

  val pause_intrinsic : unit -> unit t
  val get : memory -> 'a rd Addr.t -> ('t, 'v) value -> 'v t

  val allocate :
    memory -> kind:[ `Leaf | `Node ] -> ?len:int -> string list -> rdwr Addr.t t

  val delete : memory -> _ Addr.t -> int -> unit t
  val collect : memory -> _ Addr.t -> len:int -> uid:int -> unit t
end

let[@coverage off] pp_value : type c a. (c, a) value Fmt.t =
 fun ppf -> function
  | LEInt -> Fmt.pf ppf "leintnat"
  | LEInt31 -> Fmt.pf ppf "leint31"
  | LEInt16 -> Fmt.pf ppf "leint16"
  | LEInt64 -> Fmt.pf ppf "leint64"
  | LEInt128 -> Fmt.pf ppf "leint128"
  | Int8 -> Fmt.pf ppf "int8"
  | Addr_rd -> Fmt.pf ppf "addr"
  | Addr_rdwr -> Fmt.pf ppf "addr"
  | OCaml_string -> Fmt.pf ppf "ocaml_string"
  | OCaml_string_length -> Fmt.pf ppf "leintnat"

let[@coverage off] pp_of_value :
    type c a. ?prefer_hex:bool -> (c, a) value -> a Fmt.t =
 fun ?(prefer_hex = false) -> function
  | LEInt ->
      fun ppf v ->
        if v < 0 || prefer_hex then Fmt.pf ppf "%16x" v else Fmt.pf ppf "%10d" v
  | LEInt31 -> Fmt.fmt "%10d"
  | LEInt16 -> Fmt.fmt "%5d"
  | LEInt64 -> Fmt.fmt "%19Ld"
  | LEInt128 -> Fmt.fmt "%S"
  | Int8 -> Fmt.fmt "%3d"
  | Addr_rd -> fun ppf addr -> Fmt.pf ppf "%016x" (addr :> int)
  | Addr_rdwr -> fun ppf addr -> Fmt.pf ppf "%016x" (addr :> int)
  | OCaml_string -> Fmt.fmt "%S"
  | OCaml_string_length -> Fmt.int

module Value = struct
  let int8 = Int8
  let leint16 = LEInt16
  let leint31 = LEInt31
  let leintnat = LEInt
  let leint64 = LEInt64
  let leint128 = LEInt128
  let addr_rd = Addr_rd
  let addr_rdwr = Addr_rdwr
  let ocaml_string = OCaml_string
  let ocaml_string_length = OCaml_string_length
end

let _cache_line_size = 64
let _write_latency_in_ns = 0
let _cpu_freq_mhz = 2100
let ( <.> ) f g x = f (g x)
let _prefix = 4
let _header_kind = 0

let _header_prefix =
  if Sys.word_size = 64 then _header_kind + 8 else _header_kind + 4

let _header_prefix_count = _header_prefix + _prefix
(* XXX(dinosaure): [prefix_count] is **not** the length of [prefix]
   if [prefix_count > _prefix]. In that case, we do a compression-path
   of [prefix_count] bytes and save only 4 bytes of this prefix into
   [prefix] to help a pessimistic check. *)

let _header_owner = _header_prefix_count + 4

(* XXX(dinosaure): [owner] is a unique identifier (like a PID) to tell us which
   process requires a node to be alive. It helps us to collect and re-use nodes
   which are not needed anymore by any readers.

   To ensure that the node can be re-used, we use this unique ID and keep
   globally who wants to read to the tree (readers) (into a [ring]). If this ID
   does not exist anymore inside our [ring], we can safely re-use the node. *)
let _header_depth =
  if Sys.word_size = 64 then _header_owner + 8 else _header_owner + 4
(* XXX(dinosaure): [depth] includes [prefix]. For example, we have a
   path-compression of 4-bytes into our **second**-level node (and parent of it
   has no prefix):

   depth = parent(depth) + length(prefix) + 1 = 5

   [depth] is a **constant**, it permits to set the prefix safely while reading.
   [find] trusts on this value first before to look into
   [prefix]/[prefix_count].
   /!\ [length(prefix) != prefix_count] - see [prefix_count].
       [length(prefix) == min _prefix prefix_count].
*)

let _header_count = _header_depth + 4
let _header_compact_count = _header_count + 2
let _header_length = _header_compact_count + 2

let () =
  match Sys.word_size = 64 with
  | true -> assert (_header_length = 32)
  | false -> assert (_header_length = 24)

let _bits_kind = Sys.word_size - 4
let _n4_kind = 0b00
let _n16_kind = 0b01
let _n48_kind = 0b10
let _n256_kind = 0b11

let _n4_align_length =
  let len = (_header_length + 4 + A.length) / A.length in
  (len * A.length) - (_header_length + 4)
(* XXX(dinosaure): to be sure that addresses are aligned, we fill the
 * gap between the header, 4 bytes (needed by N4) and addresses. By this
 * way, on [aarch64], we ensure that any accesses are aligned. *)

let _n4_branch addr i =
  A.(addr + _header_length + 4 + _n4_align_length + (A.length * i))

let _sizeof_n4 = _header_length + 4 + _n4_align_length + (4 * A.length)
let _sizeof_n16 = _header_length + 16 + (16 * A.length)
let _sizeof_n48 = _header_length + 256 + (48 * A.length)
let _sizeof_n256 = _header_length + (256 * A.length)
let _count = String.make 2 '\000'
let _compact_count = String.make 2 '\000'
let _n4_ks = String.make 4 '\000'
let _n4_vs = String.concat "" (List.init 4 (const string_of_null_addr))
let _n4_align = String.make _n4_align_length '\xff'

(* XXX(dinosaure): note for me, [msync(2)] does not ensure the **order** of
   writes and I'm really not sure about the use of it so we use [clflush] which
   flushes the memory [_cache_line_size] by [_cache_line_size]. By this way, we
   ensure that even if we don't control the order of writes on these areas, it
   seems that P-ART (see RECIPE paper) ensures orders in the design of ROWEX.

   The conversion action from ROWEX to P-ART is: Insert cache line flush
   and memory fence instructions after **each** [store].

   I got more informations, if we use [mmap] which is the case here (see
   [persistent.ml]), we ask to the kernel to map a file into memory and then
   expose this memory region into the application's virtual address space.

   Such region is treated as byte-addressable storage, Behind the scenes, page
   caching occurs, which is where the kernel pauses the application to perform
   the I/O operation, but the underlying storage can only talk in blocks
   ([pagesize]). So, even if a single byte is changed, the entire 4K block is
   moved to storage, which is not very efficient.

   So we must ensure that when we write something, we **really** write
   something to ensure the "power-fail" atomicity. [clflush] with [fence]
   help us about that when it ensure our write in a failure protected domain.
   [clflushopt] is **weakly** ordered (as [msync]) so we must follow it by
   an [sfence] instruction. *)

module Make (S : S) = struct
  let ( let* ) = S.bind
  let ( >>| ) x f = S.bind x (S.return <.> f)

  open S

  let get_version memory addr =
    atomic_get memory A.(addr + _header_kind) Value.leintnat
  [@@inline]

  let get_type memory addr =
    let* value = atomic_get memory A.(addr + _header_kind) Value.leintnat in
    return (value lsr _bits_kind)
  [@@inline]

  let get_prefix memory addr =
    (* XXX(dinosaure): may be we can optimize this part with [Value.leintnat]
       for a 64-bits architecture. However, we assume that 1 bit will disappear.
       Considering little-endian architecture, we probably should start with
       [prefix_count] and, then, [prefix]. [prefix_count] can ~safely~ fit into
       31 bits and [prefix] will use the rest (32 bits).

       By this way, we permit to use a native integer instead a boxed [int64].

       TODO!

       So we consider that the only layout possible of values such as [leint64]
       is a little-endian layout (no way!). I did a mistake about a possible
       abstraction over /endian/ but it's much more simpler to consider all with
       little-endian. By this way, the assumption between [prefix_count] and
       [prefix] remains and it's a good news. However, we should check that! *)
    let* value = atomic_get memory A.(addr + _header_prefix) Value.leint64 in
    let p0 = Int64.(to_int (logand value 0xffffL)) in
    let p1 = Int64.(to_int (logand (shift_right value 16) 0xffffL)) in
    let prefix = Bytes.create _prefix in
    bytes_set16 prefix 0 p0;
    bytes_set16 prefix 2 p1;
    return (Bytes.unsafe_to_string prefix, Int64.(to_int (shift_right value 32)))

  let get_compact_count m addr =
    atomic_get m A.(addr + _header_compact_count) Value.leint16
  [@@inline always]

  let get_count m addr = atomic_get m A.(addr + _header_count) Value.leint16
  [@@inline always]

  let get_depth m addr = atomic_get m A.(addr + _header_depth) Value.leint31
  [@@inline always]

  (* TODO(dinosaure): we actually store the depth into a 31-bits word. We can
     "optimize" [OCaml_string] and be sure that we are not able to have a key
     larger than 2^31 bytes (currently, the length of a string corresponds to
     2^64 or 2^32 according the machine because we use a [uintnat]). *)

  let set_prefix memory addr ~prefix ~prefix_count flush =
    if prefix_count = 0 then
      atomic_set memory A.(addr + _header_prefix) Value.leint64 0L
    else
      let p0 = string_get16 prefix 0 in
      let p1 = string_get16 prefix 2 in
      let prefix = Int64.(logor (shift_left (of_int p1) 16) (of_int p0)) in
      let rs = Int64.(logor (shift_left (of_int prefix_count) 32) prefix) in
      let* () = atomic_set memory A.(addr + _header_prefix) Value.leint64 rs in
      if flush then persist memory A.(addr + _header_prefix) ~len:8
      else return ()

  (**** FIND CHILD ****)

  let n4_find_child m addr k =
    let rec go i =
      if i < 4 then
        let kp = A.(addr + _header_length + i) in
        let b = _n4_branch addr i in
        let* k' = atomic_get m kp Value.int8 in
        let* c = atomic_get m b Value.addr_rd in
        if (not (Addr.is_null c)) && Char.code k == k' then return c
        else go (succ i)
      else return Addr.(to_rdonly null)
    in
    go 0

  external n16_get_child : int -> int -> string -> int = "caml_n16_get_child"
  [@@noalloc]

  external ctz : int -> int = "caml_ctz" [@@noalloc]

  let rec _n16_find_child m addr k bitfield =
    if bitfield = 0 then return A.(to_rdonly null)
    else
      let p = ctz bitfield in
      let b = A.(addr + _header_length + 16 + (p * A.length)) in
      let* k' = atomic_get m A.(addr + _header_length + p) Value.int8 in
      let* value = atomic_get m b Value.addr_rd in
      if (not (A.is_null value)) && k' = k lxor 128 then return value
      else _n16_find_child m addr k (bitfield lxor (1 lsl p))

  let n16_find_child m addr k =
    let* keys = atomic_get m A.(addr + _header_length) Value.leint128 in
    (* XXX(dinosaure): Dragoon here! How to load atomically a 128 bits integer
       and save it into a string? *)
    let bitfield = n16_get_child 16 k keys in
    _n16_find_child m addr k bitfield

  let n48_find_child m addr k =
    let* pos' = atomic_get m A.(addr + _header_length + k) Value.int8 in
    if pos' != 48 then
      let b = A.(addr + _header_length + 256 + (A.length * pos')) in
      atomic_get m b Value.addr_rd
    else return A.(to_rdonly null)

  let n256_find_child m addr k =
    atomic_get m A.(addr + _header_length + (A.length * k)) Value.addr_rd

  let rec _node_any_child m addr ~header child idx max =
    if idx = max then return child
    else
      let b = A.(addr + _header_length + header + (idx * A.length)) in
      let* child' = atomic_get m b Value.addr_rd in
      if not (Addr.is_null child') then
        if Addr.unsafe_to_int child' land 1 = 1 then return child'
        else _node_any_child m addr ~header child' (succ idx) max
      else _node_any_child m addr ~header child (succ idx) max
  [@@inline]

  let n4_any_child m addr =
    let header = 4 + _n4_align_length in
    _node_any_child m addr ~header A.(to_rdonly null) 0 4

  let n16_any_child m addr =
    _node_any_child m addr ~header:16 A.(to_rdonly null) 0 16

  let n48_any_child m addr =
    _node_any_child m addr ~header:256 A.(to_rdonly null) 0 48

  let n256_any_child m addr =
    _node_any_child m addr ~header:0 A.(to_rdonly null) 0 256

  let any_child m (addr : _ rd A.t) =
    let* ty = get_type m addr in
    match ty with
    | 0 -> n4_any_child m addr
    | 1 -> n16_any_child m addr
    | 2 -> n48_any_child m addr
    | 3 -> n256_any_child m addr
    | _ -> assert false

  let rec minimum : memory -> ro A.t -> ro A.t t =
   fun m addr ->
    if (addr :> int) land 1 = 1 then return addr
    else
      let* addr = any_child m addr in
      minimum m addr

  let minimum m (addr : _ rd A.t) = minimum m (A.to_rdonly addr) [@@inline]

  let pp_type_of_int ppf = function
    | 0 -> Fmt.string ppf "node4"
    | 1 -> Fmt.string ppf "node16"
    | 2 -> Fmt.string ppf "node48"
    | 3 -> Fmt.string ppf "node256"
    | _ -> assert false

  let pp_char ppf x =
    let code = Char.code x in
    if code >= 33 && code < 127 then Fmt.pf ppf "'%c'" x
    else Fmt.pf ppf "%02x" code

  let find_child m addr k =
    let* ty = get_type m addr in
    Log.debug (fun m ->
        m "find child with %a byte (%a)" pp_char k pp_type_of_int ty);
    match ty with
    | 0 -> n4_find_child m addr k
    | 1 -> n16_find_child m addr (Char.code k)
    | 2 -> n48_find_child m addr (Char.code k)
    | 3 -> n256_find_child m addr (Char.code k)
    | _ -> assert false

  let rec _check_prefix ~key ~prefix ~level idx max =
    if idx < max then
      if prefix.![idx] != key.![level] then raise Not_found
      else _check_prefix ~key ~prefix ~level:(succ level) (succ idx) max

  let check_prefix m addr ~key level =
    let* depth = get m A.(addr + _header_depth) Value.leint31 in
    if String.length key < depth then raise Not_found;
    let* prefix, prefix_count = get_prefix m addr in
    if prefix_count + level < depth then return depth
    else if prefix_count > 0 then begin
      let idx = level + prefix_count - depth
      and max = min prefix_count _prefix in
      _check_prefix ~key ~prefix ~level idx max;
      let level = level + (max - idx) in
      if prefix_count > _prefix then
        let level = level + (prefix_count - _prefix) in
        return level
      else return (-level)
    end
    else return (-level)

  let memcmp a b =
    if String.length a != String.length b then raise Not_found;
    let len = String.length a in
    let len0 = len land 3 in
    let len1 = len lsr 2 in
    for i = 0 to len1 - 1 do
      let i = i * 4 in
      if String.unsafe_get_uint32 a i <> String.unsafe_get_uint32 b i then
        raise Not_found
    done;
    for i = 0 to len0 - 1 do
      let i = (len1 * 4) + i in
      if a.[i] != b.[i] then raise Not_found
    done

  let memeq a b =
    if String.length a != String.length b then false
    else
      let len = String.length a in
      let len0 = len land 3 in
      let len1 = len lsr 2 in
      let i = ref 0 and continue = ref true in
      while !i < len1 && !continue do
        let x = !i * 4 in
        if String.unsafe_get_uint32 a x <> String.unsafe_get_uint32 b x then
          continue := false;
        incr i
      done;
      i := 0;
      while !i < len0 && !continue do
        let x = (len1 * 4) + !i in
        if a.[x] != b.[x] then continue := false;
        incr i
      done;
      !continue

  type pessimistic =
    | Match of { level : int }
    | Skipped_level
    | No_match of {
          non_matching_key : char
        ; non_matching_prefix : string
        ; level : int
      }

  let get_value_of_leaf m addr ~key =
    let len = (String.length key + size_of_word) / size_of_word in
    let len = 1 + len in
    let len = len * size_of_word in
    get m A.(addr + len) Value.leintnat

  let rec _lookup m n ~key ~optimistic_match level =
    let* res = check_prefix m n ~key level in
    Log.debug (fun m -> m "lookup: current node %016x" (Addr.unsafe_to_int n));
    Log.debug (fun m -> m "lookup: check prefix %d (level:%d)" res level);
    Log.debug (fun m -> m "lookup: optimistic? %b" (res >= 0));
    let optimistic_match, level =
      if res >= 0 then (true, res) else (optimistic_match, abs res)
    in
    if String.length key < level then raise Not_found;
    let* n = find_child m n key.![level] in
    if A.is_null n then raise Not_found;
    if (n :> int) land 1 = 1 then begin
      Log.debug (fun m -> m "lookup: found a leaf");
      let leaf = Leaf.prj (A.unsafe_to_leaf n) in
      if level < String.length key - 1 || optimistic_match then begin
        let* key' = get m leaf Value.ocaml_string in
        Log.debug (fun m -> m "find: memcmp %S %S" key key');
        memcmp key key';
        get_value_of_leaf m leaf ~key:key'
      end
      else get_value_of_leaf m leaf ~key
    end
    else begin
      Log.debug (fun m -> m "lookup: continue (%d => %d)" level (succ level));
      _lookup m n ~key ~optimistic_match (succ level)
    end

  let lookup m n key =
    let n = A.unsafe_to_int n in
    let n = A.of_int_to_rdonly n in
    _lookup m n ~key ~optimistic_match:false 0

  [@@@warning "-37"]

  type 'a succ = S : 'a succ
  type zero = Z

  [@@@warning "+37"]

  type 'a node =
    | N4 : rdwr A.t -> zero node
    | N16 : rdwr A.t -> zero succ node
    | N48 : rdwr A.t -> zero succ succ node
    | N256 : rdwr A.t -> zero succ succ succ node

  let addr_of : type a. a node -> rdwr A.t = function
    | N4 addr -> addr
    | N16 addr -> addr
    | N48 addr -> addr
    | N256 addr -> addr

  let size_of : type a. a node -> int = function
    | N4 _ -> _sizeof_n4
    | N16 _ -> _sizeof_n16
    | N48 _ -> _sizeof_n48
    | N256 _ -> _sizeof_n256

  let n4 addr = N4 addr
  let n16 addr = N16 addr
  let n48 addr = N48 addr
  let n256 addr = N256 addr

  let add_child_n256 m (N256 addr) k v flush =
    let* count = get_count m addr in
    if count >= 256 then return false
    else if flush then
      let dst = A.(addr + _header_length + (k * A.length)) in
      let* () = movnt64 m ~dst (Addr.unsafe_to_int v) in
      let* _ = fetch_add m A.(addr + _header_count) Value.leint16 1 in
      return true
    else
      let b = A.(addr + _header_length + (k * A.length)) in
      let* () = atomic_set m b Value.addr_rd (Addr.to_rdonly v) in
      let* _ = fetch_add m A.(addr + _header_count) Value.leint16 1 in
      return true

  let rec sync_ccount_n48 m addr =
    let* ccount = get_compact_count m addr in
    let b = A.(addr + _header_length + 256 + (ccount * A.length)) in
    let* children = atomic_get m b Value.addr_rd in
    if Addr.is_null children then return true
    else
      let* _ = fetch_add m A.(addr + _header_compact_count) Value.leint16 1 in
      let* ccount = get_compact_count m addr in
      if ccount == 48 then return false else sync_ccount_n48 m addr

  let add_child_n48 m (N48 addr) k v flush =
    Log.debug (fun m ->
        m "(node48) %016x[%a] <~ %016x" (Addr.unsafe_to_int addr) pp_char
          (Char.unsafe_chr k) (Addr.unsafe_to_int v));
    let* ccount = get_compact_count m addr in
    if ccount == 48 then return false
    else
      let* continue = sync_ccount_n48 m addr in
      if not continue then return false
      else if flush then begin
        let* ccount = get_compact_count m addr in
        let b = A.(addr + _header_length + 256 + (ccount * A.length)) in
        let* () = atomic_set m b Value.addr_rd (Addr.to_rdonly v) in
        let* ccount = get_compact_count m addr in
        let b = A.(addr + _header_length + 256 + (ccount * A.length)) in
        let* () = persist m b ~len:A.length in
        let child_index_64 = A.(addr + _header_length) in
        let* ccount = get_compact_count m addr in
        let* () = set_n48_key m child_index_64 k ccount in
        let* _ = fetch_add m A.(addr + _header_compact_count) Value.leint16 1 in
        let* _ = fetch_add m A.(addr + _header_count) Value.leint16 1 in
        return true
      end
      else
        let* ccount = get_compact_count m addr in
        let b = A.(addr + _header_length + 256 + (ccount * A.length)) in
        let* () = atomic_set m b Value.addr_rd (Addr.to_rdonly v) in
        let pk = A.(addr + _header_length + k) in
        let* ccount = get_compact_count m addr in
        let* () = atomic_set m pk Value.int8 ccount in
        let* _ = fetch_add m A.(addr + _header_compact_count) Value.leint16 1 in
        let* _ = fetch_add m A.(addr + _header_count) Value.leint16 1 in
        return false

  let add_child_n16 m (N16 addr) k v flush =
    let* ccount = get_compact_count m addr in
    if ccount == 16 then return false
    else
      let* i = fetch_add m A.(addr + _header_compact_count) Value.leint16 1 in
      let* _ = fetch_add m A.(addr + _header_count) Value.leint16 1 in
      let b = A.(addr + _header_length + 16 + (i * A.length)) in
      let pk = A.(addr + _header_length + i) in
      if flush then
        let* () = atomic_set m pk Value.int8 (k lxor 128) in
        (* NOTE(dinosaure): this [clflush] will failure-atomically flush the
           cache line including counters and entire key entries. *)
        let* () = persist m addr ~len:8 in
        let* () = movnt64 m ~dst:b (Addr.unsafe_to_int v) in
        let* () = persist m b ~len:A.length in
        return true
      else
        let* () = atomic_set m pk Value.int8 (k lxor 128) in
        let* () = atomic_set m b Value.addr_rd (Addr.to_rdonly v) in
        return true

  let add_child_n4 m (N4 addr) k v flush =
    let* ccount = get_compact_count m addr in
    if ccount == 4 then return false
    else
      let* i = fetch_add m A.(addr + _header_compact_count) Value.leint16 1 in
      let* _ = fetch_add m A.(addr + _header_count) Value.leint16 1 in
      let b = _n4_branch addr i in
      let pk = A.(addr + _header_length + i) in
      if flush then
        let* () = atomic_set m pk Value.int8 k in
        let* () = persist m addr ~len:_sizeof_n4 in
        let* () = movnt64 m ~dst:b (Addr.unsafe_to_int v) in
        return true
      else
        let* () = atomic_set m pk Value.int8 k in
        let* () = atomic_set m b Value.addr_rd (Addr.to_rdonly v) in
        return true

  let add_child :
      type s. memory -> s node -> char -> 'c rd Addr.t -> bool -> bool t =
   fun m n k v flush ->
    let k = Char.code k in
    match n with
    | N4 _ as n -> add_child_n4 m n k v flush
    | N16 _ as n -> add_child_n16 m n k v flush
    | N48 _ as n -> add_child_n48 m n k v flush
    | N256 _ as n -> add_child_n256 m n k v flush

  let write_unlock m addr =
    let* _ = fetch_add m A.(addr + _header_kind) Value.leintnat 0b10 in
    return (Log.debug (fun m -> m "%016x unlocked" (Addr.unsafe_to_int addr)))

  let write_unlock_and_obsolete m addr =
    let* v = fetch_add m A.(addr + _header_kind) Value.leintnat 0b11 in
    return
      (Log.debug (fun m ->
           m "%016x:%016x unlocked & obsolete" (Addr.unsafe_to_int addr) v))

  let is_obsolete version = version land 1 = 1

  let read_unlock_or_restart m addr expected =
    let* value = atomic_get m A.(addr + _header_kind) Value.leintnat in
    return (expected = value)
  [@@inline]

  let rec until_is_locked m addr version retries =
    if retries > 1000 then
      Log.err (fun m ->
          m "%016x locked for a too long time (%d)" (Addr.unsafe_to_int addr)
            retries);
    if version land 0b10 = 0b10 then
      let* () = pause_intrinsic () in
      let* version = atomic_get m A.(addr + _header_kind) Value.leintnat in
      until_is_locked m addr version (succ retries)
    else return version
  [@@inline]

  let rec write_lock_or_restart m addr need_to_restart =
    let* version = atomic_get m A.(addr + _header_kind) Value.leintnat in
    let* version = until_is_locked m addr version 0 in
    if is_obsolete version then begin
      need_to_restart := true;
      return ()
    end
    else
      let* res =
        compare_exchange m ~weak:true
          A.(addr + _header_kind)
          Value.leintnat (Atomic.make version) (version + 0b10)
      in
      if not res then write_lock_or_restart m addr need_to_restart
      else
        return (Log.debug (fun m -> m "%016x locked" (Addr.unsafe_to_int addr)))
  [@@inline]

  let write_unlock_obsolete m addr =
    fetch_add m A.(addr + _header_kind) Value.leintnat 0b11

  let lock_version_or_restart m addr version need_to_restart =
    if version land 0b10 = 0b10 || version land 1 = 1 then begin
      need_to_restart := true;
      return version
    end
    else
      let* set =
        compare_exchange m
          A.(addr + _header_kind)
          Value.leintnat (Atomic.make version) (version + 0b10)
      in
      if set then begin
        Log.debug (fun m -> m "%016x locked" (Addr.unsafe_to_int addr));
        return (version + 0b10)
      end
      else begin
        need_to_restart := true;
        return version
      end

  let rec _n4_update_child m (addr : 'a wr A.t) k ptr i =
    let* compact_count = get_compact_count m addr in
    if i < compact_count then
      let b =
        A.(addr + _header_length + 4 + _n4_align_length + (i * A.length))
      in
      let* key = atomic_get m A.(addr + _header_length + i) Value.int8 in
      let* child = atomic_get m b Value.addr_rd in
      if (not (A.is_null child)) && key = k then
        movnt64 m ~dst:b (Addr.unsafe_to_int ptr)
      else _n4_update_child m addr k ptr (succ i)
    else begin
      Log.err (fun m ->
          m "%016x (node4) is not in-sync" (Addr.unsafe_to_int addr));
      assert false
    end

  let n4_update_child m addr k ptr = _n4_update_child m addr k ptr 0

  let rec _n16_child_pos m addr k bitfield : rdwr A.t t =
    if bitfield = 0 then begin
      Log.err (fun m ->
          m "%016x (node16) is not in-sync" (Addr.unsafe_to_int addr));
      assert false
    end
    else
      let p = ctz bitfield in
      let* k' = atomic_get m A.(addr + _header_length + p) Value.int8 in
      let kp = A.(addr + _header_length + 16 + (p * A.length)) in
      let* value = atomic_get m kp Value.addr_rd in
      if (not (A.is_null value)) && k' = k lxor 128 then
        return A.(addr + _header_length + 16 + (p * A.length))
      else _n16_child_pos m addr k (bitfield lxor (1 lsl p))

  let _n16_child_pos m addr k =
    let* compact_count = get_compact_count m addr in
    let* keys = atomic_get m A.(addr + _header_length) Value.leint128 in
    let bitfield = n16_get_child compact_count k keys in
    _n16_child_pos m addr k bitfield

  let n16_update_child m addr k ptr =
    let* addr = _n16_child_pos m addr k in
    let addr = A.of_int_to_wronly (addr :> int) in
    movnt64 m ~dst:addr (Addr.unsafe_to_int ptr)

  let n48_update_child m addr k ptr =
    let* i = atomic_get m A.(addr + _header_length + k) Value.int8 in
    let b = A.(addr + _header_length + 256 + (i * A.length)) in
    movnt64 m ~dst:b (Addr.unsafe_to_int ptr)

  let n256_update_child m addr k ptr =
    let b = A.(addr + _header_length + (k * A.length)) in
    movnt64 m ~dst:b (Addr.unsafe_to_int ptr)

  let update_child : memory -> 'c0 wr A.t -> char -> 'c1 rd A.t -> unit t =
   fun m addr k ptr ->
    let* ty = get_type m addr in
    Log.debug (fun m ->
        m "%016x[%a] <- %016x" (A.unsafe_to_int addr) pp_char k
          (A.unsafe_to_int ptr));
    let k = Char.code k in
    match ty with
    | 0 -> n4_update_child m addr k ptr
    | 1 -> n16_update_child m addr k ptr
    | 2 -> n48_update_child m addr k ptr
    | 3 -> n256_update_child m addr k ptr
    | _ -> assert false

  let alloc_n4 m ~prefix:p ~prefix_count ~level =
    Log.debug (fun m ->
        m "allocation of a <n4> (prefix:%S, prefix_count:%d, level:%d)" p
          prefix_count level);
    let prefix = Bytes.make 4 '\000' in
    Bytes.blit_string p 0 prefix 0 (min _prefix (String.length p));
    let prefix_count = leint31_to_string prefix_count in
    let k = leintnat_to_string ((_n4_kind lsl _bits_kind) lor 0b100) in
    let o = leintnat_to_string 0 in
    let l = leint31_to_string level in
    allocate m ~kind:`Node
      [
        k; Bytes.unsafe_to_string prefix; prefix_count; o; l; _count
      ; _compact_count; _n4_ks; _n4_align; _n4_vs
      ]
      ~len:_sizeof_n4
    >>| n4

  let _n16_ks = String.make 16 '\000'
  let _n16_vs = String.concat "" (List.init 16 (const string_of_null_addr))

  let alloc_n16 m ~prefix:p ~prefix_count ~level =
    let prefix = Bytes.make 4 '\000' in
    Bytes.blit_string p 0 prefix 0 (min _prefix (String.length p));
    let prefix_count = leint31_to_string prefix_count in
    let k = leintnat_to_string ((_n16_kind lsl _bits_kind) lor 0b100) in
    let o = leintnat_to_string 0 in
    let l = leint31_to_string level in
    allocate m ~kind:`Node
      [
        k; Bytes.unsafe_to_string prefix; prefix_count; o; l; _count
      ; _compact_count; _n16_ks; _n16_vs
      ]
      ~len:_sizeof_n16
    >>| n16

  let _n48_ks = String.make 256 '\048'
  let _n48_vs = String.concat "" (List.init 48 (const string_of_null_addr))

  let alloc_n48 m ~prefix:p ~prefix_count ~level =
    let prefix = Bytes.make 4 '\000' in
    Bytes.blit_string p 0 prefix 0 (min _prefix (String.length p));
    let prefix_count = leint31_to_string prefix_count in
    let k = leintnat_to_string ((_n48_kind lsl _bits_kind) lor 0b100) in
    let o = leintnat_to_string 0 in
    let l = leint31_to_string level in
    allocate m ~kind:`Node
      [
        k; Bytes.unsafe_to_string prefix; prefix_count; o; l; _count
      ; _compact_count; _n48_ks; _n48_vs
      ]
      ~len:_sizeof_n48
    >>| n48

  let _n256_vs = String.concat "" (List.init 256 (const string_of_null_addr))

  let alloc_n256 m ~prefix:p ~prefix_count ~level =
    let prefix = Bytes.make 4 '\000' in
    Bytes.blit_string p 0 prefix 0 (min _prefix (String.length p));
    let prefix_count = leint31_to_string prefix_count in
    let k = leintnat_to_string ((_n256_kind lsl _bits_kind) lor 0b100) in
    let o = leintnat_to_string 0 in
    let l = leint31_to_string level in
    allocate m ~kind:`Node
      [
        k; Bytes.unsafe_to_string prefix; prefix_count; o; l; _count
      ; _compact_count; _n256_vs
      ]
      ~len:_sizeof_n256
    >>| n256

  let alloc :
      type s.
         memory
      -> according_to:s node
      -> prefix:string
      -> prefix_count:int
      -> level:int
      -> s node t =
   fun m ~according_to ~prefix ~prefix_count ~level ->
    match according_to with
    | N4 _ -> alloc_n4 m ~prefix ~prefix_count ~level
    | N16 _ -> alloc_n16 m ~prefix ~prefix_count ~level
    | N48 _ -> alloc_n48 m ~prefix ~prefix_count ~level
    | N256 _ -> alloc_n256 m ~prefix ~prefix_count ~level

  let alloc_bigger :
      type s.
         memory
      -> according_to:s node
      -> prefix:string
      -> prefix_count:int
      -> level:int
      -> s succ node t =
   fun m ~according_to ~prefix ~prefix_count ~level ->
    match according_to with
    | N4 _ -> alloc_n16 m ~prefix ~prefix_count ~level
    | N16 _ -> alloc_n48 m ~prefix ~prefix_count ~level
    | N48 _ -> alloc_n256 m ~prefix ~prefix_count ~level
    | N256 _ -> assert false

  let rec _copy_n4_into : type s. memory -> zero node -> s node -> int -> unit t
      =
   fun m (N4 addr as nx) ny i ->
    let* ccount = get_compact_count m addr in
    if i >= ccount then return ()
    else
      let b = _n4_branch addr i in
      let* value = atomic_get m b Value.addr_rd in
      match A.is_null value with
      | true -> (_copy_n4_into [@tailcall]) m nx ny (succ i)
      | false ->
          let kp = A.(addr + _header_length + i) in
          let* k = atomic_get m kp Value.int8 in
          let* _ = add_child m ny (Char.unsafe_chr k) value false in
          (* XXX(dinosaure): assert (_ = true); *)
          (_copy_n4_into [@tailcall]) m nx ny (succ i)

  let copy_n4_into : type s. memory -> zero node -> s node -> unit t =
   fun m (N4 _ as nx) ny -> _copy_n4_into m nx ny 0

  let rec _copy_n16_into :
      type s. memory -> zero succ node -> s node -> int -> unit t =
   fun m (N16 addr as nx) ny i ->
    let* ccount = get_compact_count m addr in
    if i >= ccount then return ()
    else
      let b = A.(addr + _header_length + 16 + (i * A.length)) in
      let* value = atomic_get m b Value.addr_rd in
      match A.is_null value with
      | true -> _copy_n16_into m nx ny (succ i)
      | false ->
          let kp = A.(addr + _header_length + i) in
          let* k = atomic_get m kp Value.int8 in
          let* _ = add_child m ny (Char.unsafe_chr (k lxor 128)) value false in
          _copy_n16_into m nx ny (succ i)

  let copy_n16_into : type s. memory -> zero succ node -> s node -> unit t =
   fun m (N16 _ as nx) ny -> _copy_n16_into m nx ny 0

  let rec _copy_n48_into :
      type s. memory -> zero succ succ node -> s node -> int -> unit t =
   fun m (N48 addr as nx) ny i ->
    if i == 256 then return ()
    else
      let* j = atomic_get m A.(addr + _header_length + i) Value.int8 in
      match j with
      | 48 -> _copy_n48_into m nx ny (succ i)
      | _ ->
          let b = A.(addr + _header_length + 256 + (j * A.length)) in
          let* value = atomic_get m b Value.addr_rd in
          let* _ = add_child m ny (Char.unsafe_chr i) value false in
          (* XXX(dinosaure): assert (_ = true); *)
          _copy_n48_into m nx ny (succ i)

  let copy_n48_into : type s. memory -> zero succ succ node -> s node -> unit t
      =
   fun m (N48 _ as nx) ny -> _copy_n48_into m nx ny 0

  let rec _copy_n256_into :
      type s. memory -> zero succ succ succ node -> s node -> int -> unit t =
   fun m (N256 addr as nx) ny i ->
    if i == 256 then return ()
    else
      let b = A.(addr + _header_length + (i * A.length)) in
      let* value = atomic_get m b Value.addr_rd in
      match A.is_null value with
      | true -> _copy_n256_into m nx ny (succ i)
      | false ->
          let* _ = add_child m ny (Char.unsafe_chr i) value false in
          _copy_n256_into m nx ny (succ i)

  let copy_n256_into :
      type s. memory -> zero succ succ succ node -> s node -> unit t =
   fun m (N256 _ as nx) ny -> _copy_n256_into m nx ny 0

  let copy_into : type s. memory -> s node -> s succ node -> unit t =
   fun m nx ny ->
    match nx with
    | N4 _ -> copy_n4_into m nx ny
    | N16 _ -> copy_n16_into m nx ny
    | N48 _ -> copy_n48_into m nx ny
    | N256 _ -> copy_n256_into m nx ny

  let insert_grow :
      type s.
         memory
      -> s node
      -> 'a wr Addr.t
      -> char
      -> char
      -> 'a rd Addr.t
      -> bool ref
      -> unit t =
   fun m n p k kp v need_to_restart ->
    let addr = addr_of n in
    Log.debug (fun m ->
        m "insert: %016x[%a] <- %016x"
          (Addr.unsafe_to_int (addr_of n))
          pp_char k (Addr.unsafe_to_int v));
    let* inserted = add_child m n k v true in
    if inserted then write_unlock m addr
    else
      let* prefix, prefix_count = get_prefix m addr in
      let* level = get_depth m addr in
      let* n' = alloc_bigger m ~according_to:n ~prefix ~prefix_count ~level in
      let* () = copy_into m n n' in
      let* _ = add_child m n' k v false in
      (* XXX(dinosaure): assert (_ = true); *)
      let* () = write_lock_or_restart m p need_to_restart in
      if !need_to_restart then
        let size' = size_of n' in
        let addr' = addr_of n' in
        let* () = delete m addr' size' in
        write_unlock m addr
      else
        let size' = size_of n' in
        let addr' = addr_of n' in
        let* () = persist m addr' ~len:size' in
        let* () = update_child m p kp (Addr.to_rdonly addr') in
        let* () = write_unlock m p in
        let* () = write_unlock_and_obsolete m addr in
        let size = size_of n in
        let* uid = atomic_get m A.(addr + _header_owner) Value.leintnat in
        collect m addr ~len:size ~uid

  let copy_into : type s. memory -> s node -> s node -> unit t =
   fun m nx ny ->
    match nx with
    | N4 _ -> copy_n4_into m nx ny
    | N16 _ -> copy_n16_into m nx ny
    | N48 _ -> copy_n48_into m nx ny
    | N256 _ -> copy_n256_into m nx ny

  let insert_compact :
      type s.
         memory
      -> s node
      -> 'a wr Addr.t
      -> char
      -> char
      -> 'a rd Addr.t
      -> bool ref
      -> bool t =
   fun m n p k kp v need_to_restart ->
    let addr = addr_of n in
    let* prefix, prefix_count = get_prefix m addr in
    let* level = get_depth m addr in
    let* n' = alloc m ~according_to:n ~prefix ~prefix_count ~level in
    let* () = copy_into m n n' in
    let* added = add_child m n' k v false in
    if not added then
      let size' = size_of n' in
      let addr' = addr_of n' in
      let* () = delete m addr' size' in
      return false
    else
      let* () = write_lock_or_restart m p need_to_restart in
      if !need_to_restart then
        let size' = size_of n' in
        let addr' = addr_of n' in
        let* () = delete m addr' size' in
        let* () = write_unlock m addr in
        return true
      else
        let size' = size_of n' in
        let addr' = addr_of n' in
        let* () = persist m addr' ~len:size' in
        let* () = update_child m p kp (A.to_rdonly addr') in
        let* () = write_unlock m p in
        let* () = write_unlock_and_obsolete m addr in
        let size = size_of n in
        let* uid = atomic_get m A.(addr + _header_owner) Value.leintnat in
        let* () = collect m addr ~len:size ~uid in
        return true

  let minimum_key m addr =
    let* leaf = minimum m addr in
    get m (Leaf.prj (A.unsafe_to_leaf leaf)) Value.ocaml_string

  let check_prefix_pessimistic m addr ~key level =
    let* prefix, prefix_count = get_prefix m addr in
    Log.debug (fun m ->
        m "insertion: check pessimistic %S:%d" prefix prefix_count);
    let* depth = get_depth m addr in
    let* res =
      if prefix_count + level != depth then begin
        let need_to_recover = ref false in
        let* v = get_version m addr in
        let* _ = lock_version_or_restart m addr v need_to_recover in
        let* prefix, prefix_count =
          if !need_to_recover = false then begin
            Log.debug (fun m -> m "insertion: inconsistent state");
            let dis = if depth > level then depth - level else level - depth in
            let* kr = minimum_key m addr in
            let prefix_count = dis in
            let prefix = Bytes.make _prefix '\000' in
            for i = 0 to min dis _prefix - 1 do
              Bytes.set prefix i kr.![level + i]
            done;
            let prefix = Bytes.unsafe_to_string prefix in
            Log.debug (fun m ->
                m "insertion: set prefix to %S:%d" prefix prefix_count);
            let* () = set_prefix m addr ~prefix ~prefix_count true in
            let* () = write_unlock m addr in
            return (prefix, prefix_count)
          end
          else return (prefix, prefix_count)
        in
        if prefix_count + level < depth then return None
        else return (Some (prefix, prefix_count))
      end
      else return (Some (prefix, prefix_count))
    in
    match res with
    | None -> return Skipped_level
    | Some (prefix, prefix_count) ->
        Log.debug (fun m ->
            m "insertion: check prefix (pessimistic) with %S:%d" prefix
              prefix_count);
        if prefix_count > 0 then begin
          let level' = level in
          let kt = Lazy.from_fun @@ fun () -> minimum_key m addr in
          let rec go level i =
            if i < prefix_count then begin
              if i == _prefix then ignore (Lazy.force kt);
              let* cur =
                if i >= _prefix then
                  let* kt = Lazy.force kt in
                  return kt.![level]
                else return prefix.![i]
              in
              if cur != key.![level] then begin
                let non_matching_key = cur in
                let* non_matching_prefix =
                  if prefix_count > _prefix then begin
                    if i < _prefix then ignore (Lazy.force kt);
                    let* kt = Lazy.force kt in
                    let non_matching_prefix = Bytes.make _prefix '\000' in
                    let top =
                      min (prefix_count - (level - level') - 1) _prefix
                    in
                    for j = 0 to top - 1 do
                      Bytes.set non_matching_prefix j kt.![level + j + 1]
                    done;
                    return (Bytes.unsafe_to_string non_matching_prefix)
                  end
                  else
                    let non_matching_prefix = Bytes.make _prefix '\000' in
                    let top = prefix_count - i - 1 in
                    for j = 0 to top - 1 do
                      Bytes.set non_matching_prefix j prefix.![i + j + 1]
                    done;
                    return (Bytes.unsafe_to_string non_matching_prefix)
                in
                return
                  (No_match { non_matching_key; non_matching_prefix; level })
              end
              else go (succ level) (succ i)
            end
            else return (Match { level })
          in
          go level (level + prefix_count - depth)
        end
        else return (Match { level })

  let insert_and_unlock m addr p k kp v need_to_restart =
    let* ty = get_type m addr in
    match ty with
    | 0 ->
        let* ccount = get_compact_count m addr in
        let* count = get_count m addr in
        if ccount == 4 && count <= 3 then
          let* res = insert_compact m (N4 addr) p k kp v need_to_restart in
          if not res then insert_grow m (N4 addr) p k kp v need_to_restart
          else return ()
        else insert_grow m (N4 addr) p k kp v need_to_restart
    | 1 ->
        let* ccount = get_compact_count m addr in
        let* count = get_count m addr in
        if ccount == 16 && count <= 14 then
          let* res = insert_compact m (N16 addr) p k kp v need_to_restart in
          if not res then insert_grow m (N16 addr) p k kp v need_to_restart
          else return ()
        else insert_grow m (N16 addr) p k kp v need_to_restart
    | 2 ->
        let* ccount = get_compact_count m addr in
        let* count = get_count m addr in
        if ccount == 48 && count != 48 then
          let* res = insert_compact m (N48 addr) p k kp v need_to_restart in
          if not res then insert_grow m (N48 addr) p k kp v need_to_restart
          else return ()
        else insert_grow m (N48 addr) p k kp v need_to_restart
    | 3 ->
        Log.debug (fun m ->
            m "insert: %016x[%02x] <- %016x (node256)" (A.unsafe_to_int addr)
              (Char.code k) (A.unsafe_to_int v));
        let* res = add_child m (N256 addr) k v true in
        if res then write_unlock m addr
        else
          let* _ = insert_compact m (N256 addr) p k kp v need_to_restart in
          return ()
    | _ -> assert false

  let _check_or_raise_duplicate ~level:off a b =
    if String.length a = String.length b then (
      let idx = ref (String.length a - 1) in
      while !idx >= off && a.[!idx] = b.[!idx] do
        decr idx
      done;
      if !idx < off then raise Duplicate)

  let rec insert m root key leaf =
    let retries = ref 0 in
    let rec restart () =
      incr retries;
      if !retries > 100 then
        Log.err (fun m -> m "Too many retries to insert %S" (key :> string));
      Log.debug (fun m -> m "insert: retry");
      (insert [@tailcall]) m root key leaf
    and _insert node next_node _parent kn level =
      let need_to_restart = ref false in
      let parent = node in
      let kp = kn in
      let node = next_node in
      let* v = get_version m node in
      Log.debug (fun m ->
          m "insertion: walk into %016x and check prefix" (A.unsafe_to_int node));
      let* res = check_prefix_pessimistic m node ~key level in
      match res with
      | Skipped_level ->
          Log.debug (fun m -> m "insertion: skipped level");
          restart ()
      | No_match { non_matching_key; non_matching_prefix; level = next_level }
        ->
          Log.debug (fun m ->
              m "insertion: no match %a %S (%d => %d)" pp_char non_matching_key
                non_matching_prefix level next_level);
          let* _ = lock_version_or_restart m node v need_to_restart in
          if !need_to_restart then (restart [@tailcall]) ()
          else
            let* prefix, _ = get_prefix m node in
            let prefix_count = next_level - level in
            (* 1) create a new node which will be parent of node,
                  set common prefix,
                  level to this node *)
            let* n' = alloc_n4 m ~prefix ~prefix_count ~level:next_level in
            (* 2) add node and leaf as children *)
            let* _ = add_child m n' key.![next_level] leaf false in
            let* _ = add_child m n' non_matching_key (A.to_rdonly node) false in
            let* () = persist m (addr_of n') ~len:_sizeof_n4 in
            (* 3) lock_version_or_restart,
                  update parent to point to the new node,
                  unlock *)
            let* () = write_lock_or_restart m parent need_to_restart in
            if !need_to_restart then
              let* () = delete m (addr_of n') _sizeof_n4 in
              let* () = write_unlock m node in
              (restart [@tailcall]) ()
            else
              let* () = update_child m parent kp (A.to_rdonly (addr_of n')) in
              let* () = write_unlock m parent in
              let* () =
                let* _, prefix_count = get_prefix m node in
                Log.debug (fun m ->
                    m "insert: prefix count = %d - ((%d - %d) + 1)" prefix_count
                      next_level level);
                let prefix_count = prefix_count - (next_level - level + 1) in
                Log.debug (fun m ->
                    m "insert: set prefix %S:%d" non_matching_prefix
                      prefix_count);
                set_prefix m node ~prefix:non_matching_prefix ~prefix_count true
              in
              let* () = write_unlock m node in
              return ()
      | Match { level = next_level } ->
          Log.debug (fun m ->
              m "insertion: match (level:%d => %d)" level next_level);
          let level = next_level in
          let kn = key.![level] in
          let* next_node = find_child m node kn in
          if Addr.is_null next_node then
            let* _ = lock_version_or_restart m node v need_to_restart in
            if !need_to_restart then (restart [@tailcall]) ()
            else
              let () =
                Log.debug (fun m ->
                    m "insertion: the branch is free, insert the leaf")
              in
              let* () =
                insert_and_unlock m node parent kn kp leaf need_to_restart
              in
              if !need_to_restart then (restart [@tailcall]) () else return ()
          else if (next_node :> int) land 1 = 1 then begin
            Log.debug (fun m -> m "insertion: the next node is a leaf");
            let* _ = lock_version_or_restart m node v need_to_restart in
            if !need_to_restart then (restart [@tailcall]) ()
            else
              let* key' =
                let leaf = Leaf.prj (A.unsafe_to_leaf next_node) in
                get m leaf Value.ocaml_string
              in
              let level = level + 1 in
              Log.debug (fun m ->
                  m "insertion: match with a leaf %S %S" key' key);
              (* TODO(dinosaure): we probably can do something smarter
                 then [=]. [next_level] can be used to check only a part
                 of [key] instead of the whole string. *)
              if key' = key then begin
                let* () = write_unlock m node in
                raise Duplicate
              end
              else begin
                let pl = ref 0 in
                while key'.![level + !pl] == key.![level + !pl] do
                  incr pl
                done;
                let* n' =
                  let prefix = String.sub key level (min !pl _prefix) in
                  alloc_n4 m ~prefix ~prefix_count:!pl ~level:(level + !pl)
                in
                let* _ = add_child m n' key.![level + !pl] leaf false in
                let* _ = add_child m n' key'.![level + !pl] next_node false in
                let* () = persist m (addr_of n') ~len:_sizeof_n4 in
                let* () =
                  update_child m node key.[level - 1] (A.to_rdonly (addr_of n'))
                in
                let* () = write_unlock m node in
                return ()
              end
          end
          else
            _insert node (Addr.unsafe_to_rdwr next_node) parent kn (succ level)
    in
    let null = A.(of_int_to_rdwr (null :> int)) in
    _insert null root null '\000' 0

  let _n4_get_second_child m addr key =
    let rec go i =
      let* compact_count =
        atomic_get m A.(addr + _header_compact_count) Value.leint16
      in
      if i < compact_count then
        let* child =
          atomic_get m
            A.(addr + _header_length + 4 + _n4_align_length + (A.length * i))
            Value.addr_rdwr
        in
        if not (A.is_null child) then
          let* k = atomic_get m A.(addr + _header_length + i) Value.int8 in
          if k != key then return (child, Char.unsafe_chr k) else go (succ i)
        else go (succ i)
      else return (A.null, '\000')
    in
    go 0

  let _get_second_child m addr chr =
    let key = Char.code chr in
    let* ty = get_type m addr in
    match ty with 0 -> _n4_get_second_child m addr key | _ -> assert false

  let _n4_remove m ~force:_ ?(flush = true) addr key =
    let rec go i =
      let* compact_count = get_compact_count m addr in
      if i < compact_count then
        let cp =
          A.(addr + _header_length + 4 + _n4_align_length + (A.length * i))
        in
        let kp = A.(addr + _header_length + i) in
        let* child = atomic_get m cp Value.addr_rdwr in
        let* k = atomic_get m kp Value.int8 in
        if (not (A.is_null child)) && k == key then
          let b =
            A.(addr + _header_length + 4 + _n4_align_length + (i * A.length))
          in
          let* () = atomic_set m b Value.addr_rd A.(to_rdonly null) in
          let* () = if flush then persist m cp ~len:A.length else return () in
          let* _ = fetch_sub m A.(addr + _header_count) Value.leint16 1 in
          return true
        else go (succ i)
      else return false
    in
    go 0

  let _n16_remove m ~force ?(flush = true) addr key =
    let* count = atomic_get m A.(addr + _header_count) Value.leint16 in
    if count <= 3 && not force then return false
    else
      let* c = _n16_child_pos m addr key in
      let* () = atomic_set m A.(to_wronly c) Value.addr_rd A.(to_rdonly null) in
      let* () =
        if flush then persist m A.(to_wronly c) ~len:A.length else return ()
      in
      let* _ = fetch_sub m A.(addr + _header_count) Value.leint16 1 in
      return true

  let _n48_remove m ~force ?(flush = true) addr key =
    let* count = atomic_get m A.(addr + _header_count) Value.leint16 in
    if count <= 12 && not force then return false
    else
      let* () =
        if flush then
          let kp = A.(addr + _header_length + key) in
          let* i = atomic_get m kp Value.int8 in
          let c = A.(addr + _header_length + 256 + (i * A.length)) in
          let* () =
            atomic_set m A.(to_wronly c) Value.addr_rd A.(to_rdonly null)
          in
          let* () = persist m A.(to_wronly c) ~len:A.length in
          let* () = atomic_set m kp Value.int8 48 in
          persist m A.(to_wronly (addr + _header_length + (key / 8))) ~len:64
        else
          let kp = A.(addr + _header_length + key) in
          let* i = atomic_get m kp Value.int8 in
          let c = A.(addr + _header_length + 256 + (i * A.length)) in
          let* () =
            atomic_set m A.(to_wronly c) Value.addr_rd A.(to_rdonly null)
          in
          atomic_set m kp Value.int8 48
      in
      let* _ = fetch_sub m A.(addr + _header_count) Value.leint16 1 in
      return true

  let _n256_remove m ~force ?(flush = true) addr key =
    let* count = atomic_get m A.(addr + _header_count) Value.leint16 in
    if count <= 37 && not force then return false
    else
      let leaf = A.(addr + _header_length + (A.length * key)) in
      let* () =
        atomic_set m A.(to_wronly leaf) Value.addr_rd A.(to_rdonly null)
      in
      let* () =
        if flush then persist m A.(to_wronly leaf) ~len:A.length else return ()
      in
      let* _ = fetch_sub m A.(addr + _header_count) Value.leint16 1 in
      return true

  let _remove m ~force ?flush addr chr =
    let k = Char.code chr in
    let* ty = get_type m addr in
    match ty with
    | 0 -> _n4_remove m ~force ?flush addr k
    | 1 -> _n16_remove m ~force ?flush addr k
    | 2 -> _n48_remove m ~force ?flush addr k
    | 3 -> _n256_remove m ~force ?flush addr k
    | _ -> assert false

  let shrink :
      type v.
         memory
      -> v succ node
      -> v node
      -> _ A.t
      -> char
      -> char
      -> bool ref
      -> unit t =
   fun m n n_small p k kp need_to_restart ->
    let* () = write_lock_or_restart m p need_to_restart in
    let size' = size_of n_small in
    let addr' = addr_of n_small in
    if !need_to_restart then
      let* () = delete m addr' size' in
      write_unlock m (addr_of n)
    else
      let* _ = _remove m ~force:true ~flush:true (addr_of n) k in
      let* () =
        match (n, n_small) with
        | N16 _, N4 _ -> copy_n16_into m n n_small
        | N48 _, N16 _ -> copy_n48_into m n n_small
        | N256 _, N48 _ -> copy_n256_into m n n_small
      in
      let* () = persist m A.(to_wronly addr') ~len:size' in
      let* () = update_child m p kp (A.to_rdonly addr') in
      let* () = write_unlock m p in
      let* _ = write_unlock_obsolete m (addr_of n) in
      let* uid = atomic_get m A.(addr_of n + _header_owner) Value.leintnat in
      let n_length = size_of n in
      collect m (addr_of n) ~len:n_length ~uid

  let remove_and_shrink m n p k kp need_to_restart =
    let* res = _remove m ~force:(A.is_null p) ~flush:true n k in
    if res then write_unlock m n
    else
      let* ty = get_type m n in
      (* TODO(dinosaure): repetition with [remove_and_unlock]. *)
      let* level = get m A.(n + _header_depth) Value.leint31 in
      let* prefix, prefix_count = get_prefix m n in
      match ty with
      | 1 ->
          let* n_small = alloc_n4 m ~prefix ~prefix_count ~level in
          shrink m (N16 n) n_small p k kp need_to_restart
      | 2 ->
          let* n_small = alloc_n16 m ~prefix ~prefix_count ~level in
          shrink m (N48 n) n_small p k kp need_to_restart
      | 3 ->
          let* n_small = alloc_n48 m ~prefix ~prefix_count ~level in
          shrink m (N256 n) n_small p k kp need_to_restart
      | _ -> assert false

  let remove_and_unlock m n p k kp need_to_restart =
    let* ty = get_type m n in
    match ty with
    | 0 ->
        let* _ = _n4_remove m ~force:false ~flush:true n (Char.code k) in
        write_unlock m n
    | _ -> remove_and_shrink m n p k kp need_to_restart

  let add_prefix_before m n0 n1 k =
    let* p0, p0_length = get_prefix m n0 in
    let* p1, p1_length = get_prefix m n1 in
    let prefix_copy_count = min _prefix (p1_length + 1) in
    let p0' = Bytes.of_string p0 in
    Bytes.blit p0' 0 p0' prefix_copy_count
      (min p0_length (_prefix - prefix_copy_count));
    Bytes.blit_string p1 0 p0' 0 (min prefix_copy_count p1_length);
    if p1_length < _prefix then Bytes.set p0' (prefix_copy_count - 1) k;
    let p0_count' = p1_length + 1 in
    set_prefix m n0
      ~prefix:(Bytes.unsafe_to_string p0')
      ~prefix_count:p0_count' true

  type check =
    | No_match
    | Optimistic_match of { level : int }
    | Match of { level : int }

  let rec _check_prefix ~key ~prefix ~level idx max =
    if idx < max then
      if prefix.![idx] != key.![level] then return No_match
      else _check_prefix ~key ~prefix ~level:(succ level) (succ idx) max
    else return (Match { level })

  let check_prefix m addr ~key level =
    let* depth = get m A.(addr + _header_depth) Value.leint31 in
    if String.length key < depth then return No_match
    else
      let* prefix, prefix_count = get_prefix m addr in
      if prefix_count + level < depth then
        return (Optimistic_match { level = depth })
      else if prefix_count > 0 then begin
        let idx = level + prefix_count - depth
        and max = min prefix_count _prefix in
        let* res = _check_prefix ~key ~prefix ~level idx max in
        match res with
        | Match { level } when prefix_count > _prefix ->
            let level = level + (prefix_count - _prefix) in
            return (Optimistic_match { level })
        | No_match as no_match -> return no_match
        | Match _ as result -> return result
        | Optimistic_match _ -> assert false
      end
      else return (Match { level })

  type result = Restart | Return

  let size_of_node m addr =
    let* ty = get_type m addr in
    match ty with
    | 0 -> return _sizeof_n4
    | 1 -> return _sizeof_n16
    | 2 -> return _sizeof_n48
    | 3 -> return _sizeof_n256
    | _ -> assert false

  let rebalance m root node parent ~key kp level =
    let restart = ref false in
    let* count = get_count m node in
    if count == 2 && node != root then begin
      let* second_node, ks = _get_second_child m node key.![level] in
      if (second_node :> int) land 1 = 1 then begin
        let* () = write_lock_or_restart m parent restart in
        if !restart then
          let* () = write_unlock m node in
          return Restart
        else begin
          let* () = update_child m parent kp (Addr.to_rdonly second_node) in
          let* () = write_unlock m parent in
          let* _ = write_unlock_obsolete m node in
          let* uid = atomic_get m A.(node + _header_owner) Value.leintnat in
          let* len = size_of_node m node in
          let* () = collect m node ~len ~uid in
          return Return
        end
      end
      else begin
        let* v_child = get_version m second_node in
        let* _ = lock_version_or_restart m second_node v_child restart in
        if !restart then
          let* () = write_unlock m node in
          return Restart
        else begin
          let* () = write_lock_or_restart m parent restart in
          if !restart then
            let* () = write_unlock m node in
            let* () = write_unlock m second_node in
            return Restart
          else
            let* () = update_child m parent kp (Addr.to_rdonly second_node) in
            let* () = add_prefix_before m second_node node ks in
            let* () = write_unlock m parent in
            let* _ = write_unlock_obsolete m node in
            let* uid = atomic_get m A.(node + _header_owner) Value.leintnat in
            let* len = size_of_node m node in
            let* () = collect m node ~len ~uid in
            let* () = write_unlock m second_node in
            return Return
        end
      end
    end
    else
      let* () = remove_and_unlock m node parent key.![level] kp restart in
      if !restart then return Restart else return Return

  let rec remove m root key : unit t =
    let rec restart () = (remove [@tailcall]) m root key
    and _remove node next_node ~key kn level =
      let parent = node in
      let kp = kn in
      let node = next_node in
      let need_to_restart = ref false in
      let* v = get_version m node in
      let* res = check_prefix m node ~key level in
      match res with
      | No_match ->
          if is_obsolete v then
            let* const = read_unlock_or_restart m node v in
            if not const then (restart [@tailcall]) () else return ()
          else return ()
      | Optimistic_match { level } | Match { level } ->
          let kn = key.![level] in
          let* next_node = find_child m node kn in
          if Addr.is_null next_node then begin
            if is_obsolete v then
              let* const = read_unlock_or_restart m node v in
              if not const then (restart [@tailcall]) () else return ()
            else return ()
          end
          else if (next_node :> int) land 1 = 1 then begin
            let* _ = lock_version_or_restart m node v need_to_restart in
            Log.debug (fun m ->
                m "remove: start to rebalance (restart? %b)" !need_to_restart);
            if !need_to_restart then (restart [@tailcall]) ()
            else begin
              let* key' =
                let leaf = Leaf.prj (A.unsafe_to_leaf next_node) in
                get m leaf Value.ocaml_string
              in
              if not (memeq key key') then write_unlock m node
              else begin
                let* count = get_count m node in
                assert (Addr.is_null parent || count != 1);
                let* res = rebalance m root node parent ~key kp level in
                match res with
                | Restart -> (restart [@tailcall]) ()
                | Return -> return ()
              end
            end
          end
          else _remove node (Addr.unsafe_to_rdwr next_node) ~key kn (succ level)
    in
    let null = A.(of_int_to_rdwr (null :> int)) in
    _remove null root ~key '\000' 0

  let remove m root key =
    Log.debug (fun m -> m "Remove");
    Log.debug (fun m -> m "@[<hov>%a@]" (Hxd_string.pp Hxd.default) key);
    remove m root key

  let rec _check_prefix ~key ~key_len ~prefix ~level idx max =
    if idx < max then
      if prefix.[idx] <> key.[level + idx] then false
      else _check_prefix ~key ~key_len ~prefix ~level (succ idx) max
    else true

  type unoptimized_check_prefix = Not_found | Match of int

  let unoptimized_check_prefix m (addr : _ rd A.t) ~key ~key_len level =
    let* depth = get m A.(addr + _header_depth) Value.leint31 in
    if key_len < depth then return Not_found
    else
      let* prefix, prefix_count = get_prefix m addr in
      if prefix_count + level < depth then return (Match (depth - level))
      else if prefix_count > 0 then
        let idx = level + prefix_count - depth
        and max = min prefix_count _prefix in
        if not (_check_prefix ~key ~key_len ~prefix ~level idx max) then
          return Not_found
        else if prefix_count > _prefix then
          return (Match (max + (prefix_count - _prefix)))
        else return (Match (-max))
      else return (Match 0)

  let rec _exists m (node : _ rd A.t) ~key ~key_len ~optimistic_match level =
    let* res = unoptimized_check_prefix m node ~key ~key_len level in
    match res with
    | Not_found -> return false
    | Match res ->
        let optimistic_match = if res > 0 then true else optimistic_match in
        let level = level + abs res in
        if key_len < level then return false
        else
          let* node = find_child m node key.![level] in
          if A.is_null node then return false
          else if (node :> int) land 1 = 1 (* XXX(dinosaure): it is a leaf. *)
          then
            let leaf = Leaf.prj (A.unsafe_to_leaf node) in
            let* key_len' = get m leaf Value.ocaml_string_length in
            if key_len != key_len' then return false
            else if level < key_len - 1 || optimistic_match then (
              let* key' = get m leaf Value.ocaml_string in
              Log.debug (fun m -> m "memeq %S %S" key key');
              return (memeq key key'))
            else return true
          else _exists m node ~key ~key_len ~optimistic_match (succ level)

  let exists : memory -> _ rd A.t -> key:string -> key_len:int -> bool t =
   fun m (node : _ rd A.t) ~key ~key_len ->
    let node = A.unsafe_to_int node in
    let node = A.of_int_to_rdonly node in
    _exists m node ~key ~key_len ~optimistic_match:false 0

  let exists m addr key =
    let key_len = String.length key in
    exists m addr ~key ~key_len

  let make m =
    let* (N256 addr) = alloc_n256 m ~prefix:"" ~prefix_count:0 ~level:0 in
    return addr

  let insert m root key value =
    let len_w = (String.length key + size_of_word) / size_of_word in
    let len_b = len_w * size_of_word in
    let pad = Bytes.make (len_b - String.length key) '\000' in
    let rst = (len_w * size_of_word) - 1 - String.length key in
    Bytes.set pad (Bytes.length pad - 1) (Char.unsafe_chr rst);
    let pad = Bytes.unsafe_to_string pad in
    Log.debug (fun m ->
        m "Insert %S => %d (%d word(s))" key value (1 + len_w + 1));
    let hdr = leintnat_to_string ((0b101 lsl _bits_kind) lor (1 + len_w + 1)) in
    let value = leintnat_to_string value in
    let* leaf = allocate m ~kind:`Leaf [ hdr; key; pad; value ] in
    insert m root key (A.unsafe_of_leaf (Leaf.inj leaf))
end
