exception Duplicate
exception Too_many_retries

(** Persistent implementation of Adaptive Radix Tree.

    This module implements the core of ROWEX/P-ART from the given way to
    atomically load and store values. This implementation wants to ensure 2
    things:
    - [insert] and [lookup] can be executed in {b true} parallelism
    - persistence is ensured by required {i syscalls} (see {!val:S.persist})

    {2 A quick note about atomic operations.}

    The "normal" model for our atomic operations is always the acquire/release
    model for our variables. This corresponds to what a CPU can normally
    consider if it complies with the TSO memory model (which is the case for an
    [x86] processor). In the case of a CPU such as [ARM], it is necessary to
    make this model explicit. However, there is an optimization where certain
    writes can be relaxed. It is difficult to know the impact of such an
    optimization: and to announce our ordering property as weak.

    Thus, to simplify, we can consider that all loads have the acquire property
    and all stores have the release property. *)

type key = private string

val key : string -> key
external unsafe_key : string -> key = "%identity"

type 'a rd = < rd : unit ; .. > as 'a
type 'a wr = < wr : unit ; .. > as 'a
type ro = < rd : unit >
type wo = < wr : unit >
type rdwr = < rd : unit ; wr : unit >

module Addr : sig
  type 'a t = private int

  val null : rdwr t
  val is_null : 'a t -> bool
  external of_int_to_rdonly : int -> ro t = "%identity"
  external of_int_to_wronly : int -> wo t = "%identity"
  external of_int_to_rdwr : int -> rdwr t = "%identity"
  external to_wronly : 'a wr t -> wo t = "%identity"
  external to_rdonly : 'a rd t -> ro t = "%identity"
  external unsafe_to_int : _ t -> int = "%identity"
  val ( + ) : 'a t -> int -> 'a t
end

type ('c, 'a) value =
  | Int8 : (atomic, int) value
  | LEInt : (atomic, int) value
  | LEInt16 : (atomic, int) value
  | LEInt31 : (atomic, int) value
  | LEInt64 : (atomic, int64) value
  | LEInt128 : (atomic, string) value
  | Addr_rd : (atomic, ro Addr.t) value
  | Addr_rdwr : (atomic, rdwr Addr.t) value
  | OCaml_string : (non_atomic, string) value
  | OCaml_string_length : (non_atomic, int) value

and atomic = Atomic
and non_atomic = Non_atomic

val pp_value : Format.formatter -> ('c, 'a) value -> unit

val pp_of_value :
  ?prefer_hex:bool -> ('c, 'a) value -> Format.formatter -> 'a -> unit

module type S = sig
  type 'a t
  type memory

  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val return : 'a -> 'a t
  val atomic_get : memory -> 'a rd Addr.t -> (atomic, 'v) value -> 'v t
  val atomic_set : memory -> 'a wr Addr.t -> (atomic, 'v) value -> 'v -> unit t

  val persist : memory -> 'a wr Addr.t -> len:int -> unit t
  (** [persist addr ~len] forces the data to get written out to memory. Even if
      cache can be used to load some values, [persist] ensures that the value is
      really stored {b persistently} into the given destination such as we
      guarantee data validity despite power failures.

      More concretely, it should be (for [len <= word_size]):
      {[
        sfence clwb addr sfence
      ]}

      {b NOTE}: the first [sfence] is not systematically needed depending on
      what was done before (and if it's revelant for the current computation
      regardless the status of the cache). Such disposition is hard to track and
      we prefer to assume a correct write order than a micro-optimization. *)

  val movnt64 : memory -> dst:'a wr Addr.t -> int -> unit t
  (** [movnt64] is a special instruction that:
      - must write bypassing the write cache
      - must not load the page being written to in the cache

      The goal here is that the write must be fundamentally persistent, but we
      tolerate a cache miss if we try to read the modified page. We tolerate
      this because [movnt64] is involved in creating a new node (and therefore a
      new key insertion) that should not be read anytime soon. *)

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
  (** [pause_intrinsic] provides a hint to the processor that the code sequence
      is a spin-wait loop. If ROWEX is used in a scheduler offering the [Yield]
      directive, it is advisable to use the latter. *)

  val get : memory -> 'a rd Addr.t -> ('t, 'v) value -> 'v t

  (** Allocation and ROWEX

      ROWEX's allocation policy is quite simple: the algorithm requests blocks
      and the implementer can reuse "residual" blocks. The algorithm also
      informs when a block should no longer be reachable for any {b new}
      writers. However, these blocks can still safely be used by current
      readers. The implementer must therefore keep these blocks until the
      readers active during the {!val:collect} have completed their tasks.

      Finally, the algorithm can also request the deletion of a block it has
      just allocated. It does this via the {!val:delete} action, and it is safe
      to consider the block as available. *)

  val allocate :
    memory -> kind:[ `Leaf | `Node ] -> ?len:int -> string list -> rdwr Addr.t t

  val delete : memory -> _ Addr.t -> int -> unit t
  val collect : memory -> _ Addr.t -> len:int -> uid:int -> unit t
end

module Make (S : S) : sig
  open S

  val lookup : memory -> 'a rd Addr.t -> key -> int64 t
  val iter : memory -> fn:(key -> int64 -> unit) -> 'a rd Addr.t -> unit t

  val insert : ?or_update:bool -> memory -> rdwr Addr.t -> key -> int64 -> unit t
  (** [insert m root key value] adds [key => value] to the tree.

      @raise Duplicate if [key] is already bound and [or_update] is [false]
      (the default). When [or_update] is [true], an existing binding is
      {b atomically} replaced instead. *)

  val exists : memory -> 'a rd Addr.t -> key -> bool t
  val remove : memory -> rdwr Addr.t -> key -> unit t
  val make : memory -> rdwr Addr.t t
end

(** / *)

val _header_kind : int
val _bits_kind : int
val _header_prefix : int
val _prefix : int
val _header_compact_count : int
val _header_length : int
val _header_count : int
val _header_depth : int
val _header_owner : int
val _n4_align_length : int
val _sizeof_n4 : int
val _sizeof_n16 : int
val _sizeof_n48 : int
val _sizeof_n256 : int
