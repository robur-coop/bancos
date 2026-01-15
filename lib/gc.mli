type 'mem t
type uid = private int

val make :
  extend_and_copy:(uid -> 'mem -> 'mem * int) -> 'mem Atomic.t -> 'mem t

val memory : 'mem t -> 'mem
val atomic_memory : 'mem t -> 'mem
val with_memory : 'mem t -> 'mem -> 'mem t
val null : uid

module type S = sig
  type memory

  val length : memory -> int
  val atomic_fetch_add_leuintnat : memory -> int -> int -> int
  val atomic_set_leuintnat : memory -> int -> int -> unit
  val set_int32 : memory -> int -> int32 -> unit
  val set_uint8 : memory -> int -> int -> unit
end

module Make (C : S) : sig
  type memory = C.memory

  val gen : unit -> uid

  val alloc :
       memory t
    -> writer:uid
    -> kind:[ `Node | `Leaf ]
    -> int
    -> string list
    -> Rowex.rdwr Rowex.Addr.t

  val collect : 'mem t -> uid -> 'cap Rowex.Addr.t -> len:int -> uid:int -> unit
  val delete : 'mem t -> 'cap Rowex.Addr.t -> int -> unit
  val unsafe_delete : 'mem t -> 'cap Rowex.Addr.t -> int -> unit
  val add_process : 'mem t -> [ `Wr | `Rd ] -> uid
  val release_process : 'mem t -> [ `Wr | `Rd ] -> uid:uid -> unit
end
