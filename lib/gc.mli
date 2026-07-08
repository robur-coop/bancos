(* Copyright (C) 2026 Romain Calascibetta <romain.calascibetta@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.
*)

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

  val blit_from_string :
    string -> src_off:int -> memory -> dst_off:int -> len:int -> unit
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
