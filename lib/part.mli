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

type memory =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

type t
type reader
type writer
type uid = private int

val lookup : reader -> Rowex.key -> int64
val iter : fn:(Rowex.key -> int64 -> unit) -> reader -> unit
val exists : reader -> Rowex.key -> bool
val remove : writer -> Rowex.key -> unit

val insert : writer -> Rowex.key -> int64 -> unit
(** @raise Rowex.Duplicate if the key already exists. *)

val update : writer -> Rowex.key -> int64 -> unit
(** Like {!insert} but atomically replaces an existing binding instead of
    raising {!Rowex.Duplicate}. *)

val from_system : ?size:int -> string -> t
val reader : t -> (uid:uid -> reader -> 'a) -> 'a
val writer : t -> (uid:uid -> writer -> 'a) -> ('a, exn) result
(* {b NOTE}: The writer specified in the function is not {b shareable} and
   cannot be used across multiple domains. It must be assigned to a specific
   domain and remain there. *)
