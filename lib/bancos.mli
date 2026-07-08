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

type t
type command

type result =
  [ `Ok
  | `Not_found of Rowex.key
  | `Found of Rowex.key * int64
  | `Too_many_retries of Rowex.key
  | `Exists of Rowex.key ]

val insert : t -> Rowex.key -> int64 -> command
val remove : t -> Rowex.key -> command
val lookup : t -> Rowex.key -> command
val iter : fn:(Rowex.key -> int64 -> unit) -> t -> command
val exists : t -> Rowex.key -> command
val await : command -> result
val is_running : command -> bool

val openfile :
     ?readers:int
  -> ?writers:int
  -> ?size:int
  -> ?init:unit Lazy.t Stdlib.Domain.DLS.key
  -> string
  -> t

val close : t -> unit
