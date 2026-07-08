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

type reader
type writer
type t

val tag : int Logs.Tag.def
val lookup : reader -> Rowex.key -> int64
val exists : reader -> Rowex.key -> bool
val remove : writer -> Rowex.key -> unit
val insert : writer -> Rowex.key -> int64 -> unit
val make : ?tags:Logs.Tag.set -> Mkernel.Block.t -> t
val reader : t -> (reader -> 'a) -> 'a
val writer : t -> (writer -> 'a) -> 'a
