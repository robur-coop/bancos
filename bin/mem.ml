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

open Rowex

external get_int128_le : Bstr.t -> int -> bytes -> unit
  = "caml_atomic_get_leuint128"
[@@noalloc]

external get_ocaml_string : Bstr.t -> int -> string = "caml_get_ocaml_string"

let get_int_le bstr (addr : _ Addr.t) =
  if Sys.word_size = 64 then
    Bstr.get_int64_le bstr (addr :> int) |> Int64.to_int
  else Bstr.get_int32_le bstr (addr :> int) |> Int32.to_int

let get : type v. Bstr.t -> 'a rd Addr.t -> (atomic, v) value -> v =
 fun bstr addr -> function
  | Rowex.Int8 -> Bstr.get_uint8 bstr (addr :> int)
  | LEInt -> get_int_le bstr addr
  | LEInt16 -> Bstr.get_uint16_le bstr (addr :> int)
  | LEInt31 -> Bstr.get_int32_le bstr (addr :> int) |> Int32.to_int
  | LEInt128 ->
      let tmp = Bytes.create 16 in
      get_leuint128 bstr (addr :> int) tmp;
      Bytes.unsafe_to_string tmp
  | Addr_rd ->
      let addr = get_int_le bstr addr in
      Addr.of_int_to_rdonly addr
  | Addr_rdwr ->
      let addr = get_int_le bstr addr in
      Addr.of_int_to_rdwr addr
  | OCaml_string -> get_ocaml_string bstr (addr :> int)
  | OCaml_string_length -> get_int_le bstr addr

module Node = struct
  let get_type bstr addr =
    get bstr Addr.(addr + _header_kind) Rowex.LEInt lsr _bits_kind

  let get_version bstr addr = get bstr Addr.(addr + _header_kind) Rowex.LEInt
  let get_depth bstr addr = get bstr Addr.(addr + _header_depth)
  let get_count bstr addr = get bstr Addr.(addr + _header_count)

  type prefix = { prefix : string; prefix_count : int; value : int64 }

  let get_prefix bstr addr =
    let value = get bstr Addr.(addr + _header_prefix) Rowex.LEInt64 in
    let p0 = Int64.(to_int (logand value 0xffffL)) in
    let p1 = Int64.(to_int (logand (shift_right value 16) 0xffffL)) in
    let tmp = Bytes.create _prefix in
    Bytes.set_uint16_ne prefix 0 p0;
    Bytes.set_uint16_ne prefix 2 p1;
    let prefix = Bytes.unsafe_to_string tmp in
    let prefix_count = Int64.(to_int (shift_right value 32)) in
    { value; prefix; prefix_count }

  let get_compact_count bstr addr =
    get bstr Addr.(addr + _header_compact_count) Rowex.LEInt16

  type t = {
      kind : [ `N4 | `N16 | `N48 | `N256 ]
    ; version : int
    ; prefix : prefix
    ; depth : int
    ; count : int
    ; compact_count : int
  }

  let get bstr addr =
    let kind =
      match get_type bstr addr with
      | 0 -> `N4
      | 1 -> `N16
      | 2 -> `N48
      | 3 -> `N256
      | _ -> Fmt.failwith "Invalid ROWEX node at %016x" (addr :> int)
    in
    let prefix = get_prefix bstr addr in
    let version = get_version bstr addr in
    let depth = get_depth bstr addr in
    let count = get_count bstr addr in
    let compact_count = get_compact_count bstr addr in
    { kind; prefix; version; depth; count; compact_count }
end
