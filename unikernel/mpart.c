/* Copyright (C) 2026 Romain Calascibetta <romain.calascibetta@gmail.com>

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
*/

#include <caml/bigarray.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

#define memory_uint64_off(src, off)                                            \
  ((uint64_t *)((uint8_t *)Caml_ba_data_val(src) + Unsigned_long_val(off)))

CAMLprim value caml_set_n48_key(value memory, value addr, value k, value v) {
  uint64_t *child_index64 = memory_uint64_off(memory, addr);
  uint64_t index64 = child_index64[Unsigned_long_val(k) / 8];
  uint8_t *index8 = (uint8_t *)&index64;
  index8[Unsigned_long_val(k) % 8] = Unsigned_long_val(v);
  // the only difference with part.c here is that we don't use [movnt64]
  child_index64[Unsigned_long_val(k) / 8] = index64;

  return Val_unit;
}
