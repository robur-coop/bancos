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
