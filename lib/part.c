#include <assert.h>
#include <caml/address_class.h>
#include <caml/bigarray.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <stdatomic.h>

#define is_aligned(ptr, byte_count)                                            \
  (((uintptr_t)(const void *)(ptr)) % (byte_count) == 0)

#define memory_uint8_off(src, off)                                             \
  ((uint8_t *)((uint8_t *)Caml_ba_data_val(src) + Unsigned_long_val(off)))

#define memory_uintnat_off(src, off)                                           \
  ((uintnat *)((uint8_t *)Caml_ba_data_val(src) + Unsigned_long_val(off)))

#define memory_uint16_off(src, off)                                            \
  ((uint16_t *)((uint8_t *)Caml_ba_data_val(src) + Unsigned_long_val(off)))

#define memory_uint32_off(src, off)                                            \
  ((uint32_t *)((uint8_t *)Caml_ba_data_val(src) + Unsigned_long_val(off)))

#define memory_uint64_off(src, off)                                            \
  ((uint64_t *)((uint8_t *)Caml_ba_data_val(src) + Unsigned_long_val(off)))

CAMLprim value caml_atomic_get_uint8(value memory, value addr) {
  uint8_t v =
      __atomic_load_n(memory_uint8_off(memory, addr), memory_order_seq_cst);
  return Val_long(v);
}

CAMLprim value caml_atomic_set_uint8(value memory, value addr, value v) {
  uint8_t x = Unsigned_long_val(v);
  __atomic_store_n(memory_uint8_off(memory, addr), x, memory_order_seq_cst);
  return Val_unit;
}

/* XXX(dinosaure): About LE/BE, the usual layout is little-endian. It does not
   exist a portable and static way to see if we compile on a
   big-endian/little-endian computer. To check that, we run a little program
   (see [conf/endian.exe]) which tells to us the endianess of the computer.

   This code is usually compiled with no flags but can be compiled with
   [-DART_BIG_ENDIAN] to switch the code to a big-endian architecture.

   [ARCH_SIXTYFOUR] is given by caml to know if [intnat] is a:
   - [uint64_t] if [ARCH_SIXTYFOUR] is defined
   - [uint32_t] otherwise
 */

CAMLprim value caml_atomic_get_leuintnat(value memory, value addr) {
#if defined(__aarch64__)
  assert(is_aligned(memory_uintnat_off(memory, addr), sizeof(uintnat)));
#endif
  uintnat v =
      __atomic_load_n(memory_uintnat_off(memory, addr), memory_order_seq_cst);
#if defined(ART_BIG_ENDIAN) && defined(__ARCH_SIXTYFOUR)
  v = __bswap_64(v);
#elif defined(ART_BIG_ENDIAN)
  v = __bswap_32(v);
#endif
  return Val_long(v);
}

CAMLprim value caml_atomic_set_leuintnat(value memory, value addr, value v) {
  uintnat x = Unsigned_long_val(v);
#if defined(ART_BIG_ENDIAN) && defined(ARCH_SIXTYFOUR)
  x = __bswap_64(x);
#elif defined(ART_BIG_ENDIAN)
  x = __bswap_32(x);
#endif
#if defined(__aarch64__)
  assert(is_aligned(memory_uintnat_off(memory, addr), sizeof(uintnat)));
#endif
  __atomic_store_n(memory_uintnat_off(memory, addr), x, memory_order_seq_cst);
  return Val_unit;
}

CAMLprim value caml_atomic_get_leuint16(value memory, value addr) {
#if defined(__aarch64__)
  assert(is_aligned(memory_uint16_off(memory, addr), sizeof(uint16_t)));
#endif
  uint16_t v =
      __atomic_load_n(memory_uint16_off(memory, addr), memory_order_seq_cst);
#if defined(ART_BIG_ENDIAN)
  v = __bswap_16(v);
#endif
  return Val_long(v);
}

CAMLprim value caml_atomic_set_leuint16(value memory, value addr, value v) {
  uint16_t x = Unsigned_long_val(v);
#if defined(ART_BIG_ENDIAN)
  x = __bswap_16(x);
#endif
#if defined(__aarch64__)
  assert(is_aligned(memory_uint16_off(memory, addr), sizeof(uint16_t)));
#endif
  __atomic_store_n(memory_uint16_off(memory, addr), x, memory_order_seq_cst);
  return Val_unit;
}

CAMLprim value caml_atomic_get_leuint31(value memory, value addr) {
#if defined(__aarch64__)
  assert(is_aligned(memory_uint32_off(memory, addr), sizeof(uint32_t)));
#endif
  uint32_t v =
      __atomic_load_n(memory_uint32_off(memory, addr), memory_order_seq_cst);
#if defined(ART_BIG_ENDIAN)
  v = __bswap_32(v);
#endif
  return Val_long(v & 0x7fffffff);
}

CAMLprim value caml_atomic_set_leuint31(value memory, value addr, value v) {
  uint32_t x = Unsigned_long_val(v);
#if defined(ART_BIG_ENDIAN)
  x = __bswap_32(x);
#endif
#if defined(__aarch64__)
  assert(is_aligned(memory_uint32_off(memory, addr), sizeof(uint32_t)));
#endif
  __atomic_store_n(memory_uint32_off(memory, addr), (x & 0x7fffffff),
                   memory_order_seq_cst);
  return Val_unit;
}

uint64_t caml_atomic_get_leuint64(value memory, value addr) {
#if defined(__aarch64__)
  assert(is_aligned(memory_uint64_off(memory, addr), sizeof(uint64_t)));
#endif
  uint64_t v =
      __atomic_load_n(memory_uint64_off(memory, addr), memory_order_seq_cst);
#if defined(ART_BIG_ENDIAN)
  v = __bswap_64(v);
#endif
  return (v);
}

CAMLprim value caml_atomic_set_leuint64(value memory, value addr, uint64_t x) {
#if defined(ART_BIG_ENDIAN)
  x = __bswap_64(x);
#endif
#if defined(__aarch64__)
  assert(is_aligned(memory_uint64_off(memory, addr), sizeof(uint64_t)));
#endif
  __atomic_store_n(memory_uint64_off(memory, addr), x, memory_order_seq_cst);
  return Val_unit;
}

#if defined(ART_SSE2)
#include <emmintrin.h>
#elif defined(ART_NEON)
#include "sse2neon.h"
#else
#if defined(__arm__) || defined(__aarch64__)
#error "Unsupported target. You must support NEON."
#else
#error "Unsupported target. You must support SSE2."
#endif
#endif

#define memory_uint128_off(src, off)                                           \
  ((__m128i *)((uint8_t *)Caml_ba_data_val(src) + Unsigned_long_val(off)))

CAMLprim value caml_atomic_get_leuint128(value memory, value addr, value res) {
  /* XXX(dinosaure): non-aligned operation. */
  __m128i v =
      _mm_loadu_si128((const __m128i *)memory_uint128_off(memory, addr));

#if defined(ART_BIG_ENDIAN) && (defined(ART_SSSE3) || defined(ART_NEON))
  _mm_shuffle_epi8(
      v, _mm_set_epi32(0x0C0D0E0F, 0x08090A0B, 0x04050607, 0x00010203));
#elif defined(ART_BIG_ENDIAN)
#error                                                                         \
    "Unsupported target. For a big-endian architecture, you must support SSSE3 or NEON."
#endif
  _mm_storeu_si128((__m128i *)Bytes_val(res), v);

  return Val_unit;
}

CAMLprim value caml_atomic_fetch_add_leuint16(value memory, value addr,
                                              value v) {
  intnat res;
#if defined(__aarch64__)
  assert(is_aligned(memory_uint16_off(memory, addr), sizeof(uint16_t)));
#endif
#if defined(ART_BIG_ENDIAN)
#error "atomic_fetch_add on big-endian is not supported."
#else
  res = __atomic_fetch_add(memory_uint16_off(memory, addr),
                           Unsigned_long_val(v), memory_order_seq_cst);
#endif
  return Val_long(res);
}

CAMLprim value caml_atomic_fetch_add_leuintnat(value memory, value addr,
                                               value v) {
  intnat res;
#if defined(__aarch64__)
  assert(is_aligned(memory_uint16_off(memory, addr), sizeof(uintnat)));
#endif
#if defined(ART_BIG_ENDIAN)
#error "atomic_fetch_add on big-endian is not supported."
#elif defined(ARCH_SIXTYFOUR)
  res = __atomic_fetch_add(memory_uint64_off(memory, addr),
                           Unsigned_long_val(v), memory_order_seq_cst);
#else
  res = __atomic_fetch_add(memory_uint32_off(memory, addr),
                           Unsigned_long_val(v), memory_order_seq_cst);
#endif
  return Val_long(res);
}

CAMLprim value caml_atomic_fetch_sub_leuintnat(value memory, value addr,
                                               value v) {
  intnat res;
#if defined(ART_BIG_ENDIAN)
#error "atomic_fetch_sub on big-endian is not supported."
#elif defined(ARCH_SIXTYFOUR)
  res = __atomic_fetch_sub(memory_uint64_off(memory, addr),
                           Unsigned_long_val(v), memory_order_seq_cst);
#else
  res = __atomic_fetch_sub(memory_uint32_off(memory, addr),
                           Unsigned_long_val(v), memory_order_seq_cst);
#endif
  return Val_long(res);
}

CAMLprim value caml_atomic_fetch_sub_leuint16(value memory, value addr,
                                              value v) {
  intnat res;
#if defined(__aarch64__)
  assert(is_aligned(memory_uint16_off(memory, addr), sizeof(uint16_t)));
#endif
#if defined(ART_BIG_ENDIAN)
#error "atomic_fetch_sub on big-endian is not supported."
  res = __atomic_fetch_sub(memory_uint16_off(memory, addr),
                           Unsigned_long_val(v), memory_order_seq_cst);
#endif
  return Val_long(res);
}

CAMLprim value caml_atomic_fetch_or_leuintnat(value memory, value addr,
                                              value v) {
  intnat res;
#if defined(ART_BIG_ENDIAN)
#error "atomic_fetch_or on big-endian is not supported."
#elif defined(ARCH_SIXTYFOUR)
  res = __atomic_fetch_or(memory_uint64_off(memory, addr), Unsigned_long_val(v),
                          memory_order_seq_cst);
#else
  res = __atomic_fetch_or(memory_uint32_off(memory, addr), Unsigned_long_val(v),
                          memory_order_seq_cst);
#endif
  return Val_long(res);
}

#ifndef __unused
#define __unused(x) x __attribute__((unused))
#endif

#define __unit() value __unused(unit)

CAMLprim value caml_pause_intrinsic(__unit()) {
  _mm_pause(); /* XXX(dinosaure): platform-dependent, see plasma_spin.h for a
                  portable pause. */
  return Val_unit;
}

CAMLprim value caml_atomic_compare_exchange_strong_leuintnat(value memory,
                                                             value addr,
                                                             value expected,
                                                             value desired) {
#if defined(__aarch64__)
  assert(is_aligned(memory_uintnat_off(memory, addr), sizeof(uintnat)));
#endif
  uintnat *v = memory_uintnat_off(memory, addr);
  uintnat v_expected = Unsigned_long_val(Field(expected, 0));

  intnat res =
      __atomic_compare_exchange_n(v, &v_expected, Unsigned_long_val(desired), 0,
                                  memory_order_seq_cst, memory_order_seq_cst);
  Field(expected, 0) = Val_long(v_expected);
  return Val_bool(res);
}

CAMLprim value caml_atomic_compare_exchange_weak_leuintnat(value memory,
                                                           value addr,
                                                           value expected,
                                                           value desired) {
#if defined(__aarch64__)
  assert(is_aligned(memory_uintnat_off(memory, addr), sizeof(uintnat)));
#endif
  uintnat *v = memory_uintnat_off(memory, addr);
  uintnat v_expected = Unsigned_long_val(Field(expected, 0));

  intnat res =
      __atomic_compare_exchange_n(v, &v_expected, Unsigned_long_val(desired), 1,
                                  memory_order_seq_cst, memory_order_seq_cst);
  Field(expected, 0) = Val_long(v_expected);
  return Val_bool(res);
}

CAMLprim value caml_get_leint31(value memory, value addr) {
#if defined(__aarch64__)
  assert(is_aligned(memory_uint32_off(memory, addr), sizeof(uint32_t)));
#endif
  return Val_long(memory_uint32_off(memory, addr)[0] & 0x7fffffff);
}

CAMLprim value caml_get_leintnat(value memory, value addr) {
#if defined(ARCH_SIXTYFOUR)
  return Val_long(memory_uint64_off(memory, addr)[0]);
#else
  return Val_long(memory_uint32_off(memory, addr)[0]);
#endif
}

/* XXX(dinosaure): instr. below should appears only
 * according to [ART_{CLWB,CLFLUSHOPT,CLFLUSH,DC_CVAC}]
 * and the architecture of the host (target?) system.
 *
 * Currently, [ART_CLWB] can appear even if we are on
 * [__arch64__] architecture which is wrong. */

#ifdef ART_CLWB
void clwb(const void *ptr) { __asm__ volatile("clwb %0" : "+m"(ptr)); }

void clwb_range(const void *ptr, uint64_t len) {
  uintptr_t start = (uintptr_t)ptr & ~(64 - 1);
  for (; start < (uintptr_t)ptr + len; start += 64) {
    clwb((void *)start);
  }
}

void sfence() { __asm__ volatile("sfence" ::: "memory"); }

CAMLprim value caml_persist(value memory, value addr, value len) {
  sfence();
  clwb_range(memory_uint8_off(memory, addr), Long_val(len));
  sfence();
  return Val_unit;
}
#elif ART_DC_CVAC
void dc_cvac(const void *ptr) {
  __asm__ volatile("dc cvac, %0" ::"r"(ptr) : "memory");
}

void dc_cvac_range(const void *ptr, uint64_t len) {
  uintptr_t start = (uintptr_t)ptr & ~(64 - 1);
  // XXX(dinosaure): assume cache-line = 64 on aarch64
  for (; start < (uintptr_t)ptr + len; start += 64) {
    dc_cvac((void *)start);
  }
}

void sfence() { __asm__ volatile("dmb ishst" ::: "memory"); }

CAMLprim value caml_persist(value memory, value addr, value len) {
  sfence();
  dc_cvac_range(memory_uint8_off(memory, addr), Long_val(len));
  sfence();
  return Val_unit;
}
#elif ART_CLFLUSHOPT
void clflushopt(const void *ptr) {
  __asm__ volatile("clflushopt %0" : "+m"(ptr));
}

void clflushopt_range(const void *ptr, uint64_t len) {
  uintptr_t start = (uintptr_t)ptr & ~(64 - 1);
  for (; start < (uintptr_t)ptr + len; start += 64) {
    clflushopt((void *)start);
  }
}

void sfence() { __asm__ volatile("sfence" ::: "memory"); }

CAMLprim value caml_persist(value memory, value addr, value len) {
  sfence();
  clflushopt_range(memory_uint8_off(memory, addr), Long_val(len));
  sfence();
  return Val_unit;
}
#elif ART_CLFLUSH
void clflush(const void *ptr) { __asm__ volatile("clflush %0" : "+m"(ptr)); }

void clflush_range(const void *ptr, uint64_t len) {
  uintptr_t start = (uintptr_t)ptr & ~(64 - 1);
  for (; start < (uintptr_t)ptr + len; start += 64) {
    clflush((void *)start);
  }
}

void mfence() { __asm__ volatile("mfence" ::: "memory"); }
void sfence() { __asm__ volatile("sfence" ::: "memory"); }

CAMLprim value caml_persist(value memory, value addr, value len) {
  mfence();
  clflush_range(memory_uint8_off(memory, addr), Long_val(len));
  mfence();
  return Val_unit;
}
#else
#error "Unsupported system"
#endif

#include <caml/alloc.h>
#include <caml/memory.h>
#include <sys/mman.h>

CAMLprim value caml_get_ocaml_string(value memory, value addr) {
  CAMLparam2(memory, addr);
  CAMLlocal1(res);

  const uint8_t *v = memory_uint8_off(memory, addr);
  res = caml_copy_string(v + sizeof(uintnat));

  CAMLreturn(res);
}

CAMLprim value caml_get_ocaml_string_length(value memory, value addr) {
  const uintnat *v0 = memory_uintnat_off(memory, addr);
  uintnat ln = v0[0];

#if defined(__ARCH_SIXTYFOUR)
  ln &= 0xfffffffffffffff;
#else
  ln &= 0xfffffff;
#endif

#if defined(ART_BIG_ENDIAN) && defined(__ARCH_SIXTYFOUR)
  ln = __bswap_64(ln);
#elif defined(ART_BIG_ENDIAN)
  ln = __bswap_32(ln);
#endif

  return Val_long((ln - 2) * sizeof(uintnat) -
                  (v0[ln - 2] >> ((sizeof(uintnat) * 8) - 8)) - 1);
}

void movnt64(uint64_t *dst, uint64_t const src) {
  sfence();
  _mm_stream_si64((long long int *)dst, src);
  sfence();
}

CAMLprim value caml_set_n48_key(value memory, value addr, value k, value v) {
  const uint64_t *child_index64 = memory_uint64_off(memory, addr);
  uint64_t index64 = child_index64[Unsigned_long_val(k) / 8];
  uint8_t *index8 = (uint8_t *)&index64;
  index8[Unsigned_long_val(k) % 8] = Unsigned_long_val(v);
  movnt64((uint64_t *)&child_index64[Unsigned_long_val(k) / 8], index64);

  return Val_unit;
}

CAMLprim value caml_movnt64(value memory, value dst, value src) {
  movnt64(memory_uint64_off(memory, dst), Unsigned_long_val(src));

  return Val_unit;
}

CAMLprim value caml_msync(value ba) {
  struct caml_ba_array *b = Caml_ba_array_val(ba);

  if (b->flags & CAML_BA_MAPPED_FILE)
    assert(msync(b->data, b->dim[0], MS_SYNC | MS_INVALIDATE) == 0);

  return Val_unit;
}
