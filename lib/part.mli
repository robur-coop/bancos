type memory =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

type t
type reader
type writer

val lookup : reader -> Rowex.key -> int
val exists : reader -> Rowex.key -> bool
val remove : writer -> Rowex.key -> unit
val insert : writer -> Rowex.key -> int -> unit
val from_system : filepath:string -> t
val reader : t -> reader
val writer : t -> (writer -> 'a) -> ('a, exn) result
