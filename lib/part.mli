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
