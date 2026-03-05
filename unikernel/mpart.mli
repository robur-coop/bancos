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
