type t

val lookup : t -> Rowex.key -> int64
val insert : t -> Rowex.key -> int64 -> unit
val exists : t -> Rowex.key -> bool
val remove : t -> Rowex.key -> unit
val make : unit -> t
