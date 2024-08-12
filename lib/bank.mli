type t

val lookup : t -> Rowex.key -> int
val insert : t -> Rowex.key -> int -> unit
val exists : t -> Rowex.key -> bool
val remove : t -> Rowex.key -> unit
val make : unit -> t
