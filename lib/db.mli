type t
type command

type result =
  [ `Ok
  | `Not_found of Rowex.key
  | `Found of Rowex.key * int
  | `Duplicate of Rowex.key
  | `Exists of Rowex.key ]

val insert : t -> Rowex.key -> int -> command
val remove : t -> Rowex.key -> command
val lookup : t -> Rowex.key -> command
val exists : t -> Rowex.key -> command
val await : command -> result
val is_running : command -> bool
val openfile : ?readers:int -> ?writers:int -> string -> t
val close : t -> unit
