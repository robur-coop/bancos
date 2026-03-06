type t
type command

type result =
  [ `Ok
  | `Not_found of Rowex.key
  | `Found of Rowex.key * int64
  | `Duplicate of Rowex.key
  | `Too_many_retries of Rowex.key
  | `Exists of Rowex.key ]

val insert : t -> Rowex.key -> int64 -> command
val remove : t -> Rowex.key -> command
val lookup : t -> Rowex.key -> command
val iter : fn:(Rowex.key -> int64 -> unit) -> t -> command
val exists : t -> Rowex.key -> command
val await : command -> result
val is_running : command -> bool

val openfile :
     ?readers:int
  -> ?writers:int
  -> ?size:int
  -> ?init:unit Lazy.t Stdlib.Domain.DLS.key
  -> string
  -> t

val close : t -> unit
