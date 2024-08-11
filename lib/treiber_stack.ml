type 'a node = Nil | Cons of { value : : 'a; tail : 'a node }
type 'a t = 'a node Atomic.t

let make () = Atomic.make Nil
let is_empty t = Atomic.get t = Nil

let push t value =
  let go backoff =
    let tail = Atomic.get t in
    let cons = Cons { value; tail } in
    if not (Atomic.compare_and_set t tail cons)
    then go (Backoff.once backoff) in
  go Backoff.default

exception Empty

let pop t =
  let rec go backoff =
    match Atomic.get t with
    | Nil -> raise Empty
    | Cons { value; tail } as cons ->
      if Atomic.compare_and_set t cons tail
      then value else go (Backoff.once backoff) in
  go Backoff.default

let peek t =
  match Atomic.get t with
  | Nil -> raise Empty
  | Cons { value; _ } -> value
