type command =
  | Insert of Rowex.key * int
  | Lookup of Rowex.key
  | Remove of Rowex.key

let pp_command ppf = function
  | Insert (k, v) -> Fmt.pf ppf "<insert:%S:%x>" (k :> string) v
  | Lookup k -> Fmt.pf ppf "<lookup:%S>" (k :> string)
  | Remove k -> Fmt.pf ppf "<remove:%S>" (k :> string)

open Crowbar

let gen_key =
  map [ bytes ] @@ function
  | "" -> bad_test ()
  | key -> ( try Rowex.key key with _ -> bad_test ())

let gen_insert = map [ gen_key; int ] @@ fun k v -> Insert (k, v)
let gen_insert = with_printer pp_command gen_insert
let gen_lookup = map [ gen_key ] @@ fun k -> Lookup k
let gen_lookup = with_printer pp_command gen_lookup
let gen_remove = map [ gen_key ] @@ fun k -> Remove k
let gen_remove = with_printer pp_command gen_remove
let gen_command = choose [ gen_insert; gen_lookup; gen_remove ]

module Oracle = struct
  type t = (Rowex.key, int) Hashtbl.t

  let lookup t key =
    try `Found (Hashtbl.find t key) with Not_found -> `Not_found

  let insert t key value =
    if Hashtbl.mem t key then `Duplicate
    else begin
      Hashtbl.add t key value;
      `Ok
    end

  let remove t key =
    Hashtbl.remove t key;
    `Ok

  let do_command t = function
    | Insert (k, v) -> insert t k v
    | Lookup k -> lookup t k
    | Remove k -> remove t k
end

module Rowex = struct
  include Mem

  let lookup t key = try `Found (lookup t key) with Not_found -> `Not_found

  let insert t key value =
    try
      insert t key value;
      `Ok
    with Rowex.Duplicate -> `Duplicate

  let remove t key =
    remove t key;
    `Ok

  let do_command t = function
    | Insert (k, v) -> insert t k v
    | Lookup k -> lookup t k
    | Remove k -> remove t k
end

let pp_result ppf = function
  | `Ok -> Fmt.string ppf "<ok>"
  | `Found v -> Fmt.pf ppf "<found:%x>" v
  | `Not_found -> Fmt.string ppf "<not-found>"
  | `Duplicate -> Fmt.string ppf "<duplicate>"

let () =
  add_test ~name:"rowex" [ list1 gen_command ] @@ fun cmds ->
  let t0 = Hashtbl.create 0x10 in
  let t1 = Rowex.make () in
  let r0 = List.map (Oracle.do_command t0) cmds in
  let r1 = List.map (Rowex.do_command t1) cmds in
  check_eq ~pp:Fmt.(Dump.list pp_result) r0 r1;
  Hashtbl.iter
    begin
      fun k v ->
        match Rowex.lookup t1 k with
        | `Found v' -> check_eq v v'
        | `Not_found -> Crowbar.failf "%S does not exist" (k :> string)
    end
    t0
