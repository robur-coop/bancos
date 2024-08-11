type command =
  | Insert of Rowex.key * int
  | Remove of Rowex.key
  | Lookup of Rowex.key
  | Noop

let clean commands =
  let active, awaits = List.partition Db.is_running commands in
  Fmt.epr "%d active(s) tasks are pending\n%!" (List.length active);
  let rec go : Db.command list -> unit = function
    | [] -> ()
    | cmd :: rest ->
        begin
          match Db.await cmd with
          | `Not_found key ->
              Logs.err (fun m -> m "%S not found" (key :> string))
          | `Found (key, value) ->
              Logs.info (fun m -> m "%S => %x" (key :> string) value)
          | `Duplicate key ->
              Logs.err (fun m -> m "%S already exists" (key :> string))
          | `Exists key -> Logs.info (fun m -> m "%S exists" (key :> string))
          | `Ok -> ()
        end;
        go rest
  in
  go awaits;
  active

let execute ?quiet:_ commands filepath =
  Miou.run ~domains:6 @@ fun () ->
  let t = Db.openfile filepath in
  Logs.debug (fun m -> m "ROWEX file loaded");
  let rec go active_commands =
    let active_commands = clean active_commands in
    match commands () with
    | None -> active_commands
    | Some Noop -> go active_commands
    | Some (Lookup key) ->
        let cmd = Db.lookup t key in
        go (cmd :: active_commands)
    | Some (Insert (key, value)) ->
        let cmd = Db.insert t key value in
        go (cmd :: active_commands)
    | Some (Remove key) ->
        let cmd = Db.remove t key in
        go (cmd :: active_commands)
  in
  let active_commands = go [] in
  Logs.debug (fun m -> m "Commands sended, start to clean-up results");
  let rec go = function
    | [] -> ()
    | active_commands -> go (clean active_commands)
  in
  go active_commands;
  Logs.debug (fun m -> m "Results consumed, start to close the db file");
  Fmt.epr ">>> kill the db\n%!";
  Db.close t

let parse line =
  match String.split_on_char ' ' line with
  | "insert" :: key :: value :: _ -> (
      try
        let key = Rowex.key key in
        let value = int_of_string value in
        Ok (Insert (key, value))
      with _ -> Error `Invalid_insert_command)
  | "remove" :: key :: _ -> (
      try Ok (Remove (Rowex.key key)) with _ -> Error `Invalid_remove_command)
  | ("find" | "lookup") :: key :: _ -> (
      try Ok (Lookup (Rowex.key key)) with _ -> Error `Invalid_find_command)
  | "#" :: _ -> Ok Noop
  | _ -> Error `Invalid_command

let rec commands_from_in_channel ?(close = ignore) ic =
  match input_line ic with
  | exception End_of_file ->
      close ();
      None
  | line -> (
      match parse line with
      | Ok command -> Some command
      | Error _ ->
          Logs.err (fun m -> m "Invalid command: %S" line);
          commands_from_in_channel ic)

let setup_commands input =
  match input with
  | None -> fun () -> commands_from_in_channel stdin
  | Some commands ->
      let ic = open_in commands in
      let close () = close_in ic in
      fun () -> commands_from_in_channel ~close ic

let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt

let run quiet commands filepath =
  execute ~quiet commands (Fpath.to_string filepath);
  `Ok ()

open Cmdliner
open Args

let index =
  let doc = "The ROWEX file." in
  let parser = Fpath.of_string in
  let pp = Fpath.pp in
  let filepath = Arg.conv (parser, pp) in
  Arg.(required & opt (some filepath) None & info [ "i"; "index" ] ~doc)

let commands =
  let doc =
    "A file which contains different commands to execute into the index file."
  in
  let parser str =
    match Fpath.of_string str with
    | Ok _ when Sys.file_exists str -> Ok str
    | Ok v -> error_msgf "%a does not exists" Fpath.pp v
    | Error _ as err -> err
  in
  Arg.(
    value
    & opt (some (conv (parser, Fmt.string))) None
    & info [ "c"; "commands" ] ~doc)

let term_setup_commands = Term.(const setup_commands $ commands)

let term =
  Term.(ret (const run $ term_setup_logs $ term_setup_commands $ index))

let cmd =
  let doc = "A simple tool to manipulate an KV-store (serialized)." in
  let man = [] in
  Cmd.v (Cmd.info "db" ~doc ~man) term

let () = exit (Cmd.eval cmd)
