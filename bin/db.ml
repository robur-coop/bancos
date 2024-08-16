type command =
  | Insert of Rowex.key * int
  | Remove of Rowex.key
  | Lookup of Rowex.key
  | Noop

let iter ?(quiet = false) to_delete node =
  let cmd = Miou.Sequence.data node in
  if Bancos.is_running cmd then ()
  else begin
    to_delete := node :: !to_delete;
    match Bancos.await cmd with
    | `Not_found key -> Logs.err (fun m -> m "%S not found" (key :> string))
    | `Found (key, value) ->
        if not quiet then Fmt.pr "%S => %d\n%!" (key :> string) value;
        Logs.info (fun m -> m "%S => %x" (key :> string) value)
    | `Duplicate key ->
        Logs.err (fun m -> m "%S already exists" (key :> string))
    | `Exists key -> Logs.info (fun m -> m "%S exists" (key :> string))
    | `Ok -> ()
  end

let clean ?(quiet = false) commands =
  let to_delete = ref [] in
  Miou.Sequence.iter_node ~f:(iter ~quiet to_delete) commands;
  List.iter Miou.Sequence.remove !to_delete

let execute ?(quiet = false) ?(and_remove = false) commands ~readers ~writers
    filepath =
  Miou.run ~domains:(readers + writers) @@ fun () ->
  let t = Bancos.openfile ~readers ~writers filepath in
  Logs.debug (fun m -> m "ROWEX file loaded");
  let seq = Miou.Sequence.create () in
  let rec go () =
    clean ~quiet seq;
    match commands () with
    | None -> ()
    | Some Noop -> go ()
    | Some (Lookup key) ->
        let cmd = Bancos.lookup t key in
        ignore Miou.Sequence.(add Left seq cmd);
        go ()
    | Some (Insert (key, value)) ->
        let cmd = Bancos.insert t key value in
        ignore Miou.Sequence.(add Left seq cmd);
        go ()
    | Some (Remove key) ->
        let cmd = Bancos.remove t key in
        ignore Miou.Sequence.(add Left seq cmd);
        go ()
  in
  go ();
  Logs.debug (fun m -> m "Commands sended, start to clean-up results");
  while Miou.Sequence.is_empty seq = false do
    clean ~quiet seq
  done;
  Logs.debug (fun m -> m "Results consumed, start to close the db file");
  Bancos.close t;
  if and_remove then Unix.unlink filepath

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

let run quiet commands filepath readers writers and_remove =
  execute ~quiet ~and_remove commands ~readers ~writers
    (Fpath.to_string filepath);
  `Ok ()

open Cmdliner
open Args

let writers =
  let doc = "The number of writers." in
  Arg.(value & opt int 2 & info [ "w"; "writers" ] ~doc)

let readers =
  let doc = "The number of readers." in
  Arg.(value & opt int 4 & info [ "r"; "readers" ] ~doc)

let index =
  let doc = "The ROWEX file." in
  let parser = Fpath.of_string in
  let pp = Fpath.pp in
  let filepath = Arg.conv (parser, pp) in
  Arg.(required & opt (some filepath) None & info [ "i"; "index" ] ~doc)

let and_remove =
  let doc = "Remove the idx file produced." in
  Arg.(value & flag & info [ "and-remove" ] ~doc)

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
  Term.(
    ret
      (const run $ term_setup_logs $ term_setup_commands $ index $ readers
     $ writers $ and_remove))

let cmd =
  let doc = "A simple tool to manipulate an KV-store (parallel)." in
  let man = [] in
  Cmd.v (Cmd.info "db" ~doc ~man) term

let () = exit (Cmd.eval cmd)
