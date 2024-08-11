type command =
  | Insert of Rowex.key * int
  | Remove of Rowex.key
  | Lookup of Rowex.key
  | Noop

let execute ?(quiet = false) commands filepath =
  Miou.run ~domains:0 @@ fun () ->
  let t = Part.from_system ~filepath in
  let reader = Part.reader t in
  let rec go n =
    match commands () with
    | None -> if not quiet then Fmt.pr "db: %d action(s) committed\n%!" n
    | Some Noop -> go n
    | Some (Lookup key) -> begin
        match Part.lookup reader key with
        | value ->
            if not quiet then Fmt.pr "%S => %d\n%!" (key :> string) value;
            Logs.info (fun m -> m "%S => %d" (key :> string) value);
            go (succ n)
        | exception Not_found ->
            Logs.err (fun m -> m "%S does not exist" (key :> string));
            raise Not_found
      end
    | Some (Insert (key, value)) ->
        let[@warning "-8"] (Ok ()) =
          Part.writer t @@ fun writer ->
          begin
            try Part.insert writer key value
            with Rowex.Duplicate ->
              Part.remove writer key;
              Part.insert writer key value
          end
        in
        go (succ n)
    | Some (Remove key) ->
        let[@warning "-8"] (Ok ()) =
          Part.writer t @@ fun writer -> Part.remove writer key
        in
        go (succ n)
  in
  go 0

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
