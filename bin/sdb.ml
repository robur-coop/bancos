(* Copyright (C) 2026 Romain Calascibetta <romain.calascibetta@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.
*)

type command =
  | Insert of Rowex.key * int64
  | Remove of Rowex.key
  | Lookup of Rowex.key
  | Noop

let execute ?(quiet = false) commands filepath size =
  Miou.run ~domains:0 @@ fun () ->
  let t = Part.from_system ~size filepath in
  let rec go n =
    match commands () with
    | None -> if not quiet then Fmt.pr "db: %d action(s) committed\n%!" n
    | Some Noop -> go n
    | Some (Lookup key) ->
        let () =
          Part.reader t @@ fun ~uid:_ reader ->
          match Part.lookup reader key with
          | value ->
              if not quiet then Fmt.pr "%S => %Ld\n%!" (key :> string) value;
              Logs.info (fun m -> m "%S => %Ld" (key :> string) value)
          | exception Not_found ->
              Logs.err (fun m -> m "%S does not exist" (key :> string));
              raise Not_found
        in
        go (succ n)
    | Some (Insert (key, value)) ->
        let[@warning "-8"] (Ok ()) =
          Part.writer t @@ fun ~uid:_ writer ->
          begin try Part.insert writer key value
          with Rowex.Duplicate ->
            Part.remove writer key;
            Part.insert writer key value
          end
        in
        go (succ n)
    | Some (Remove key) ->
        let[@warning "-8"] (Ok ()) =
          Part.writer t @@ fun ~uid:_ writer -> Part.remove writer key
        in
        go (succ n)
  in
  go 0

let parse line =
  match String.split_on_char ' ' line with
  | "insert" :: key :: value :: _ -> (
      try
        let key = Rowex.key key in
        let value = Int64.of_string value in
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

let run (quiet, key) commands filepath size =
  let reporter = Stdlib.Domain.DLS.get key in
  let () = Lazy.force reporter in
  execute ~quiet commands (Fpath.to_string filepath) size;
  `Ok ()

open Cmdliner
open Bancos_cli

let index =
  let doc = "The ROWEX file" in
  let parser = Fpath.of_string in
  let pp = Fpath.pp in
  let v = Arg.conv (parser, pp) in
  Arg.(required & opt (some v) None & info [ "i"; "index" ] ~doc)

let size =
  let doc = "The size of the ROWEX file" in
  let open Arg in
  value & opt size 10485760 & info [ "s"; "size" ] ~doc ~docv:"SIZE"

let commands =
  let doc =
    "Specify a file which contains different commands to execute into the \
     given index file"
  in
  let parser str =
    match Fpath.of_string str with
    | Ok _ when Sys.file_exists str -> Ok str
    | Ok v -> error_msgf "%a does not exists" Fpath.pp v
    | Error _ as err -> err
  in
  let open Arg in
  value
  & opt (some (conv (parser, Fmt.string))) None
  & info [ "c"; "commands" ] ~doc

let term_setup_commands = Term.(const setup_commands $ commands)

let term =
  let open Term in
  const run $ term_setup_logs $ term_setup_commands $ index $ size |> ret

let cmd =
  let doc = "A simple tool to manipulate an KV-store (serialized)" in
  let man = [] in
  Cmd.v (Cmd.info "db" ~doc ~man) term

let () = exit (Cmd.eval cmd)
