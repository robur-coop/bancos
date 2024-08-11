open Cmdliner

let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt

let verbosity =
  let env = Cmd.Env.info "DB_LOGS" in
  Logs_cli.level ~env ()

let renderer =
  let env = Cmd.Env.info "DB_FMT" in
  Fmt_cli.style_renderer ~env ()

let utf_8 =
  let doc = "Allow us to emit UTF-8 characters." in
  let env = Cmd.Env.info "DB_UTF_8" in
  Arg.(value & opt bool true & info [ "with-utf-8" ] ~doc ~env)

let app_style = `Cyan
let err_style = `Red
let warn_style = `Yellow
let info_style = `Blue
let debug_style = `Green

let pp_header ~pp_h ppf (l, h) =
  match l with
  | Logs.Error ->
      pp_h ppf err_style (match h with None -> "ERROR" | Some h -> h)
  | Logs.Warning ->
      pp_h ppf warn_style (match h with None -> "WARN" | Some h -> h)
  | Logs.Info ->
      pp_h ppf info_style (match h with None -> "INFO" | Some h -> h)
  | Logs.Debug ->
      pp_h ppf debug_style (match h with None -> "DEBUG" | Some h -> h)
  | Logs.App -> (
      match h with
      | Some h -> Fmt.pf ppf "[%a] " Fmt.(styled app_style (fmt "%10s")) h
      | None -> ())

let pp_header =
  let pp_h ppf style h = Fmt.pf ppf "[%a]" Fmt.(styled style (fmt "%10s")) h in
  pp_header ~pp_h

let anchor = Unix.gettimeofday ()
let now () = Unix.gettimeofday () -. anchor
let () = Logs_threaded.enable ()

let reporter ppf =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let with_metadata header _tags k ppf fmt =
      Fmt.kpf k ppf
        ("[+%a][%3d]%a[%a]: @[<hov>" ^^ fmt ^^ "@]\n%!")
        Fmt.(styled `Cyan (fmt "%.06f"))
        (now ())
        (Stdlib.Domain.self () :> int)
        pp_header (level, header)
        Fmt.(styled `Magenta (fmt "%20s"))
        (Logs.Src.name src)
    in
    msgf @@ fun ?header ?tags fmt -> with_metadata header tags k ppf fmt
  in
  { Logs.report }

let setup_logs utf_8 style_renderer level =
  Fmt_tty.setup_std_outputs ~utf_8 ?style_renderer ();
  Logs.set_level level;
  let reporter = reporter Fmt.stderr in
  Logs.set_reporter reporter;
  Option.is_none level

let term_setup_logs = Term.(const setup_logs $ utf_8 $ renderer $ verbosity)
