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

let reporter sources ppfs =
  let re = Option.map Re.compile sources in
  let print src =
    let some re =
      (Fun.negate List.is_empty) (Re.matches re (Logs.Src.name src))
    in
    Option.fold ~none:true ~some re
  in
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
    match (level, print src) with
    | Logs.Debug, false -> k ()
    | _, true | _ ->
        msgf @@ fun ?header ?tags fmt ->
        with_metadata header tags k ppfs.((Stdlib.Domain.self () :> int)) fmt
  in
  { Logs.report }

let regexp : (string * [ `None | `Re of Re.t ]) Arg.conv =
  let parser str =
    match Re.Pcre.re str with
    | re -> Ok (str, `Re re)
    | exception _ -> error_msgf "Invalid PCRegexp: %S" str
  in
  let pp ppf (str, _) = Fmt.string ppf str in
  Arg.conv (parser, pp)

let sources =
  let doc = "A regexp (PCRE syntax) to identify which log we print." in
  let open Arg in
  value & opt_all regexp [ ("", `None) ] & info [ "l" ] ~doc ~docv:"REGEXP"

let setup_sources = function
  | [ (_, `None) ] -> None
  | res ->
      let res = List.map snd res in
      let res =
        List.fold_left
          (fun acc -> function `Re re -> re :: acc | _ -> acc)
          [] res
      in
      Some (Re.alt res)

let setup_sources = Term.(const setup_sources $ sources)

let logs_per_domains =
  let doc = "Produce a log file per domains (to avoid the global lock)." in
  Arg.(value & flag & info [ "logs-per-domains" ] ~doc)

let setup_logs utf_8 style_renderer sources level logs_per_domains =
  Fmt_tty.setup_std_outputs ~utf_8 ?style_renderer ();
  Logs.set_level level;
  let domains = Stdlib.Domain.recommended_domain_count () in
  let fn () =
    match logs_per_domains with
    | false ->
        Lazy.from_fun @@ fun () ->
        Logs_threaded.enable ();
        let ppfs = Array.init domains (Fun.const Fmt.stderr) in
        Logs.set_reporter (reporter sources ppfs)
    | true ->
        Lazy.from_fun @@ fun () ->
        let fn _ =
          let filepath = Filename.temp_file "log-" ".log" in
          let oc = open_out_bin filepath in
          Format.formatter_of_out_channel oc
        in
        let ppfs = Array.init domains fn in
        Logs.set_reporter (reporter sources ppfs)
  in
  let key = Stdlib.Domain.DLS.new_key fn in
  (Option.is_none level, key)

let term_setup_logs =
  Term.(
    const setup_logs $ utf_8 $ renderer $ setup_sources $ verbosity
    $ logs_per_domains)

let bytes_of_string s =
  let s = String.trim s in
  let len = String.length s in
  let rec find_non_digit i =
    if i >= len then i
    else if s.[i] >= '0' && s.[i] <= '9' then find_non_digit (i + 1)
    else i
  in
  let idx = find_non_digit 0 in
  let number_str = String.sub s 0 idx |> String.trim in
  let unit_str = String.sub s idx (len - idx) |> String.trim in
  let ( let* ) = Option.bind in
  let* number = int_of_string_opt number_str in
  let* multiplier =
    match String.lowercase_ascii unit_str with
    | "" | "b" -> Some 1
    | "kib" -> Some 1024
    | "mib" -> Some (1024 * 1024)
    | "gib" -> Some (1024 * 1024 * 1024)
    | "tib" -> Some (1024 * 1024 * 1024 * 1024)
    | _ -> None
  in
  Some (number * multiplier)

let sizes = [| "B"; "KiB"; "MiB"; "GiB"; "TiB" |]

let bytes_to_size = function
  | 0 -> "0b"
  | n ->
      let n = float_of_int n in
      let i = Float.floor (Float.log n /. Float.log 1024.) in
      let r = n /. Float.pow 1024. i in
      Fmt.str "%.0f%s" r sizes.(int_of_float i)

let size =
  let parser str =
    match bytes_of_string str with
    | Some n -> Ok n
    | None -> error_msgf "Invalid size: %S" str
  in
  Arg.conv (parser, Fmt.(using bytes_to_size string))
