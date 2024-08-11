external get_neuint32 : Part.memory -> int -> int32 = "%caml_bigstring_get32"
external get_neuint64 : Part.memory -> int -> int64 = "%caml_bigstring_get64"
external get_neuint16 : Part.memory -> int -> int = "%caml_bigstring_get16"
external get_char : Part.memory -> int -> char = "%caml_ba_ref_1"
external get_int8 : Part.memory -> int -> int = "%caml_ba_ref_1"
external swap16 : int -> int = "%bswap16"
external swap32 : int32 -> int32 = "%bswap_int32"
external swap64 : int64 -> int64 = "%bswap_int64"
external bytes_set16 : bytes -> int -> int -> unit = "%caml_bytes_set16u"

external get_ocaml_string : Part.memory -> int -> string
  = "caml_get_ocaml_string"

external get_leuint128 : Part.memory -> int -> bytes -> unit
  = "caml_atomic_get_leuint128"
[@@noalloc]

let size_of_word = Sys.word_size / 8

let get_leuint128 memory off =
  let res = Bytes.create 16 in
  get_leuint128 memory off res;
  Bytes.unsafe_to_string res

let get_leuint16 =
  if Sys.big_endian then fun m o -> swap16 (get_neuint16 m o) else get_neuint16

let get_leuint31 =
  if Sys.big_endian then fun m o -> Int32.to_int (swap32 (get_neuint32 m o))
  else fun m o -> Int32.to_int (get_neuint32 m o)

let get_leuint32 =
  if Sys.big_endian then fun m o -> swap32 (get_neuint32 m o) else get_neuint32

let get_leuint64 =
  if Sys.big_endian then fun m o -> swap64 (get_neuint64 m o) else get_neuint64

let get_leuintnat =
  match Sys.word_size with
  | 32 -> fun m o -> Int32.to_int (get_leuint32 m o)
  | 64 -> fun m o -> Int64.to_int (get_leuint64 m o)
  | _ -> assert false

let get_type memory addr =
  let value = get_leuintnat memory (addr + Rowex._header_kind) in
  value lsr Rowex._bits_kind

let get_version memory addr = get_leuintnat memory (addr + Rowex._header_kind)
let get_depth memory addr = get_leuint31 memory (addr + Rowex._header_depth)
let get_count memory addr = get_leuint16 memory (addr + Rowex._header_count)
let is_obsolete version = version land 1 = 1
let is_locked version = version land 0b10 = 0b10
let sizes = [| "B"; "KiB"; "MiB"; "GiB"; "TiB"; "PiB"; "EiB"; "ZiB"; "YiB" |]

let bytes_to_size ?(decimals = 2) ppf = function
  | 0 -> Fmt.string ppf "0 byte"
  | n ->
      let n = float_of_int n in
      let i = Float.floor (Float.log n /. Float.log 1024.) in
      let r = n /. Float.pow 1024. i in
      Fmt.pf ppf "%.*f %s" decimals r sizes.(int_of_float i)

let load_memory filepath =
  let fd = Unix.openfile filepath Unix.[ O_RDONLY ] 0o644 in
  let finally () = Unix.close fd in
  Fun.protect ~finally @@ fun () ->
  let stat = Unix.fstat fd in
  let memory =
    Unix.map_file fd ~pos:0L Bigarray.char Bigarray.c_layout false
      [| stat.st_size |]
  in
  let memory = Bigarray.array1_of_genarray memory in
  memory

let errorf ?(help = false) fmt = Fmt.kstr (fun msg -> `Error (help, msg)) fmt

type stats = {
    obsolete : int
  ; locked : int
  ; node : int
  ; leaf : int
  ; free : int
}

let size_of = function
  | 0 -> Rowex._sizeof_n4
  | 1 -> Rowex._sizeof_n16
  | 2 -> Rowex._sizeof_n48
  | 3 -> Rowex._sizeof_n256
  | _ -> assert false

let rec collect ~max children memory addr i =
  if i >= max then List.rev children
  else begin
    let child = get_leuintnat memory addr in
    if child == (Rowex.Addr.null :> int) then
      collect ~max children memory (addr + size_of_word) (succ i)
    else if child land 1 = 1 then
      collect ~max
        ((child lsr 1) :: children)
        memory (addr + size_of_word) (succ i)
    else collect ~max (child :: children) memory (addr + size_of_word) (succ i)
  end

let collect ~hdr ~max memory addr =
  collect ~max [] memory (addr + Rowex._header_length + hdr) 0

let children_of_node memory addr = function
  | 0 -> collect ~hdr:(4 + Rowex._n4_align_length) ~max:4 memory addr
  | 1 -> collect ~hdr:16 ~max:16 memory addr
  | 2 -> collect ~hdr:256 ~max:48 memory addr
  | 3 -> collect ~hdr:0 ~max:256 memory addr
  | _ -> assert false

let rec count stats memory addr top =
  if addr >= top then stats
  else
    match get_type memory addr with
    | (0 | 1 | 2 | 3) as ty ->
        let stats = { stats with node = stats.node + 1 } in
        let version = get_version memory addr in
        let stats =
          if is_obsolete version then
            {
              stats with
              obsolete = stats.obsolete + 1
            ; free = stats.free + size_of ty
            }
          else stats
        in
        let stats =
          if is_locked version then { stats with obsolete = stats.obsolete + 1 }
          else stats
        in
        count stats memory (addr + size_of ty) top
    | 5 ->
        let stats = { stats with leaf = stats.leaf + 1 } in
        let len = get_leuintnat memory addr in
        let len =
          if Sys.word_size = 64 then len land 0xfffffffffffffff * size_of_word
          else len land 0xfffffff * size_of_word
        in
        count stats memory (addr + len) top
    | _ -> Fmt.failwith "Invalid ROWEX cell at %016x" addr

let empty = { obsolete = 0; locked = 0; node = 0; leaf = 0; free = 0 }

let run _quiet filepath =
  let memory = load_memory (Fpath.to_string filepath) in
  if Bigarray.Array1.dim memory < size_of_word * 2 then
    errorf "Invalid rowex file: %a (it requires %d bytes at least)" Fpath.pp
      filepath (size_of_word * 2)
  else
    let brk = get_leuintnat memory 0 in
    let root = get_leuintnat memory size_of_word in
    let percent =
      let top = float_of_int brk in
      top *. 100. /. float_of_int (Bigarray.Array1.dim memory)
    in
    Fmt.pr "%a (%a):\n%!" Fpath.pp filepath
      (bytes_to_size ?decimals:None)
      (Bigarray.Array1.dim memory);
    Fmt.pr "brk  %016x (%.02f %%)\n%!" brk percent;
    Fmt.pr "root %016x\n%!" root;
    let stats = count empty memory root brk in
    Fmt.pr "     %d node(s)\n%!" stats.node;
    let free_percent =
      let free = float_of_int stats.free in
      free *. 100. /. float_of_int (Bigarray.Array1.dim memory)
    in
    Fmt.pr "     %d locked node(s)\n%!" stats.locked;
    Fmt.pr "     %d obsolete node(s) (%.02f %%, %a)\n%!" stats.obsolete
      free_percent
      (bytes_to_size ?decimals:None)
      stats.free;
    Fmt.pr "     %d entrie(s)\n%!" stats.leaf;
    `Ok ()

open Cmdliner
open Args

let index =
  let doc = "The index file." in
  let parser = Fpath.of_string in
  let pp = Fpath.pp in
  let filepath = Arg.conv (parser, pp) in
  Arg.(required & pos 0 (some filepath) None & info [] ~doc)

let term = Term.(ret (const run $ term_setup_logs $ index))

let cmd =
  let doc = "A simple tool to manipulate an KV-store." in
  let man = [] in
  Cmd.v (Cmd.info "db" ~doc ~man) term

let () = exit (Cmd.eval cmd)
