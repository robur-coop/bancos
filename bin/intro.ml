[@@@warning "-27"]

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

let sub_string memory ~off ~len =
  let res = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set res i (get_char memory (off + i))
  done;
  Bytes.unsafe_to_string res

let to_string_base ~bits ~base ~prefix x =
  let y = ref x in
  if !y = 0 then String.make bits '0'
  else
    let buffer = Bytes.create bits in
    let conv = "0123456789abcdef" in
    let i = ref (Bytes.length buffer) in
    while !y <> 0 && !i > 0 do
      let x', digit = (abs (!y / base), abs (!y mod base)) in
      y := x';
      decr i;
      Bytes.set buffer !i conv.[digit]
    done;
    prefix ^ Bytes.sub_string buffer !i (Bytes.length buffer - !i)

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

let get_type memory addr =
  get_leuintnat memory (addr + Rowex._header_kind) lsr Rowex._bits_kind

let get_version memory addr = get_leuintnat memory (addr + Rowex._header_kind)
let get_depth memory addr = get_leuint31 memory (addr + Rowex._header_depth)
let get_count memory addr = get_leuint16 memory (addr + Rowex._header_count)

let get_prefix memory addr =
  let value = get_leuint64 memory (addr + Rowex._header_prefix) in
  let p0 = Int64.(to_int (logand value 0xffffL)) in
  let p1 = Int64.(to_int (logand (shift_right value 16) 0xffffL)) in
  let prefix = Bytes.create Rowex._prefix in
  bytes_set16 prefix 0 p0;
  bytes_set16 prefix 2 p1;
  (value, Bytes.unsafe_to_string prefix, Int64.(to_int (shift_right value 32)))

let get_compact_count memory addr =
  get_leuint16 memory (addr + Rowex._header_compact_count)

let errorf ?(help = false) fmt = Fmt.kstr (fun msg -> `Error (help, msg)) fmt
let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt

type hdr = {
    kind : [ `N4 | `N16 | `N48 | `N256 ]
  ; version : int
  ; prefix_raw : int64
  ; prefix : string
  ; prefix_count : int
  ; depth : int
  ; count : int
  ; compact_count : int
}

let load_hdr memory addr =
  let kind =
    match get_type memory addr with
    | 0 -> `N4
    | 1 -> `N16
    | 2 -> `N48
    | 3 -> `N256
    | _ -> Fmt.failwith "Invalid node (unknown type)"
  in
  let prefix_raw, prefix, prefix_count = get_prefix memory addr in
  let version = get_version memory addr in
  let depth = get_depth memory addr in
  let count = get_count memory addr in
  let compact_count = get_compact_count memory addr in
  {
    kind
  ; version
  ; prefix_raw
  ; prefix
  ; prefix_count
  ; depth
  ; count
  ; compact_count
  }

type n256 = int option array
type n48 = { addr : int; memory : Part.memory; arr : int option array }

type n16 = {
    addr : int
  ; memory : Part.memory
  ; keys : string
  ; arr : int option array
}

type n4 = n16
type node = N4 of n4 | N16 of n16 | N48 of n48 | N256 of n256

let load_n256 memory addr =
  let arr = Array.make 256 None in
  for i = 0 to 255 do
    let addr =
      get_leuintnat memory (addr + Rowex._header_length + (size_of_word * i))
    in
    if addr <> Rowex.Addr.(unsafe_to_int null) then arr.(i) <- Some addr
  done;
  arr

let load_n48 memory addr =
  let arr = Array.make 256 None in
  for i = 0 to 255 do
    let pos = get_int8 memory (addr + Rowex._header_length + i) in
    if pos != 48 then
      arr.(i) <-
        Some
          (get_leuintnat memory
             (addr + Rowex._header_length + 256 + (size_of_word * pos)))
  done;
  { addr; memory; arr }

let load_n16 memory addr =
  let keys = get_leuint128 memory (addr + Rowex._header_length) in
  let arr = Array.make 16 None in
  for i = 0 to 15 do
    let v =
      get_leuintnat memory
        (addr + Rowex._header_length + 16 + (size_of_word * i))
    in
    if (Rowex.Addr.null :> int) <> v then arr.(i) <- Some v
  done;
  { addr; memory; keys; arr }

let load_n4 memory addr =
  let keys = sub_string memory ~off:(addr + Rowex._header_length) ~len:4 in
  let arr = Array.make 4 None in
  for i = 0 to 3 do
    let v =
      get_leuintnat memory
        (addr + Rowex._header_length + 4 + Rowex._n4_align_length
       + (size_of_word * i))
    in
    if (Rowex.Addr.null :> int) <> v then arr.(i) <- Some v
  done;
  { addr; memory; keys; arr }

let load_node memory addr =
  match get_type memory addr with
  | 0 -> N4 (load_n4 memory addr)
  | 1 -> N16 (load_n16 memory addr)
  | 2 -> N48 (load_n48 memory addr)
  | 3 -> N256 (load_n256 memory addr)
  | _ -> Fmt.failwith "Invalid node (unknown type)"

let pp_kind ppf = function
  | `N4 -> Fmt.string ppf "<n4>"
  | `N16 -> Fmt.string ppf "<n16>"
  | `N48 -> Fmt.string ppf "<n48>"
  | `N256 -> Fmt.string ppf "<n256>"

let pp_hex ppf s =
  for idx = 0 to String.length s - 1 do
    let c = s.[idx] in
    Format.fprintf ppf "%02x" (int_of_char c);
    if idx mod 2 = 1 && idx < String.length s - 1 then
      Format.pp_print_string ppf " "
  done

let pp_bin ?(prefix = "") ~bits ppf x =
  Fmt.string ppf (to_string_base ~bits ~prefix ~base:2 x)

let pp_hdr ppf hdr =
  Fmt.pf ppf "     node          %a\n%!" pp_kind hdr.kind;
  Fmt.pf ppf "%04x prefix        %S (%a)\n%!" Rowex._header_prefix hdr.prefix
    pp_hex hdr.prefix;
  Fmt.pf ppf "%04x prefix-count  %d\n%!" Rowex._header_prefix hdr.prefix_count;
  Fmt.pf ppf "     prefix (raw)  %016Lx\n%!" hdr.prefix_raw;
  let version = hdr.version land lnot (0b111 lsl Rowex._bits_kind) in
  Fmt.pf ppf "%04x version       %04x v%d\n%!" Rowex._header_kind version
    (version / 4);
  Fmt.pf ppf "     obsolete?     %b\n%!" (version land 1 = 1);
  Fmt.pf ppf "     locked?       %b\n%!" (version land 0b10 = 0b10);
  Fmt.pf ppf "%04x depth         %d\n%!" Rowex._header_depth hdr.depth;
  Fmt.pf ppf "%04x count         %d\n%!" Rowex._header_count hdr.count;
  Fmt.pf ppf "%04x compact count %d\n%!" Rowex._header_compact_count
    hdr.compact_count

let pp_char ppf x =
  if x >= 33 && x < 127 then Fmt.pf ppf "['%c']" (Char.chr x)
  else Fmt.pf ppf "     "

let pp_pointer ppf x =
  if x land 1 = 1 then Fmt.pf ppf "leaf(%016x)" (x lsr 1)
  else Fmt.pf ppf "%016x" x

let pp_n256 ppf arr =
  for i = 0 to Array.length arr - 1 do
    match arr.(i) with
    | None -> ()
    | Some addr -> Fmt.pf ppf "%a[%02x] => %a\n%!" pp_char i i pp_pointer addr
  done

let pp_n48 ppf ({ addr; memory; arr } : n48) =
  let n48_hdr = sub_string memory ~off:(addr + Rowex._header_length) ~len:256 in
  Fmt.pf ppf "@[<hov>%a@]\n%!" (Hxd_string.pp Hxd.default) n48_hdr;
  Fmt.pf ppf "\n%!";
  for i = 0 to Array.length arr - 1 do
    match arr.(i) with
    | None -> ()
    | Some addr -> Fmt.pf ppf "%a[%02x] => %a\n%!" pp_char i i pp_pointer addr
  done

let pp_n16 ppf { addr; memory; keys; arr } =
  Fmt.pf ppf "%04x arr           %a\n%!" Rowex._header_length pp_hex keys;
  Fmt.pf ppf "\n%!";
  for i = 0 to Array.length arr - 1 do
    match arr.(i) with
    | None -> ()
    | Some addr ->
        let chr = Char.code keys.[i] lxor 128 in
        Fmt.pf ppf "%a[%02x] => %a\n%!" pp_char chr i pp_pointer addr
  done

let pp_n4 ppf { addr; memory; keys; arr } =
  Fmt.pf ppf "%04x arr           %a\n%!" Rowex._header_length pp_hex keys;
  Fmt.pf ppf "\n%!";
  for i = 0 to Array.length arr - 1 do
    match arr.(i) with
    | None -> ()
    | Some addr ->
        let chr = Char.code keys.[i] in
        Fmt.pf ppf "%a[%02x] => %a\n%!" pp_char chr i pp_pointer addr
  done

let pp_node ppf = function
  | N4 n4 -> pp_n4 ppf n4
  | N16 n16 -> pp_n16 ppf n16
  | N48 n48 -> pp_n48 ppf n48
  | N256 n256 -> pp_n256 ppf n256

type t = Addr of int

let run _quiet filepath (Addr addr) as_a_leaf =
  let memory = load_memory (Fpath.to_string filepath) in
  if Bigarray.Array1.dim memory < addr + Rowex._header_length then
    errorf
      "Invalid or truncated rowex file: %a (not enough space to get a valid \
       header)"
      Fpath.pp filepath
  else if not as_a_leaf then (
    let hdr = load_hdr memory addr in
    Fmt.pr "%a@%016x:\n%!" Fpath.pp filepath addr;
    Fmt.pr "%a\n%!" pp_hdr hdr;
    let raw = sub_string memory ~off:addr ~len:Rowex._header_length in
    Fmt.pr "@[<hov>%a@]\n%!" (Hxd_string.pp Hxd.default) raw;
    let node = load_node memory addr in
    Fmt.pr "\n%!";
    Fmt.pr "%a%!" pp_node node;
    `Ok ())
  else
    let length = get_leuintnat memory addr in
    let key = get_ocaml_string memory addr in
    let ofs = (String.length key + size_of_word) / size_of_word in
    let ofs = (1 + ofs) * size_of_word in
    let v = get_leuintnat memory (addr + ofs) in
    Fmt.pr "%a@%016x:\n%!" Fpath.pp filepath addr;
    Fmt.pr "     leaf\n%!";
    Fmt.pr "     length %dw\n%!" length;
    Fmt.pr "     key    %S (%a)\n%!" key pp_hex key;
    Fmt.pr "     value  %d (%016x)\n%!" v v;
    let payload = sub_string memory ~off:addr ~len:(length * size_of_word) in
    Fmt.pr "\n%!";
    Fmt.pr "@[<hov>%a@]\n%!" (Hxd_string.pp Hxd.default) payload;
    `Ok ()

open Cmdliner
open Args

let index =
  let doc = "The index file." in
  let parser = Fpath.of_string in
  let pp = Fpath.pp in
  let filepath = Arg.conv (parser, pp) in
  Arg.(required & pos 0 (some filepath) None & info [] ~doc)

let addr =
  let doc = "The address of the node." in
  let parser str =
    if String.length str > 0 && str.[0] = '@' then
      try Ok (Addr (int_of_string String.(sub str 1 (length str - 1))))
      with _ -> error_msgf "Invalid address: %S" str
    else error_msgf "Unrecognized target: %S" str
  in
  let pp ppf (Addr addr) = Fmt.pf ppf "@0x%016x" addr in
  let addr = Arg.conv (parser, pp) in
  Arg.(required & pos 1 (some addr) None & info [] ~doc)

let as_a_leaf =
  let doc = "Show information of a leaf." in
  Arg.(value & flag & info [ "as-a-leaf" ] ~doc)

let term = Term.(ret (const run $ term_setup_logs $ index $ addr $ as_a_leaf))

let cmd =
  let doc = "A simple tool to show nodes into the given KV-store." in
  let man = [] in
  Cmd.v (Cmd.info "db" ~doc ~man) term

let () = exit (Cmd.eval cmd)
