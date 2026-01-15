let () =
  let ic = open_in_bin Sys.argv.(1) in
  Format.printf "%d\n%!" (in_channel_length ic);
  close_in ic
