let src = Logs.Src.create "db"

module Log = (val Logs.src_log src : Logs.LOG)

type command =
  | Insert of Rowex.key * int * [ `Ok | `Duplicate ] Miou.Computation.t
  | Remove of Rowex.key
  | Lookup of Rowex.key * [ `Found of int | `Not_found ] Miou.Computation.t
  | Exists of Rowex.key * bool Miou.Computation.t

type result =
  [ `Ok
  | `Not_found of Rowex.key
  | `Found of Rowex.key * int
  | `Duplicate of Rowex.key
  | `Exists of Rowex.key ]

let await = function
  | Remove _ -> `Ok
  | Insert (key, _, res) -> begin
      match Miou.Computation.await_exn res with
      | `Ok -> `Ok
      | `Duplicate -> `Duplicate key
    end
  | Lookup (key, res) -> begin
      match Miou.Computation.await_exn res with
      | `Found value -> `Found (key, value)
      | `Not_found -> `Not_found key
    end
  | Exists (key, res) -> begin
      match Miou.Computation.await_exn res with
      | true -> `Exists key
      | false -> `Not_found key
    end

let is_running = function
  | Remove _ -> false
  | Insert (_, _, res) -> Miou.Computation.is_running res
  | Lookup (_, res) -> Miou.Computation.is_running res
  | Exists (_, res) -> Miou.Computation.is_running res

type t = {
    txs : command Miou.Queue.t
  ; txs_locker : Miou.Mutex.t * Miou.Condition.t
  ; rxs : command Miou.Queue.t
  ; rxs_locker : Miou.Mutex.t * Miou.Condition.t
  ; mutable close : bool Atomic.t
  ; mutable workers : int
  ; idle : Miou.Mutex.t * Miou.Condition.t
  ; part : Part.t
  ; orphans : unit Miou.orphans
}

let do_wrs t ~uid wrs =
  Part.writer t.part @@ fun writer ->
  let do_wr = function
    | Insert (key, value, res) -> begin
        Log.debug (fun m -> m "[%02x] start to insert %S" uid (key :> string));
        match Part.insert writer key value with
        | () ->
            let set = Miou.Computation.try_return res `Ok in
            Log.debug (fun m ->
                m "[%02x] %S inserted (set:%b)" uid (key :> string) set)
        | exception Rowex.Duplicate ->
            let set = Miou.Computation.try_return res `Duplicate in
            Log.debug (fun m ->
                m "[%02x] %S is a duplicate (set:%b)" uid (key :> string) set)
        | exception exn ->
            let bt = Printexc.get_raw_backtrace () in
            let set = Miou.Computation.try_cancel res (exn, bt) in
            Log.err (fun m ->
                m "[%02x] errored by %S (set:%b)" uid (Printexc.to_string exn)
                  set)
      end
    | Remove key -> Part.remove writer key
    | _ -> assert false
  in
  List.iter do_wr wrs;
  Log.debug (fun m ->
      m "[%02x] finished its tasks (%d task(s))" uid (List.length wrs))

let do_wrs ~uid t wrs =
  match do_wrs t ~uid wrs with
  | Ok () -> ()
  | Error exn ->
      Log.err (fun m -> m "[%02x] failed with %S" uid (Printexc.to_string exn))

let do_rds t rds =
  let reader = Part.reader t.part in
  let do_rd = function
    | Lookup (key, res) -> begin
        match Part.lookup reader key with
        | value -> assert (Miou.Computation.try_return res (`Found value))
        | exception Not_found ->
            assert (Miou.Computation.try_return res `Not_found)
      end
    | Exists (key, res) ->
        let exists = Part.exists reader key in
        assert (Miou.Computation.try_return res exists)
    | _ -> assert false
  in
  List.iter do_rd rds

let writer ~uid t () =
  Log.debug (fun m -> m "writer [%02x] launched" uid);
  let exception Exit in
  try
    while true do
      Log.debug (fun m -> m "writer [%02x] idle" uid);
      Miou.Mutex.lock (fst t.txs_locker);
      while Miou.Queue.is_empty t.txs && not (Atomic.get t.close) do
        Miou.Condition.wait (snd t.txs_locker) (fst t.txs_locker)
      done;
      if Atomic.get t.close then raise Exit;
      Miou.Mutex.unlock (fst t.txs_locker);
      Log.debug (fun m -> m "writer [%02x] runs" uid);
      let wrs = Miou.Queue.(to_list (transfer t.txs)) in
      do_wrs ~uid t wrs;
      Log.debug (fun m -> m "writer [%02x] sleep" uid);
      Miou.Mutex.lock (fst t.idle);
      if (not (Atomic.get t.close)) && t.workers = 0 then
        Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
    done
  with
  | Exit ->
      Miou.Mutex.unlock (fst t.txs_locker);
      Log.debug (fun m -> m "writer [%02x] quit" uid);
      Miou.Mutex.lock (fst t.idle);
      t.workers <- t.workers - 1;
      Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
  | exn ->
      Log.err (fun m ->
          m "writer [%02x] exited with: %S" uid (Printexc.to_string exn));
      Miou.Mutex.lock (fst t.idle);
      t.workers <- t.workers - 1;
      Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)

let reader t () =
  let exception Exit in
  try
    while true do
      Miou.Mutex.lock (fst t.rxs_locker);
      while Miou.Queue.is_empty t.rxs && not (Atomic.get t.close) do
        Miou.Condition.wait (snd t.rxs_locker) (fst t.rxs_locker)
      done;
      if Atomic.get t.close then raise Exit;
      Miou.Mutex.unlock (fst t.rxs_locker);
      let rds = Miou.Queue.(to_list (transfer t.rxs)) in
      do_rds t rds;
      Miou.Mutex.lock (fst t.idle);
      if (not (Atomic.get t.close)) && t.workers = 0 then
        Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
    done
  with
  | Exit ->
      Miou.Mutex.unlock (fst t.rxs_locker);
      Log.debug (fun m -> m "reader quit");
      Miou.Mutex.lock (fst t.idle);
      t.workers <- t.workers - 1;
      Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
  | exn ->
      Log.err (fun m -> m "reader exited with: %S" (Printexc.to_string exn));
      Miou.Mutex.lock (fst t.idle);
      t.workers <- t.workers - 1;
      Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)

let rec terminate t =
  match Miou.care t.orphans with
  | None -> ()
  | Some None ->
      Miou.yield ();
      terminate t
  | Some (Some prm) ->
      Miou.await_exn prm;
      terminate t

let close t =
  let exception Exit in
  let rec go backoff =
    let closed = Atomic.get t.close in
    if (not closed) && Atomic.compare_and_set t.close false true then begin
      Miou.Mutex.protect (fst t.rxs_locker)
        begin
          fun () -> Miou.Condition.broadcast (snd t.rxs_locker)
        end;
      Miou.Mutex.protect (fst t.txs_locker)
        begin
          fun () -> Miou.Condition.broadcast (snd t.txs_locker)
        end;
      try
        while true do
          Miou.Mutex.lock (fst t.idle);
          if t.workers <= 0 then raise Exit;
          Miou.Condition.wait (snd t.idle) (fst t.idle);
          Miou.Mutex.unlock (fst t.idle)
        done
      with Exit -> Miou.Mutex.unlock (fst t.idle)
    end
    else go (Miou.Backoff.once backoff)
  in
  go Miou.Backoff.default;
  terminate t

let gen =
  let v = Atomic.make 0 in
  fun () -> Atomic.fetch_and_add v 1

let openfile ?(readers = 4) ?(writers = 2) filepath =
  let part = Part.from_system ~filepath in
  let domains = Miou.Domain.all () in
  if List.length domains < readers + writers then
    Fmt.invalid_arg "We don't have enough domains for %d readers and %d writers"
      readers writers;
  let rec go (p_readers, p_writers) domains =
    if List.length p_readers = readers && List.length p_writers = writers then
      (p_readers, p_writers)
    else if List.length p_readers = readers then
      go (p_readers, List.hd domains :: p_writers) (List.tl domains)
    else go (List.hd domains :: p_readers, p_writers) (List.tl domains)
  in
  let p_readers, p_writers = go ([], []) domains in
  let orphans = Miou.orphans () in
  let t =
    {
      txs = Miou.Queue.create ()
    ; txs_locker = Miou.(Mutex.create (), Condition.create ())
    ; rxs = Miou.Queue.create ()
    ; rxs_locker = Miou.(Mutex.create (), Condition.create ())
    ; close = Atomic.make false
    ; workers = readers + writers
    ; idle = Miou.(Mutex.create (), Condition.create ())
    ; part
    ; orphans
    }
  in
  List.iter (fun pin -> ignore (Miou.call ~pin ~orphans (reader t))) p_readers;
  List.iter
    (fun pin -> ignore (Miou.call ~pin ~orphans (writer t ~uid:(gen ()))))
    p_writers;
  t

let lookup t key =
  let cmd = Lookup (key, Miou.Computation.create ()) in
  Miou.Queue.enqueue t.rxs cmd;
  Miou.Condition.signal (snd t.rxs_locker);
  cmd

let exists t key =
  let cmd = Exists (key, Miou.Computation.create ()) in
  Miou.Queue.enqueue t.rxs cmd;
  Miou.Condition.signal (snd t.rxs_locker);
  cmd

let remove t key =
  let cmd = Remove key in
  Miou.Queue.enqueue t.txs cmd;
  Miou.Condition.signal (snd t.txs_locker);
  cmd

let insert t key value =
  let cmd = Insert (key, value, Miou.Computation.create ()) in
  Miou.Queue.enqueue t.txs cmd;
  Miou.Condition.signal (snd t.txs_locker);
  cmd
