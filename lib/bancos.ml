let src = Logs.Src.create "bancos"

let try_catch ~exn:fn_exn fn =
  try fn ()
  with exn ->
    let bt = Printexc.get_raw_backtrace () in
    fn_exn bt exn

module Log = (val Logs.src_log src : Logs.LOG)

type wr = [ `Ok | `Duplicate of Rowex.key | `Too_many_retries of Rowex.key ]
type rd = [ `Found of Rowex.key * int | `Not_found of Rowex.key ]

type command =
  | Insert of Rowex.key * int * wr Miou.Computation.t
  | Remove of Rowex.key
  | Lookup of Rowex.key * rd Miou.Computation.t
  | Exists of Rowex.key * bool Miou.Computation.t

type result = [ wr | rd | `Exists of Rowex.key ]

let await = function
  | Remove _ -> `Ok
  | Insert (_, _, ivar) -> (Miou.Computation.await_exn ivar :> result)
  | Lookup (_, ivar) -> (Miou.Computation.await_exn ivar :> result)
  | Exists (key, ivar) ->
      if Miou.Computation.await_exn ivar then `Exists key else `Not_found key

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

let writer t ops =
  Part.writer t.part @@ fun ~uid writer ->
  let fn = function
    | Insert (key, value, ivar) ->
        Log.debug (fun m ->
            m "[%016x] start to insert %S" (uid :> int) (key :> string));
        let fn () =
          Part.insert writer key value;
          Miou.Computation.try_return ivar `Ok
        and exn bt = function
          | Rowex.Duplicate -> Miou.Computation.try_return ivar (`Duplicate key)
          | Rowex.Too_many_retries ->
              Miou.Computation.try_return ivar (`Too_many_retries key)
          | exn -> Miou.Computation.try_cancel ivar (exn, bt)
        in
        assert (try_catch ~exn fn)
    | Remove key -> Part.remove writer key
    | _ -> assert false
  in
  List.iter fn ops

let reader t ops =
  Part.reader t.part @@ fun ~uid:_ reader ->
  let fn = function
    | Lookup (key, ivar) -> begin
        let fn () = `Found (key, Part.lookup reader key) in
        let exn _bt _exn = `Not_found key in
        let result = try_catch ~exn fn in
        assert (Miou.Computation.try_return ivar result)
      end
    | Exists (key, ivar) ->
        let exists = Part.exists reader key in
        assert (Miou.Computation.try_return ivar exists)
    | _ -> assert false
  in
  List.iter fn ops

let task_writer init t () =
  let value = Stdlib.Domain.DLS.get init in
  let () = Lazy.force value in
  let exception Exit in
  try
    while true do
      Miou.Mutex.lock (fst t.txs_locker);
      while Miou.Queue.is_empty t.txs && not (Atomic.get t.close) do
        Miou.Condition.wait (snd t.txs_locker) (fst t.txs_locker)
      done;
      if Atomic.get t.close then raise Exit;
      Miou.Mutex.unlock (fst t.txs_locker);
      let ops = Miou.Queue.(to_list (transfer t.txs)) in
      ignore (writer t ops);
      Miou.Mutex.lock (fst t.idle);
      if (not (Atomic.get t.close)) && t.workers = 0 then
        Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
    done
  with
  | Exit ->
      Miou.Mutex.unlock (fst t.txs_locker);
      Miou.Mutex.lock (fst t.idle);
      t.workers <- t.workers - 1;
      Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
  | _exn ->
      Miou.Mutex.lock (fst t.idle);
      t.workers <- t.workers - 1;
      Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)

let task_reader init t () =
  let value = Stdlib.Domain.DLS.get init in
  let () = Lazy.force value in
  let exception Exit in
  try
    while true do
      Miou.Mutex.lock (fst t.rxs_locker);
      while Miou.Queue.is_empty t.rxs && not (Atomic.get t.close) do
        Miou.Condition.wait (snd t.rxs_locker) (fst t.rxs_locker)
      done;
      if Atomic.get t.close then raise Exit;
      Miou.Mutex.unlock (fst t.rxs_locker);
      let ops = Miou.Queue.(to_list (transfer t.rxs)) in
      reader t ops;
      Miou.Mutex.lock (fst t.idle);
      if (not (Atomic.get t.close)) && t.workers = 0 then
        Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
    done
  with
  | Exit ->
      Miou.Mutex.unlock (fst t.rxs_locker);
      Miou.Mutex.lock (fst t.idle);
      t.workers <- t.workers - 1;
      Miou.Condition.signal (snd t.idle);
      Miou.Mutex.unlock (fst t.idle)
  | _exn ->
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
      Miou.Mutex.protect (fst t.rxs_locker) begin fun () ->
          Miou.Condition.broadcast (snd t.rxs_locker)
        end;
      Miou.Mutex.protect (fst t.txs_locker) begin fun () ->
          Miou.Condition.broadcast (snd t.txs_locker)
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

let nothing =
  let fn () = Lazy.from_val () in
  Stdlib.Domain.DLS.new_key fn

let openfile ?(readers = 4) ?(writers = 2) ?size ?(init = nothing) filepath =
  let part = Part.from_system ?size filepath in
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
  let fnr pin = ignore (Miou.call ~pin ~orphans (task_reader init t)) in
  let fnw pin = ignore (Miou.call ~pin ~orphans (task_writer init t)) in
  List.iter fnr p_readers;
  List.iter fnw p_writers;
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
