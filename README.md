# Bancos, a simple KV-store

`bancos` is a KV store, meaning that it can associate a string with a number
(which can refer to a value). This KV store is persistent (meaning that it is
saved in a file or block device) and the structure can be manipulated by
several programs in parallel:
- the structure used is [ROWEX][rowex]
- persistence comes from [P-ART][p-art]

`bancos` is therefore the first step in a database system capable of inserting
(in parallel) new values or searching (also in parallel) for existing values
according to a key.

The implementation of `bancos` does not depend on any particular scheduler, but
we recommend using `bancos` with [miou][miou] to enable parallelization of
readers and writers.

## Tools

A simple example:
```sh
$ git clone https://github.com/robur-coop/bancos.git
$ cd bancos
$ opam pin add -yn .
$ opam install --deps-only -t bancos
$ dune exec bin/sdb.exe -- -i rowex.idx -c test/005.cmds
db: 24139 action(s) committed
```

And 24139 entries were added into the index file `rowex.idx`.
You can check values of them with:
```sh
$ dune exec bin/sdb.exe -- -i rowex.idx -c test/006.cmds
"desirous@pandering.us" => 8252    
"consoling@hubbubs.com.au" => 1697
"compulsory@tattooing.info" => 6262
"colloquialisms@exclude.com" => 5158
"penetrates@inefficiencies.com" => 15859
"dedicated@crocks.net" => 9554
"snoops@thud.net" => 16333
"stable@race.org" => 13586
...
```
