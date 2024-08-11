Simple test with foo
  $ echo "insert foo 42" | MIOU_DOMAINS=1 sdb -i rowex.00.idx
  db: 1 action(s) committed
  $ echo "find foo" | MIOU_DOMAINS=1 sdb -i rowex.00.idx 
  "foo" => 42
  db: 1 action(s) committed
Two key with a prefix [e]
  $ MIOU_DOMAINS=1 sdb -qi rowex.01.idx <<EOF
  > insert desirous@pandering.us 8252
  > insert dedicated@crocks.net 9554
  > EOF
  $ MIOU_DOMAINS=1 sdb -qi rowex.01.idx <<EOF
  > find desirous@pandering.us
  > find dedicated@crocks.net
  > EOF
Three key with a prefix [o]
  $ MIOU_DOMAINS=1 sdb -qi rowex.02.idx <<EOF
  > insert desirous@pandering.us 8252
  > insert consoling@hubbubs.com.au 1697
  > insert compulsory@tattooing.info 6262
  > insert colloquialisms@exclude.com 5158
  > EOF
  $ MIOU_DOMAINS=1 sdb -qi rowex.02.idx <<EOF
  > find desirous@pandering.us
  > find consoling@hubbubs.com.au
  > find compulsory@tattooing.info
  > find colloquialisms@exclude.com
  > EOF
Little corpusLittle corpus
  $ MIOU_DOMAINS=1 sdb -qi rowex.03.idx -c 001.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.03.idx -c 002.cmds
Replacement of <sales@balimandira.com>
  $ MIOU_DOMAINS=1 sdb -qi rowex.04.idx -c 007.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.04.idx -c 008.cmds
Rebalance due to different prefix
  $ MIOU_DOMAINS=1 sdb -qi rowex.05.idx <<EOF
  > insert dedicated@crocks.net 9554
  > insert dedicated@capers.com.au 23724
  > insert dedication@adorned.net 13307
  > insert dedications@cheery.net 5703
  > insert dedicate@bulletined.us 12985
  > insert dedicating@attired.org 7997
  > insert deducted@utilitarianism.us 19880
  > EOF
  $ MIOU_DOMAINS=1 sdb -qi rowex.05.idx <<EOF
  > find dedicated@crocks.net
  > find dedicated@capers.com.au
  > find dedication@adorned.net
  > find dedications@cheery.net
  > find dedicate@bulletined.us
  > find dedicating@attired.org
  > find deducted@utilitarianism.us
  > EOF
Bigger!
  $ MIOU_DOMAINS=1 sdb -qi rowex.06.idx -c 001.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.06.idx -c 002.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.07.idx -c 003.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.07.idx -c 004.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.08.idx -c 005.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.08.idx -c 006.cmds
Repeat, remove and insert!
  $ MIOU_DOMAINS=1 sdb -qi rowex.09.idx -c 005.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.09.idx -c 005.cmds
  $ MIOU_DOMAINS=1 sdb -qi rowex.09.idx -c 006.cmds
