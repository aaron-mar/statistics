accumulo shell << EOF
accumulo
createuser stat
stat
stat
createtable stat_rank
createtable stat_signature

setiter -t stat_signature -class org.stat.accumulo.StatSignatureCombiner -all -p 22

dev

grant Table.READ -t stat_rank -u stat
grant Table.READ -t stat_signature -u stat

grant Table.READ -t stat_rank -u stat
grant Table.WRITE -t stat_rank -u stat
grant Table.BULK_IMPORT -t stat_rank -u stat
grant Table.ALTER_TABLE -t stat_rank -u stat
grant Table.DROP_TABLE -t stat_rank -u stat

grant Table.READ -t stat_signature -u stat
grant Table.WRITE -t stat_signature -u stat
grant Table.BULK_IMPORT -t stat_signature -u stat
grant Table.ALTER_TABLE -t stat_signature -u stat
grant Table.DROP_TABLE -t stat_signature -u stat
