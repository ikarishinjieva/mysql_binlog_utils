mysql_binlog_utils
==================

Some utilities for mysql binlog

WARNING: these utilities are tested for mysql 5.5.33, but are not tested for mysql 5.6+

Dump binlog from pos
----
`DumpBinlogFromPos(srcFilePath string, startPos int, targetFilePath string)`

This function will dump binlog (at `srcFilePath`) from pos (`startPos`), and the output is at `targetFilePath`

If the `startPos` is 0 or 4, the whole binlog will be dump. Otherwise, the source binlog header (including `FORMAT_DESCRIPTION_EVENT` & `ROTATE_EVENT` & `PREVIOUS_GTIDS_LOG_EVENT`) will be dump as the target binlog header, and then the source data will be dump from `startPos`

This will make target binlog complete and available to replay.

Rotate relay log
----
`RotateRelayLog(relayLogPath string, endPos int)`

This function will add a rotate event to a relay log (at `relayLogPath`), after the position (`endPos`), and truncate the data after the position (`endPos`)

WARNING: after manually rotate relay log, DONOT forget to update `relay-log.index`

Fake master server
----
`NewFakeMasterServer(port int, unusedServerId int, characterSet int, keepAliveWhenFinish bool, baseDir string)`

WARNING: only support mysql 5.5.x 

This fake master server is very helpful if you want to replay some binlog files to a mysql instance, and you're afraid of `mysqlbinlog` (http://bugs.mysql.com/bug.php?id=33048, for example)

Mysql replication is [more reliable way](http://www.xaprb.com/blog/2010/09/04/why-mysql-replication-is-better-than-mysqlbinlog-for-recovery/) to replay binlog, what we need is :

1. server := NewFakeMasterServer(...)
2. server.Start()
3. In target mysql instance, `change master` to the fake server, and `start slave`
4. the server will be closed when done or error
5. you can abort the server by `server.Abort()`

####Some other features:

1. `start slave until` is also supported
2. large packet (>= `1<<24-1` bytes) is supported
3. multiple binlog files are supported, fake server will act as a real replication master (rotate to the next when one is finished)

####Arguments:

Argument|_
--- | ---
`port` | the fake server port
`unusedServerId` | the fake server id, should not be duplicate with any other mysql instance
 `characterSet` |the fake server character set id, should be the same with target mysql instance's. You can get the id by `SELECT id, collation_name FROM information_schema.collations ORDER BY id`
`keepAliveWhenFinish` |when false, the fake server will quit when all binlogs are replayed. when true, the fake server will wait for more binlogs.
`baseDir` | where the binlog files are located


----
####Pull requests and issues are warmly welcome