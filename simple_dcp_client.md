# Simple DCP Client

This Python script pretends to be a DCP client, to be used for testing purposes.

### Usage:
Currently only python2 supported.

In a terminal:
```
cd /path/to/pydcp
./simple_dcp_client.py <arguments>
```
##### Arguments:
* Username `-u` Username for creating the DCP connection
* Password `-p` Password for creating the DCP connection
* Node `--node, -n` Used to specify which cluster node to connect to (host:port) e.g. `--node localhost:11210`
* Bucket `--bucket, -b` Name of Bucket to connect to e.g. `--bucket <bucketname>`
* Vbuckets List of vbuckets to connect to e.g. `--vbuckets 0 1 2 3 4...` or `-v -1` for all 1024 vbuckets
* Start `--start, -s` (List of) Start sequence number(s) to stream on e.g. `--start 25 15 31 ...` __Note:__ if you are specifying specific start sequence numbers, you must list the same amount as the number of vbuckets.
* End `--end, -e` End sequence number to stream on. The default is `0xffffffffffffffff`
* Xattrs `--xattrs. -x` Include extended attributes
* Collections `--collections, -c` Request collections
* Keys `--keys, -k` Dump keys
* Docs `--docs, -d` Dump documents
* Filter `--filter, -f` DCP Filter
* Delete times `--delete_times` Include delete times in stream
* Compression `--compression`
* Timeout `--timeout, -t` Sets the vbucket connection timeout length in seconds. `-t -1` disables timeout
* Retry limit `--retry-limit` Controls the number of times a vb stream connection is repeated without receiving any activity (updates, deletions, NOOPs etc) before it is closed completely. Defaults to 0
* NOOP Interval `--noop-interval` Sets the time in seconds between NOOP requests from the server
* Opcode Dump `--opcode-dump` Dumps all the received opcodes via print
* Stream Request Info `--stream-req-info` Displays the vbuckets, sequence numbers and UUIDs on every stream request
* UUID `--uuid` Sets the vbucket uuid used for the stream connection. __Note:__ Like start (sequence numbers), the same amount of UUIDs to vbucket connections must be provided
* Failover Logging `--failover-logging` Enables the use of persisted JSON log files for each vbucket, which contain the failover log and sequence numbers (current and old)
* Log Path `--log-path` Sets the file path to use for storing log files e.g. `--log-path /file/path/to/directory` __Note:__ A 'log' folder will be created inside directory
* Keep Logs `--keep-logs, -l` Notifies the script to retain and use the log files currently in the file path directory
