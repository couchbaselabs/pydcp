
This is the repo for DCP stream testcases. See below for information on how to
run the testsuite.

Run all tests:

./pydcp

Logging can be changed by specifying 'verbose' option:

(None) - Error logging<br>
 -v    - Warning Logging<br>
 -vv   - Info Logging<br>
 -vvv  - Debug Logging<br>

Test suites can be specified with the 'suite' option:

(None)   - Run all tests<br>
 -s all  - Run all tests<br>
 -s dcp  - Run all DCP related tests<br>
 -s mcd  - Run all memcached related tests<br>

Some unit tests can only be run against certain server types. To specify your
server type use the 'backend' flag below:

(None)   - Couchbase Server<br>
 -b cb   - Couchbase Server<br>
 -b dev  - Couchbase Dev Server (cluster_run)<br>
 -b mcd  - Memcached with ep-engine<br>

The default host and port is 127.0.0.1:11211, but you can change these values by using the 'host' and 'port' flags:

To run the suite with xmlrunner specify the '-x' or '--xml' option

-h 10.5.2.100 -p 12000
