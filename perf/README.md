<!-- Change things from this point on -->

monasca-persister performance benchmarking
=============

This tool benchmarkes the Monasca Persister performance and throughput.
It uses JMeter and Kafka plugin to initiate Kafka metric messages and
query the Dropwizard rest api to retrieve perister processing metrics.
Because the Monasca persister python implementation does not have
similar internal processing metric rest api available, the support
for python implementation performance benchmark will be added in a
future release.

# Install

1. Download and install the latest Apache JMeter from
http://jmeter.apache.org/download_jmeter.cgi.
2. Add JMeter bin directory to the path, for example,
PATH=$PATH:/opt/apache-jmeter/bin.
3. Clone KafkaMeter repository: https://github.com/BrightTag/kafkameter.
4. Run Maven package and install Kafkameter jar to $JMETER_HOME/lib/ext
folder under the Apache JMeter installation home.

# Configure

1. Make a copy of the jmeter_test_plan.jmx file and modify the test plan
to fit your goal. The available options are:
- The number of threads(users)
- The number of loops, i.e., the number of metric messages each thread/user
will create)
- The range of the random values in the metric name and dimension values.
This controls the number of unique metric definitions the test plan will
create.

The total number of metrics = the number of threads(users) * loop count.

# Run test

1. Execute persister_perf.sh, for example,
```
    ./persister_perf.sh -t jmeter_test_plan.jmx -n 1000000 -s 192.168.1.5,192.168.1.6 -p 8091 -w 10

   -n the expected number of metric messages (equals to the number of threads(users) * loop count.
   -s a comma separated list of the Monasca Persister server hosts in the cluster
   -p Monasca Persister server port number
   -w the time to wait for before checking the processing status from the Monasca persister next time
```

2. For each test run, an output folder (postfixed by the timestamp) is
created. You can monitor the log file to watch the progress of jmeter
sending messages, Monasca persister reading messages as well as
Persister flushing the metrics into the time series database. It
includes the accumulated number of metrics created, read and flushed
and snapshots of throughput in each check interval.

3. The output file contains the summary of the performance testing result.
