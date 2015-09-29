<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Enabling Dapper-like Tracing in Hadoop
======================================

* [Enabling Dapper-like Tracing in Hadoop](#Enabling_Dapper-like_Tracing_in_Hadoop)
    * [Dapper-like Tracing in Hadoop](#Dapper-like_Tracing_in_Hadoop)
        * [HTrace](#HTrace)
        * [SpanReceivers](#SpanReceivers)
        * [Dynamic update of tracing configuration](#Dynamic_update_of_tracing_configuration)
        * [Starting tracing spans by HTrace API](#Starting_tracing_spans_by_HTrace_API)
        * [Sample code for tracing](#Sample_code_for_tracing)
        * [Starting tracing spans by configuration for HDFS client](#Starting_tracing_spans_by_configuration_for_HDFS_client)


Dapper-like Tracing in Hadoop
-----------------------------

### HTrace

[HDFS-5274](https://issues.apache.org/jira/browse/HDFS-5274) added support for tracing requests through HDFS,
using the open source tracing library,
[Apache HTrace](http://htrace.incubator.apache.org/).
Setting up tracing is quite simple, however it requires some very minor changes to your client code.

### SpanReceivers

The tracing system works by collecting information in structs called 'Spans'.
It is up to you to choose how you want to receive this information
by using implementation of [SpanReceiver](http://htrace.incubator.apache.org/#Span_Receivers)
interface bundled with HTrace or implementing it by yourself.

[HTrace](http://htrace.incubator.apache.org/) provides options such as

* FlumeSpanReceiver
* HBaseSpanReceiver
* HTracedRESTReceiver
* ZipkinSpanReceiver

See core-default.xml for a description of HTrace configuration keys.  In some
cases, you will also need to add the jar containing the SpanReceiver that you
are using to the classpath of Hadoop on each node. (In the example above,
LocalFileSpanReceiver is included in the htrace-core4 jar which is bundled
with Hadoop.)

```
    $ cp htrace-htraced/target/htrace-htraced-4.0.1-incubating.jar $HADOOP_HOME/share/hadoop/common/lib/
```

### Dynamic update of tracing configuration

You can use `hadoop trace` command to see and update the tracing configuration of each servers.
You must specify IPC server address of namenode or datanode by `-host` option.
You need to run the command against all servers if you want to update the configuration of all servers.

`hadoop trace -list` shows list of loaded span receivers associated with the id.

      $ hadoop trace -list -host 192.168.56.2:9000
      ID  CLASS
      1   org.apache.htrace.core.LocalFileSpanReceiver

      $ hadoop trace -list -host 192.168.56.2:50020
      ID  CLASS
      1   org.apache.htrace.core.LocalFileSpanReceiver

`hadoop trace -remove` removes span receiver from server.
`-remove` options takes id of span receiver as argument.

      $ hadoop trace -remove 1 -host 192.168.56.2:9000
      Removed trace span receiver 1

`hadoop trace -add` adds span receiver to server.
You need to specify the class name of span receiver as argument of `-class` option.
You can specify the configuration associated with span receiver by `-Ckey=value` options.

      $ hadoop trace -add -class org.apache.htrace.core.LocalFileSpanReceiver -Chadoop.htrace.local.file.span.receiver.path=/tmp/htrace.out -host 192.168.56.2:9000
      Added trace span receiver 2 with configuration hadoop.htrace.local.file.span.receiver.path = /tmp/htrace.out

      $ hadoop trace -list -host 192.168.56.2:9000
      ID  CLASS
      2   org.apache.htrace.core.LocalFileSpanReceiver

### Starting tracing spans by HTrace API

In order to trace, you will need to wrap the traced logic with **tracing span** as shown below.
When there is running tracing spans,
the tracing information is propagated to servers along with RPC requests.

```java
    import org.apache.hadoop.hdfs.HdfsConfiguration;
    import org.apache.htrace.core.Tracer;
    import org.apache.htrace.core.TraceScope;

    ...


    ...

        TraceScope ts = tracer.newScope("Gets");
        try {
          ... // traced logic
        } finally {
          ts.close();
        }
```

### Sample code for tracing by HTrace API

The `TracingFsShell.java` shown below is the wrapper of FsShell
which start tracing span before invoking HDFS shell command.

```java
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.FsShell;
    import org.apache.hadoop.hdfs.DFSConfigKeys;
    import org.apache.hadoop.hdfs.HdfsConfiguration;
    import org.apache.hadoop.tracing.TraceUtils;
    import org.apache.hadoop.util.ToolRunner;
    import org.apache.htrace.core.Trace;
    import org.apache.htrace.core.TraceScope;

    public class TracingFsShell {
      public static void main(String argv[]) throws Exception {
        Configuration conf = new HdfsConfiguration();
        FsShell shell = new FsShell();
        conf.setQuietMode(false);
        shell.setConf(conf);
        Tracer tracer = new Tracer.Builder("TracingFsShell").
            conf(TraceUtils.wrapHadoopConf("tracing.fs.shell.htrace.", conf)).
            build();
        int res = 0;
        TraceScope scope = tracer.newScope("FsShell");
        try {
          res = ToolRunner.run(shell, argv);
        } finally {
          scope.close();
          shell.close();
        }
        tracer.close();
        System.exit(res);
      }
    }
```

You can compile and execute this code as shown below.

    $ javac -cp `hadoop classpath` TracingFsShell.java
    $ java -cp .:`hadoop classpath` TracingFsShell -ls /

### Starting tracing spans by configuration for HDFS client

The DFSClient can enable tracing internally. This allows you to use HTrace with
your client without modifying the client source code.

Configure the span receivers and samplers in `hdfs-site.xml`
by properties `fs.client.htrace.sampler.classes` and
`fs.client.htrace.spanreceiver.classes`.  The value of
`fs.client.htrace.sampler.classes` can be NeverSampler, AlwaysSampler or
ProbabilitySampler.

* NeverSampler: HTrace is OFF for all requests to namenodes and datanodes;
* AlwaysSampler: HTrace is ON for all requests to namenodes and datanodes;
* ProbabilitySampler: HTrace is ON for some percentage% of  requests to namenodes and datanodes

```xml
      <property>
        <name>hadoop.htrace.span.receiver.classes</name>
        <value>LocalFileSpanReceiver</value>
      </property>
      <property>
        <name>fs.client.htrace.sampler.classes</name>
        <value>ProbabilitySampler</value>
      </property>
      <property>
        <name>fs.client.htrace.sampler.fraction</name>
        <value>0.01</value>
      </property>
```
