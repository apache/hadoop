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
        * [Samplers](#Samplers)
        * [SpanReceivers](#SpanReceivers)
        * [Setting up ZipkinSpanReceiver](#Setting_up_ZipkinSpanReceiver)
        * [Dynamic update of tracing configuration](#Dynamic_update_of_tracing_configuration)
        * [Starting tracing spans by HTrace API](#Starting_tracing_spans_by_HTrace_API)
        * [Sample code for tracing](#Sample_code_for_tracing)


Dapper-like Tracing in Hadoop
-----------------------------

### HTrace

[HDFS-5274](https://issues.apache.org/jira/browse/HDFS-5274) added support for tracing requests through HDFS,
using the open source tracing library,
[Apache HTrace](https://git-wip-us.apache.org/repos/asf/incubator-htrace.git). 
Setting up tracing is quite simple, however it requires some very minor changes to your client code.

### Samplers

Configure the samplers in `core-site.xml` property: `dfs.htrace.sampler`.
The value can be NeverSampler, AlwaysSampler or ProbabilitySampler.
NeverSampler: HTrace is OFF for all spans;
AlwaysSampler: HTrace is ON for all spans;
ProbabilitySampler: HTrace is ON for some percentage% of top-level spans.

      <property>
        <name>dfs.htrace.sampler</name>
        <value>NeverSampler</value>
      </property>

### SpanReceivers

The tracing system works by collecting information in structs called 'Spans'.
It is up to you to choose how you want to receive this information
by implementing the SpanReceiver interface, which defines one method:

    public void receiveSpan(Span span);

Configure what SpanReceivers you'd like to use
by putting a comma separated list of the fully-qualified class name of classes implementing SpanReceiver
in `core-site.xml` property: `hadoop.htrace.spanreceiver.classes`.

      <property>
        <name>dfs.htrace.spanreceiver.classes</name>
        <value>org.apache.htrace.impl.LocalFileSpanReceiver</value>
      </property>
      <property>
        <name>dfs.htrace.local-file-span-receiver.path</name>
        <value>/var/log/hadoop/htrace.out</value>
      </property>

You can omit package name prefix if you use span receiver bundled with HTrace.

      <property>
        <name>dfs.htrace.spanreceiver.classes</name>
        <value>LocalFileSpanReceiver</value>
      </property>

### Setting up ZipkinSpanReceiver

Instead of implementing SpanReceiver by yourself,
you can use `ZipkinSpanReceiver` which uses
[Zipkin](https://github.com/twitter/zipkin) for collecting and displaying tracing data.

In order to use `ZipkinSpanReceiver`,
you need to download and setup [Zipkin](https://github.com/twitter/zipkin) first. With docker you can start an experimental zipkin server with the following command.

```
docker run -d -p 9411:9411 -p 9410:9410 openzipkin/zipkin
```

you also need to add the jar of `htrace-zipkin` to the classpath of Hadoop on each node.
The easiest way to achieve this is downloading the binary jars from maven central add put it to the hadoop lib directory.

    $ wget http://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.9.0/libthrift-0.9.0.jar -O $HADOOP_HOME/share/hadoop/common/lib/libthrift-0.9.0.jar
    $ wget http://repo1.maven.org/maven2/org/apache/htrace/htrace-zipkin/3.1.0-incubating/htrace-zipkin-3.1.0-incubating.jar -O $HADOOP_HOME/share/hadoop/common/lib/htrace-zipkin-3.1.0-incubating.jar

The sample configuration for `ZipkinSpanReceiver` is shown below.
By adding these to `core-site.xml` of NameNode and DataNodes, `ZipkinSpanReceiver` is initialized on the startup.
You also need this configuration on the client node in addition to the servers.

      <property>
        <name>dfs.htrace.spanreceiver.classes</name>
        <value>ZipkinSpanReceiver</value>
      </property>
      <property>
        <name>dfs.htrace.zipkin.collector-hostname</name>
        <value>192.168.1.2</value>
      </property>
      <property>
        <name>dfs.htrace.zipkin.collector-port</name>
        <value>9410</value>
      </property>

### Dynamic update of tracing configuration

You can use `hadoop trace` command to see and update the tracing configuration of each servers.
You must specify IPC server address of namenode or datanode by `-host` option.
You need to run the command against all servers if you want to update the configuration of all servers.

`hadoop trace -list` shows list of loaded span receivers associated with the id.

      $ hadoop trace -list -host 192.168.56.2:9000
      ID  CLASS
      1   org.apache.htrace.impl.LocalFileSpanReceiver

      $ hadoop trace -list -host 192.168.56.2:50020
      ID  CLASS
      1   org.apache.htrace.impl.LocalFileSpanReceiver

`hadoop trace -remove` removes span receiver from server.
`-remove` options takes id of span receiver as argument.

      $ hadoop trace -remove 1 -host 192.168.56.2:9000
      Removed trace span receiver 1

`hadoop trace -add` adds span receiver to server.
You need to specify the class name of span receiver as argument of `-class` option.
You can specify the configuration associated with span receiver by `-Ckey=value` options.

      $ hadoop trace -add -class LocalFileSpanReceiver -Clocal-file-span-receiver.path=/tmp/htrace.out -host 192.168.56.2:9000
      Added trace span receiver 2 with configuration local-file-span-receiver.path = /tmp/htrace.out

      $ hadoop trace -list -host 192.168.56.2:9000
      ID  CLASS
      2   org.apache.htrace.impl.LocalFileSpanReceiver

### Starting tracing spans by HTrace API

In order to trace, you will need to wrap the traced logic with **tracing span** as shown below.
When there is running tracing spans,
the tracing information is propagated to servers along with RPC requests.

In addition, you need to initialize `SpanReceiver` once per process.

    import org.apache.hadoop.hdfs.HdfsConfiguration;
    import org.apache.hadoop.tracing.SpanReceiverHost;
    import org.apache.htrace.Sampler;
    import org.apache.htrace.Trace;
    import org.apache.htrace.TraceScope;

    ...

        SpanReceiverHost.get(new HdfsConfiguration(), "dfs");

    ...

        TraceScope ts = Trace.startSpan("Gets", Sampler.ALWAYS);
        try {
          ... // traced logic
        } finally {
          if (ts != null) ts.close();
        }

### Sample code for tracing

The `TracingFsShell.java` shown below is the wrapper of FsShell
which start tracing span before invoking HDFS shell command.

    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.FsShell;
    import org.apache.hadoop.tracing.SpanReceiverHost;
    import org.apache.hadoop.util.ToolRunner;
    import org.apache.htrace.Sampler;
    import org.apache.htrace.Trace;
    import org.apache.htrace.TraceScope;

    public class TracingFsShell {
      public static void main(String argv[]) throws Exception {
        Configuration conf = new Configuration();
        FsShell shell = new FsShell();
        conf.setQuietMode(false);
        shell.setConf(conf);
        SpanReceiverHost.get(conf, "dfs");
        int res = 0;
        TraceScope ts = null;
        try {
          ts = Trace.startSpan("FsShell", Sampler.ALWAYS);
          res = ToolRunner.run(shell, argv);
        } finally {
          shell.close();
          if (ts != null) ts.close();
        }
        System.exit(res);
      }
    }

You can compile and execute this code as shown below.

    $ javac -cp `hadoop classpath` TracingFsShell.java
    $ java -cp .:`hadoop classpath` TracingFsShell -ls /

The configuration prefix for the client-side htrace configuration is defined in the SpanReceiverHost.get call. In the case above you should use the dfs prefix as on the server side. (*dfs*.htrace…. configuration values)
