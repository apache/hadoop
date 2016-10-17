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

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

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
    $ cp htrace-htraced/target/htrace-htraced-4.1.0-incubating.jar $HADOOP_HOME/share/hadoop/common/lib/
```

### Dynamic update of tracing configuration

You can use `hadoop trace` command to see and update the tracing configuration of each servers.
You must specify IPC server address of namenode or datanode by `-host` option.
You need to run the command against all servers if you want to update the configuration of all servers.

`hadoop trace -list` shows list of loaded span receivers associated with the id.

      $ hadoop trace -list -host 192.168.56.2:9000
      ID  CLASS
      1   org.apache.htrace.core.LocalFileSpanReceiver

      $ hadoop trace -list -host 192.168.56.2:9867
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

If the cluster is Kerberized, the service principal name must be specified using `-principal` option.
For example, to show list of span receivers of a namenode:

    $ hadoop trace -list -host NN1:8020 -principal namenode/NN1@EXAMPLE.COM

Or, for a datanode:

    $ hadoop trace -list -host DN2:9867 -principal datanode/DN1@EXAMPLE.COM

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
    import org.apache.hadoop.fs.FileSystem;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.conf.Configured;
    import org.apache.hadoop.tracing.TraceUtils;
    import org.apache.hadoop.util.Tool;
    import org.apache.hadoop.util.ToolRunner;
    import org.apache.htrace.core.Tracer;
    import org.apache.htrace.core.TraceScope;
    
    public class Sample extends Configured implements Tool {
      @Override
      public int run(String argv[]) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        Tracer tracer = new Tracer.Builder("Sample").
            conf(TraceUtils.wrapHadoopConf("sample.htrace.", getConf())).
            build();
        int res = 0;
        try (TraceScope scope = tracer.newScope("sample")) {
          Thread.sleep(1000);
          fs.listStatus(new Path("/"));
        }
        tracer.close();
        return res;
      }
      
      public static void main(String argv[]) throws Exception {
        ToolRunner.run(new Sample(), argv);
      }
    }
```

You can compile and execute this code as shown below.

    $ javac -cp `hadoop classpath` Sample.java
    $ java -cp .:`hadoop classpath` Sample \
        -Dsample.htrace.span.receiver.classes=LocalFileSpanReceiver \
        -Dsample.htrace.sampler.classes=AlwaysSampler

### Starting tracing spans by FileSystem Shell

The FileSystem Shell can enable tracing by configuration properties.

Configure the span receivers and samplers in `core-site.xml` or command line
by properties `fs.client.htrace.sampler.classes` and
`fs.client.htrace.spanreceiver.classes`.

    $ hdfs dfs -Dfs.shell.htrace.span.receiver.classes=LocalFileSpanReceiver \
               -Dfs.shell.htrace.sampler.classes=AlwaysSampler \
               -ls /

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
