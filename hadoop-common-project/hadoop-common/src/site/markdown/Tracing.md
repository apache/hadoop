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

Opentelemtry in Hadoop
======================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

### Opentelemtry

[HADOOP-15566](https://issues.apache.org/jira/browse/HADOOP-15566) added support for tracing requests through HDFS,
using the open source tracing library,
[Opentelemtry](https://opentelemetry.io/).

(The following instructions will be removed after the patch has been merged)
Currently HADOOP-15566 is in progress. In order to trace requests you need to apply the [HADOOP-15566-WIP.1.patch](https://issues.apache.org/jira/secure/attachment/13030123/HADOOP-15566-WIP.1.patch)  
and the build
1. git apply HADOOP-15566-WIP.1.patch
2. mvn package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true or  execute the command ./start-build-env.sh
3. Setup [single node cluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) using the build from Step 2.
4. We have introduced two new variables OTEL_TRACES_EXPORTER (for exporting the spans), OTEL_METRICS_EXPORTER (for exporting the metrics)
5. Set `OTEL_TRACES_EXPORTER=jaeger`
6. Bring the jaeger service up using the command `docker run -d --name jaeger \
   -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
   -p 5775:5775/udp \
   -p 6831:6831/udp \
   -p 6832:6832/udp \
   -p 5778:5778 \
   -p 16686:16686 \
   -p 14268:14268 \
   -p 14250:14250 \
   -p 9411:9411 \
   jaegertracing/all-in-one:1.23` more information is available at [jaegertracing.io](https://www.jaegertracing.io/docs/1.23/getting-started/)
7. [Start](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Execution) the Single Node cluster 

### What is OpenTelemetry?
OpenTelemetry is a set of APIs, SDKs, tooling and integrations that are designed for the creation and management of telemetry data 
such as traces, metrics, and logs. The project provides a vendor-agnostic implementation that can be configured to send telemetry data 
to the backend(s) of your choice. It supports a variety of popular open-source projects including Jaeger and Prometheus.

The OpenTelemetry project consists of multiple components. These components are made available as a single implementation to ease adoption and ensure a vendor-agnostic solution. More can be read [here](https://opentelemetry.io/docs/concepts/components/)

We will be mainly discussing OpenTelemetry Tracing. [Tracing API](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md) consists of three main classes
* [TracerProvider](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#tracerprovider) is the entry point of the API. It provides access to Tracers.
* [Tracer](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#tracer) is the class responsible for creating Spans.
* [Span](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#span) is the API to trace an operation.

### Exporter
Provides functionality to emit telemetry to consumers. In our case Trace Exporter helps in collecting the traces at a central location. Exporters supported by OpenTelemetry Java can be found [here](https://github.com/open-telemetry/opentelemetry-java/tree/main/exporters)
* jaeger-thrift
* jaeger
* logging-otlp
* logging
* otlp
* prometheus
* zipkin

We are using jaeger to export the traces. Any library above mentioned can be used you need to make sure that the exporter is available during runtime
for the traces to be exported

### How to enable tracing
We are using OpenTelemetry autoconfigure option thereby the exporter can be provided at the runtime using the environment variables. We haved added two new variables to hadoop-env.sh
```
###
# Opentelemetry Trace Exporters Configuration
###
export OTEL_TRACES_EXPORTER=jaeger
export OTEL_METRICS_EXPORTER=none 
```
Add the exporter lib as a maven dependency to hadoop-project/pom.xml and build/package the project 
```
    <dependency>
        <groupId>io.opentelemetry</groupId>
        <artifactId>opentelemetry-exporter-jaeger</artifactId>
        <version>${opentelemetry.version}</version>
     </dependency>
     <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-netty-shaded</artifactId>
        <version>1.26.0</version>
     </dependency>
```
OR

copy the dependencies to `$HADOOP_HOME/share/hadoop/common/lib/`



### Adding a Trace

We have written wrapper around OpenTelemetry API in order to avoid a lot of code change that would be caused by the new APIs.
Eventually we can remove the old APIs (Wrapper code) module wise.

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
