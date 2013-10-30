/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
  <h1>Metrics 2.0</h1>
  <ul id="toc">
    <li><a href="#overview">Overview</a></li>
    <li><a href="#gettingstarted">Getting Started</a></li>
    <li><a href="#config">Configuration</a></li>
    <li><a href="#filtering">Metrics Filtering</a></li>
    <li><a href="#instrumentation">Metrics Instrumentation Strategy</a></li>
    <li><a href="#migration">Migration from previous system</a></li>
  </ul>
  <h2><a name="overview">Overview</a></h2>
  <p>This package provides a framework for metrics instrumentation
    and publication.
  </p>

  <p>The framework provides a variety of ways to implement metrics
    instrumentation easily via the simple
    {@link org.apache.hadoop.metrics2.MetricsSource} interface
    or the even simpler and more concise and declarative metrics annotations.
    The consumers of metrics just need to implement the simple
    {@link org.apache.hadoop.metrics2.MetricsSink} interface. Producers
    register the metrics sources with a metrics system, while consumers
    register the sinks. A default metrics system is provided to marshal
    metrics from sources to sinks based on (per source/sink) configuration
    options. All the metrics are also published and queryable via the
    standard JMX MBean interface. This document targets the framework users.
    Framework developers could also consult the
    <a href="http://wiki.apache.org/hadoop/HADOOP-6728-MetricsV2">design
    document</a> for architecture and implementation notes.
  </p>
  <h3>Sub-packages</h3>
  <dl>
    <dt><code>org.apache.hadoop.metrics2.annotation</code></dt>
    <dd>Public annotation interfaces for simpler metrics instrumentation.
    </dd>
    <dt><code>org.apache.hadoop.metrics2.impl</code></dt>
    <dd>Implementation classes of the framework for interface and/or
      abstract classes defined in the top-level package. Sink plugin code
      usually does not need to reference any class here.
    </dd>
    <dt> <code>org.apache.hadoop.metrics2.lib</code></dt>
    <dd>Convenience classes for implementing metrics sources, including the
      Mutable[{@link org.apache.hadoop.metrics2.lib.MutableGauge Gauge}*|
      {@link org.apache.hadoop.metrics2.lib.MutableCounter Counter}*|
      {@link org.apache.hadoop.metrics2.lib.MutableStat Stat}] and
      {@link org.apache.hadoop.metrics2.lib.MetricsRegistry}.
    </dd>
    <dt> <code>org.apache.hadoop.metrics2.filter</code></dt>
    <dd>Builtin metrics filter implementations include the
      {@link org.apache.hadoop.metrics2.filter.GlobFilter} and
      {@link org.apache.hadoop.metrics2.filter.RegexFilter}.
    </dd>
    <dt><code>org.apache.hadoop.metrics2.source</code></dt>
    <dd>Builtin metrics source implementations including the
      {@link org.apache.hadoop.metrics2.source.JvmMetrics}.
    </dd>
    <dt> <code>org.apache.hadoop.metrics2.sink</code></dt>
    <dd>Builtin metrics sink implementations including the
      {@link org.apache.hadoop.metrics2.sink.FileSink}.
    </dd>
    <dt> <code>org.apache.hadoop.metrics2.util</code></dt>
    <dd>General utilities for implementing metrics sinks etc., including the
      {@link org.apache.hadoop.metrics2.util.MetricsCache}.
    </dd>
  </dl>

  <h2><a name="gettingstarted">Getting started</a></h2>
  <h3>Implementing metrics sources</h3>
  <table width="99%" border="1" cellspacing="0" cellpadding="4">
    <tbody>
      <tr>
        <th>Using annotations</th><th>Using MetricsSource interface</th>
      </tr>
      <tr><td>
  <pre>
  &#064;Metrics(context="MyContext")
  class MyStat {

    &#064;Metric("My metric description")
    public int getMyMetric() {
      return 42;
    }
  }</pre></td><td>
  <pre>
  class MyStat implements MetricsSource {

    &#064;Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      collector.addRecord("MyStat")
          .setContext("MyContext")
          .addGauge(info("MyMetric", "My metric description"), 42);
    }
  }
  </pre>
        </td>
      </tr>
    </tbody>
  </table>
  <p>In this example we introduced the following:</p>
  <dl>
    <dt><em>&#064;Metrics</em></dt>
    <dd>The {@link org.apache.hadoop.metrics2.annotation.Metrics} annotation is
      used to indicate that the class is a metrics source.
    </dd>

    <dt><em>MyContext</em></dt>
    <dd>The optional context name typically identifies either the
      application, or a group of modules within an application or
      library.
    </dd>

    <dt><em>MyStat</em></dt>
    <dd>The class name is used (by default, or specified by name=value parameter
      in the Metrics annotation) as the metrics record name for
      which a set of metrics are to be reported.  For example, you could have a
      record named "CacheStat" for reporting a number of statistics relating to
      the usage of some cache in your application.</dd>

    <dt><em>&#064;Metric</em></dt>
    <dd>The {@link org.apache.hadoop.metrics2.annotation.Metric} annotation
      identifies a particular metric, which in this case, is the
      result of the method call getMyMetric of the "gauge" (default) type,
      which means it can vary in both directions, compared with a "counter"
      type, which can only increase or stay the same. The name of the metric
      is "MyMetric" (inferred from getMyMetric method name by default.) The 42
      here is the value of the metric which can be substituted with any valid
      java expressions.
    </dd>
  </dl>
  <p>Note, the {@link org.apache.hadoop.metrics2.MetricsSource} interface is
    more verbose but more flexible,
    allowing generated metrics names and multiple records. In fact, the
    annotation interface is implemented with the MetricsSource interface
    internally.</p>
  <h3>Implementing metrics sinks</h3>
  <pre>
  public class MySink implements MetricsSink {
    public void putMetrics(MetricsRecord record) {
      System.out.print(record);
    }
    public void init(SubsetConfiguration conf) {}
    public void flush() {}
  }</pre>
  <p>In this example there are three additional concepts:</p>
  <dl>
    <dt><em>record</em></dt>
    <dd>This object corresponds to the record created in metrics sources
      e.g., the "MyStat" in previous example.
    </dd>
    <dt><em>conf</em></dt>
    <dd>The configuration object for the sink instance with prefix removed.
      So you can get any sink specific configuration using the usual
      get* method.
    </dd>
    <dt><em>flush</em></dt>
    <dd>This method is called for each update cycle, which may involve
      more than one record. The sink should try to flush any buffered metrics
      to its backend upon the call. But it's not required that the
      implementation is synchronous.
    </dd>
  </dl>
  <p>In order to make use our <code>MyMetrics</code> and <code>MySink</code>,
    they need to be hooked up to a metrics system. In this case (and most
    cases), the <code>DefaultMetricsSystem</code> would suffice.
  </p>
  <pre>
  DefaultMetricsSystem.initialize("test"); // called once per application
  DefaultMetricsSystem.register(new MyStat());</pre>
  <h2><a name="config">Metrics system configuration</a></h2>
  <p>Sinks are usually specified in a configuration file, say,
  "hadoop-metrics2-test.properties", as:
  </p>
  <pre>
  test.sink.mysink0.class=com.example.hadoop.metrics.MySink</pre>
  <p>The configuration syntax is:</p>
  <pre>
  [prefix].[source|sink|jmx|].[instance].[option]</pre>
  <p>In the previous example, <code>test</code> is the prefix and
    <code>mysink0</code> is an instance name.
    <code>DefaultMetricsSystem</code> would try to load
    <code>hadoop-metrics2-[prefix].properties</code> first, and if not found,
    try the default <code>hadoop-metrics2.properties</code> in the class path.
    Note, the <code>[instance]</code> is an arbitrary name to uniquely
    identify a particular sink instance. The asterisk (<code>*</code>) can be
    used to specify default options.
  </p>
  <p>Consult the metrics instrumentation in jvm, rpc, hdfs and mapred, etc.
    for more examples.
  </p>

  <h2><a name="filtering">Metrics Filtering</a></h2>
  <p>One of the features of the default metrics system is metrics filtering
    configuration by source, context, record/tags and metrics. The least
    expensive way to filter out metrics would be at the source level, e.g.,
    filtering out source named "MyMetrics". The most expensive way would be
    per metric filtering.
  </p>
  <p>Here are some examples:</p>
  <pre>
  test.sink.file0.class=org.apache.hadoop.metrics2.sink.FileSink
  test.sink.file0.context=foo</pre>
  <p>In this example, we configured one sink instance that would
    accept metrics from context <code>foo</code> only.
  </p>
  <pre>
  *.source.filter.class=org.apache.hadoop.metrics2.filter.GlobFilter
  test.*.source.filter.include=foo
  test.*.source.filter.exclude=bar</pre>
  <p>In this example, we specify a source filter that includes source
    <code>foo</code> and excludes <code>bar</code>. When only include
    patterns are specified, the filter operates in the white listing mode,
    where only matched sources are included. Likewise, when only exclude
    patterns are specified, only matched sources are excluded. Sources that
    are not matched in either patterns are included as well when both patterns
    are present. Note, the include patterns have precedence over the exclude
    patterns.
  </p>
  <p>Similarly, you can specify the <code>record.filter</code> and
    <code>metric.filter</code> options, which operate at record and metric
    level, respectively. Filters can be combined to optimize
    the filtering efficiency.</p>

  <h2><a name="instrumentation">Metrics instrumentation strategy</a></h2>

  In previous examples, we showed a minimal example to use the
  metrics framework. In a larger system (like Hadoop) that allows
  custom metrics instrumentation, we recommend the following strategy:
  <pre>
  &#064;Metrics(about="My metrics description", context="MyContext")
  class MyMetrics extends MyInstrumentation {

    &#064;Metric("My gauge description") MutableGaugeInt gauge0;
    &#064;Metric("My counter description") MutableCounterLong counter0;
    &#064;Metric("My rate description") MutableRate rate0;

    &#064;Override public void setGauge0(int value) { gauge0.set(value); }
    &#064;Override public void incrCounter0() { counter0.incr(); }
    &#064;Override public void addRate0(long elapsed) { rate0.add(elapsed); }
  }
  </pre>

  Note, in this example we introduced the following:
  <dl>
    <dt><em>MyInstrumentation</em></dt>
    <dd>This is usually an abstract class (or interface) to define an
      instrumentation interface (incrCounter0 etc.) that allows different
      implementations. This could be a mechanism to allow different metrics
      systems to be used at runtime via configuration.
    </dd>
    <dt><em>Mutable[Gauge*|Counter*|Rate]</em></dt>
    <dd>These are library classes to manage mutable metrics for
      implementations of metrics sources. They produce immutable gauge and
      counters (Metric[Gauge*|Counter*]) for downstream consumption (sinks)
      upon <code>snapshot</code>. The <code>MutableRate</code>
      in particular, provides a way to measure latency and throughput of an
      operation. In this particular case, it produces a long counter
      "Rate0NumOps" and double gauge "Rate0AvgTime" when snapshotted.
    </dd>
  </dl>

  <h2><a name="migration">Migration from previous system</a></h2>
  <p>Users of the previous metrics system would notice the lack of
    <code>context</code> prefix in the configuration examples. The new
    metrics system decouples the concept for context (for grouping) with the
    implementation where a particular context object does the updating and
    publishing of metrics, which causes problems when you want to have a
    single context to be consumed by multiple backends. You would also have to
    configure an implementation instance per context, even if you have a
    backend that can handle multiple contexts (file, gangalia etc.):
  </p>
  <table width="99%" border="1" cellspacing="0" cellpadding="4">
    <tbody>
      <tr>
        <th width="40%">Before</th><th>After</th>
      </tr>
      <tr>
        <td><pre>
  context1.class=org.hadoop.metrics.file.FileContext
  context2.class=org.hadoop.metrics.file.FileContext
  ...
  contextn.class=org.hadoop.metrics.file.FileContext</pre>
        </td>
        <td><pre>
  myprefix.sink.file.class=org.hadoop.metrics2.sink.FileSink</pre>
        </td>
      </tr>
    </tbody>
  </table>
  <p>In the new metrics system, you can simulate the previous behavior by
    using the context option in the sink options like the following:
  </p>
  <table width="99%" border="1" cellspacing="0" cellpadding="4">
    <tbody>
      <tr>
        <th width="40%">Before</th><th>After</th>
      </tr>
      <tr>
        <td><pre>
  context0.class=org.hadoop.metrics.file.FileContext
  context0.fileName=context0.out
  context1.class=org.hadoop.metrics.file.FileContext
  context1.fileName=context1.out
  ...
  contextn.class=org.hadoop.metrics.file.FileContext
  contextn.fileName=contextn.out</pre>
        </td>
        <td><pre>
  myprefix.sink.*.class=org.apache.hadoop.metrics2.sink.FileSink
  myprefix.sink.file0.context=context0
  myprefix.sink.file0.filename=context1.out
  myprefix.sink.file1.context=context1
  myprefix.sink.file1.filename=context1.out
  ...
  myprefix.sink.filen.context=contextn
  myprefix.sink.filen.filename=contextn.out</pre>
        </td>
      </tr>
    </tbody>
  </table>
  <p>to send metrics of a particular context to a particular backend. Note,
    <code>myprefix</code> is an arbitrary prefix for configuration groupings,
    typically they are the name of a particular process
    (<code>namenode</code>, <code>jobtracker</code>, etc.)
  </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
package org.apache.hadoop.metrics2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;