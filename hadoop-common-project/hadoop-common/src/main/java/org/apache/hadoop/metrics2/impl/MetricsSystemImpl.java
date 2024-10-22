/**
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

package org.apache.hadoop.metrics2.impl;

import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import javax.management.ObjectName;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.classification.VisibleForTesting;
import static org.apache.hadoop.util.Preconditions.*;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import static org.apache.hadoop.metrics2.impl.MetricsConfig.*;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricsSourceBuilder;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for metrics system singletons
 */
@InterfaceAudience.Private
@Metrics(context="metricssystem")
public class MetricsSystemImpl extends MetricsSystem implements MetricsSource {

  static final Logger LOG = LoggerFactory.getLogger(MetricsSystemImpl.class);
  static final String MS_NAME = "MetricsSystem";
  static final String MS_STATS_NAME = MS_NAME +",sub=Stats";
  static final String MS_STATS_DESC = "Metrics system metrics";
  static final String MS_CONTROL_NAME = MS_NAME +",sub=Control";
  static final String MS_INIT_MODE_KEY = "hadoop.metrics.init.mode";

  enum InitMode { NORMAL, STANDBY }

  private final Map<String, MetricsSourceAdapter> sources;
  private final Map<String, MetricsSource> allSources;
  private final Map<String, MetricsSinkAdapter> sinks;
  private final Map<String, MetricsSink> allSinks;

  // The callback list is used by register(Callback callback), while
  // the callback map is used by register(String name, String desc, T sink)
  private final List<Callback> callbacks;
  private final Map<String, Callback> namedCallbacks;

  private final MetricsCollectorImpl collector;
  private final MetricsRegistry registry = new MetricsRegistry(MS_NAME);
  @Metric({"Snapshot", "Snapshot stats"}) MutableStat snapshotStat;
  @Metric({"Publish", "Publishing stats"}) MutableStat publishStat;
  @Metric("Dropped updates by all sinks") MutableCounterLong droppedPubAll;

  private final List<MetricsTag> injectedTags;

  // Things that are changed by init()/start()/stop()
  private String prefix;
  private MetricsFilter sourceFilter;
  private MetricsConfig config;
  private Map<String, MetricsConfig> sourceConfigs, sinkConfigs;
  private boolean monitoring = false;
  private Timer timer;
  private long period; // milliseconds
  private long logicalTime; // number of timer invocations * period
  private ObjectName mbeanName;
  private boolean publishSelfMetrics = true;
  private MetricsSourceAdapter sysSource;
  private int refCount = 0; // for mini cluster mode

  /**
   * Construct the metrics system
   * @param prefix  for the system
   */
  public MetricsSystemImpl(String prefix) {
    this.prefix = prefix;
    allSources = Maps.newHashMap();
    sources = Maps.newLinkedHashMap();
    allSinks = Maps.newHashMap();
    sinks = Maps.newLinkedHashMap();
    sourceConfigs = Maps.newHashMap();
    sinkConfigs = Maps.newHashMap();
    callbacks = Lists.newArrayList();
    namedCallbacks = Maps.newHashMap();
    injectedTags = Lists.newArrayList();
    collector = new MetricsCollectorImpl();
    if (prefix != null) {
      // prefix could be null for default ctor, which requires init later
      initSystemMBean();
    }
  }

  /**
   * Construct the system but not initializing (read config etc.) it.
   */
  public MetricsSystemImpl() {
    this(null);
  }

  /**
   * Initialized the metrics system with a prefix.
   * @param prefix  the system will look for configs with the prefix
   * @return the metrics system object itself
   */
  @Override
  public synchronized MetricsSystem init(String prefix) {
    if (monitoring && !DefaultMetricsSystem.inMiniClusterMode()) {
      LOG.warn(this.prefix +" metrics system already initialized!");
      return this;
    }
    this.prefix = checkNotNull(prefix, "prefix");
    ++refCount;
    if (monitoring) {
      // in mini cluster mode
      LOG.debug("{} metrics system started (again)", prefix);
      return this;
    }
    switch (initMode()) {
      case NORMAL:
        try { start(); }
        catch (MetricsConfigException e) {
          // Configuration errors (e.g., typos) should not be fatal.
          // We can always start the metrics system later via JMX.
          LOG.warn("Metrics system not started: "+ e.getMessage());
          LOG.debug("Stacktrace: ", e);
        }
        break;
      case STANDBY:
        LOG.debug("{} metrics system started in standby mode", prefix);
    }
    initSystemMBean();
    return this;
  }

  @Override
  public synchronized void start() {
    checkNotNull(prefix, "prefix");
    if (monitoring) {
      LOG.warn(prefix +" metrics system already started!",
               new MetricsException("Illegal start"));
      return;
    }
    for (Callback cb : callbacks) cb.preStart();
    for (Callback cb : namedCallbacks.values()) cb.preStart();
    configure(prefix);
    startTimer();
    monitoring = true;
    LOG.debug("{} metrics system started", prefix);
    for (Callback cb : callbacks) cb.postStart();
    for (Callback cb : namedCallbacks.values()) cb.postStart();
  }

  @Override
  public synchronized void stop() {
    if (!monitoring && !DefaultMetricsSystem.inMiniClusterMode()) {
      LOG.warn(prefix +" metrics system not yet started!",
               new MetricsException("Illegal stop"));
      return;
    }
    if (!monitoring) {
      // in mini cluster mode
      LOG.debug("{} metrics system stopped (again)", prefix);
      return;
    }
    for (Callback cb : callbacks) cb.preStop();
    for (Callback cb : namedCallbacks.values()) cb.preStop();
    LOG.debug("Stopping {} metrics system...", prefix);
    stopTimer();
    stopSources();
    stopSinks();
    clearConfigs();
    monitoring = false;
    LOG.debug("{} metrics system stopped.", prefix);
    for (Callback cb : callbacks) cb.postStop();
    for (Callback cb : namedCallbacks.values()) cb.postStop();
  }

  @Override public synchronized <T>
  T register(String name, String desc, T source) {
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(source);
    final MetricsSource s = sb.build();
    MetricsInfo si = sb.info();
    String name2 = name == null ? si.name() : name;
    final String finalDesc = desc == null ? si.description() : desc;
    final String finalName = // be friendly to non-metrics tests
        DefaultMetricsSystem.sourceName(name2, !monitoring);
    allSources.put(finalName, s);
    LOG.debug(finalName +", "+ finalDesc);
    if (monitoring) {
      registerSource(finalName, finalDesc, s);
    }
    // We want to re-register the source to pick up new config when the
    // metrics system restarts.
    register(finalName, new AbstractCallback() {
      @Override public void postStart() {
        registerSource(finalName, finalDesc, s);
      }
    });
    return source;
  }

  @Override public synchronized
  void unregisterSource(String name) {
    if (sources.containsKey(name)) {
      sources.get(name).stop();
      sources.remove(name);
    }
    if (allSources.containsKey(name)) {
      allSources.remove(name);
    }
    if (namedCallbacks.containsKey(name)) {
      namedCallbacks.remove(name);
    }
    DefaultMetricsSystem.removeSourceName(name);
  }

  synchronized
  void registerSource(String name, String desc, MetricsSource source) {
    checkNotNull(config, "config");
    MetricsConfig conf = sourceConfigs.get(name);
    MetricsSourceAdapter sa = new MetricsSourceAdapter(prefix, name, desc,
        source, injectedTags, period, conf != null ? conf
            : config.subset(SOURCE_KEY));
    sources.put(name, sa);
    sa.start();
    LOG.debug("Registered source "+ name);
  }

  @Override public synchronized <T extends MetricsSink>
  T register(final String name, final String description, final T sink) {
    LOG.debug(name +", "+ description);
    if (allSinks.containsKey(name)) {
      if(config != null && sinks.get(name) == null) {
        registerSink(name, description, sink);
      } else {
        LOG.warn("Sink "+ name +" already exists!");
      }
      return sink;
    }
    if (config != null) {
      registerSink(name, description, sink);
    }
    // We want to re-register the sink to pick up new config
    // when the metrics system restarts.
    register(name, new AbstractCallback() {
      @Override public void postStart() {
        register(name, description, sink);
      }
    });
    return sink;
  }

  synchronized void registerSink(String name, String desc, MetricsSink sink) {
    checkNotNull(config, "config");
    MetricsConfig conf = sinkConfigs.get(name);
    MetricsSinkAdapter sa = conf != null
        ? newSink(name, desc, sink, conf)
        : newSink(name, desc, sink, config.subset(SINK_KEY));
    sinks.put(name, sa);
    allSinks.put(name, sink);
    sa.start();
    LOG.debug("Registered sink {}", name);
  }

  @Override
  public synchronized void register(final Callback callback) {
    callbacks.add((Callback) getProxyForCallback(callback));
  }

  private synchronized void register(String name, final Callback callback) {
    namedCallbacks.put(name, (Callback) getProxyForCallback(callback));
  }

  private Object getProxyForCallback(final Callback callback) {
    return Proxy.newProxyInstance(callback.getClass().getClassLoader(),
        new Class<?>[] { Callback.class }, new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            try {
              return method.invoke(callback, args);
            } catch (Exception e) {
              // These are not considered fatal.
              LOG.warn("Caught exception in callback " + method.getName(), e);
            }
            return null;
          }
        });
  }

  @Override
  public synchronized void startMetricsMBeans() {
    for (MetricsSourceAdapter sa : sources.values()) {
      sa.startMBeans();
    }
  }

  @Override
  public synchronized void stopMetricsMBeans() {
    for (MetricsSourceAdapter sa : sources.values()) {
      sa.stopMBeans();
    }
  }

  @Override
  public synchronized String currentConfig() {
    PropertiesConfiguration saver = new PropertiesConfiguration();
    StringWriter writer = new StringWriter();
    saver.copy(config);
    try { saver.write(writer); }
    catch (Exception e) {
      throw new MetricsConfigException("Error stringify config", e);
    }
    return writer.toString();
  }

  private synchronized void startTimer() {
    if (timer != null) {
      LOG.warn(prefix +" metrics system timer already started!");
      return;
    }
    logicalTime = 0;
    long millis = period;
    timer = new Timer("Timer for '"+ prefix +"' metrics system", true);
    timer.scheduleAtFixedRate(new TimerTask() {
          @Override
          public void run() {
            try {
              onTimerEvent();
            } catch (Exception e) {
              LOG.warn("Error invoking metrics timer", e);
            }
          }
        }, millis, millis);
    LOG.debug("Scheduled Metric snapshot period at {} second(s).", period / 1000);
  }

  synchronized void onTimerEvent() {
    logicalTime += period;
    if (sinks.size() > 0) {
      publishMetrics(sampleMetrics(), false);
    }
  }
  
  /**
   * Requests an immediate publish of all metrics from sources to sinks.
   */
  @Override
  public synchronized void publishMetricsNow() {
    if (sinks.size() > 0) {
      publishMetrics(sampleMetrics(), true);
    }    
  }

  /**
   * Sample all the sources for a snapshot of metrics/tags
   * @return  the metrics buffer containing the snapshot
   */
  @VisibleForTesting
  public synchronized MetricsBuffer sampleMetrics() {
    collector.clear();
    MetricsBufferBuilder bufferBuilder = new MetricsBufferBuilder();

    for (Entry<String, MetricsSourceAdapter> entry : sources.entrySet()) {
      if (sourceFilter == null || sourceFilter.accepts(entry.getKey())) {
        snapshotMetrics(entry.getValue(), bufferBuilder);
      }
    }
    if (publishSelfMetrics) {
      snapshotMetrics(sysSource, bufferBuilder);
    }
    MetricsBuffer buffer = bufferBuilder.get();
    return buffer;
  }

  private void snapshotMetrics(MetricsSourceAdapter sa,
                               MetricsBufferBuilder bufferBuilder) {
    long startTime = Time.monotonicNow();
    bufferBuilder.add(sa.name(), sa.getMetrics(collector, true));
    collector.clear();
    snapshotStat.add(Time.monotonicNow() - startTime);
    LOG.debug("Snapshotted source "+ sa.name());
  }

  /**
   * Publish a metrics snapshot to all the sinks
   * @param buffer  the metrics snapshot to publish
   * @param immediate  indicates that we should publish metrics immediately
   *                   instead of using a separate thread.
   */
  synchronized void publishMetrics(MetricsBuffer buffer, boolean immediate) {
    int dropped = 0;
    for (MetricsSinkAdapter sa : sinks.values()) {
      long startTime = Time.monotonicNow();
      boolean result;
      if (immediate) {
        result = sa.putMetricsImmediate(buffer); 
      } else {
        result = sa.putMetrics(buffer, logicalTime);
      }
      dropped += result ? 0 : 1;
      publishStat.add(Time.monotonicNow() - startTime);
    }
    droppedPubAll.incr(dropped);
  }

  private synchronized void stopTimer() {
    if (timer == null) {
      LOG.warn(prefix +" metrics system timer already stopped!");
      return;
    }
    timer.cancel();
    timer = null;
  }

  private synchronized void stopSources() {
    for (Entry<String, MetricsSourceAdapter> entry : sources.entrySet()) {
      MetricsSourceAdapter sa = entry.getValue();
      LOG.debug("Stopping metrics source "+ entry.getKey() +
          ": class=" + sa.source().getClass());
      sa.stop();
    }
    sysSource.stop();
    sources.clear();
  }

  private synchronized void stopSinks() {
    for (Entry<String, MetricsSinkAdapter> entry : sinks.entrySet()) {
      MetricsSinkAdapter sa = entry.getValue();
      LOG.debug("Stopping metrics sink "+ entry.getKey() +
          ": class=" + sa.sink().getClass());
      sa.stop();
    }
    sinks.clear();
  }

  private synchronized void configure(String prefix) {
    config = MetricsConfig.create(prefix);
    configureSinks();
    configureSources();
    configureSystem();
  }

  private synchronized void configureSystem() {
    injectedTags.add(Interns.tag(MsInfo.Hostname, getHostname()));
  }

  private synchronized void configureSinks() {
    sinkConfigs = config.getInstanceConfigs(SINK_KEY);
    long confPeriodMillis = 0;
    for (Entry<String, MetricsConfig> entry : sinkConfigs.entrySet()) {
      MetricsConfig conf = entry.getValue();
      int sinkPeriod = conf.getInt(PERIOD_KEY, PERIOD_DEFAULT);
      // Support configuring periodMillis for testing.
      long sinkPeriodMillis =
          conf.getLong(PERIOD_MILLIS_KEY, sinkPeriod * 1000);
      confPeriodMillis = confPeriodMillis == 0 ? sinkPeriodMillis
          : ArithmeticUtils.gcd(confPeriodMillis, sinkPeriodMillis);
      String clsName = conf.getClassName("");
      if (clsName == null) continue;  // sink can be registered later on
      String sinkName = entry.getKey();
      try {
        MetricsSinkAdapter sa = newSink(sinkName,
            conf.getString(DESC_KEY, sinkName), conf);
        sa.start();
        sinks.put(sinkName, sa);
        allSinks.put(sinkName, sa.sink());
      } catch (Exception e) {
        LOG.warn("Error creating sink '"+ sinkName +"'", e);
      }
    }
    long periodSec = config.getInt(PERIOD_KEY, PERIOD_DEFAULT);
    period = confPeriodMillis > 0 ? confPeriodMillis
        : config.getLong(PERIOD_MILLIS_KEY, periodSec * 1000);
  }

  static MetricsSinkAdapter newSink(String name, String desc, MetricsSink sink,
                                    MetricsConfig conf) {
    return new MetricsSinkAdapter(name, desc, sink, conf.getString(CONTEXT_KEY),
        conf.getFilter(SOURCE_FILTER_KEY),
        conf.getFilter(RECORD_FILTER_KEY),
        conf.getFilter(METRIC_FILTER_KEY),
        conf.getInt(PERIOD_KEY, PERIOD_DEFAULT) * 1000,
        conf.getInt(QUEUE_CAPACITY_KEY, QUEUE_CAPACITY_DEFAULT),
        conf.getInt(RETRY_DELAY_KEY, RETRY_DELAY_DEFAULT),
        conf.getFloat(RETRY_BACKOFF_KEY, RETRY_BACKOFF_DEFAULT),
        conf.getInt(RETRY_COUNT_KEY, RETRY_COUNT_DEFAULT));
  }

  static MetricsSinkAdapter newSink(String name, String desc,
                                    MetricsConfig conf) {
    return newSink(name, desc, (MetricsSink) conf.getPlugin(""), conf);
  }

  private void configureSources() {
    sourceFilter = config.getFilter(PREFIX_DEFAULT + SOURCE_FILTER_KEY);
    sourceConfigs = config.getInstanceConfigs(SOURCE_KEY);
    registerSystemSource();
  }

  private void clearConfigs() {
    sinkConfigs.clear();
    sourceConfigs.clear();
    injectedTags.clear();
    config = null;
  }

  static String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOG.error("Error getting localhost name. Using 'localhost'...", e);
    }
    return "localhost";
  }

  private void registerSystemSource() {
    MetricsConfig sysConf = sourceConfigs.get(MS_NAME);
    sysSource = new MetricsSourceAdapter(prefix, MS_STATS_NAME, MS_STATS_DESC,
        MetricsAnnotations.makeSource(this), injectedTags, period,
        sysConf == null ? config.subset(SOURCE_KEY) : sysConf);
    sysSource.start();
  }

  @Override
  public synchronized void getMetrics(MetricsCollector builder, boolean all) {
    MetricsRecordBuilder rb = builder.addRecord(MS_NAME)
        .addGauge(MsInfo.NumActiveSources, sources.size())
        .addGauge(MsInfo.NumAllSources, allSources.size())
        .addGauge(MsInfo.NumActiveSinks, sinks.size())
        .addGauge(MsInfo.NumAllSinks, allSinks.size());

    for (MetricsSinkAdapter sa : sinks.values()) {
      sa.snapshot(rb, all);
    }
    registry.snapshot(rb, all);
  }

  private void initSystemMBean() {
    checkNotNull(prefix, "prefix should not be null here!");
    if (mbeanName == null) {
      mbeanName = MBeans.register(prefix, MS_CONTROL_NAME, this);
    }
  }

  @Override
  public synchronized boolean shutdown() {
    LOG.debug("refCount="+ refCount);
    if (refCount <= 0) {
      LOG.debug("Redundant shutdown", new Throwable());
      return true; // already shutdown
    }
    if (--refCount > 0) return false;
    if (monitoring) {
      try { stop(); }
      catch (Exception e) {
        LOG.warn("Error stopping the metrics system", e);
      }
    }
    allSources.clear();
    allSinks.clear();
    callbacks.clear();
    namedCallbacks.clear();
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
      mbeanName = null;
    }
    LOG.debug("{} metrics system shutdown complete.", prefix);
    return true;
  }

  @Override
  public MetricsSource getSource(String name) {
    return allSources.get(name);
  }

  @VisibleForTesting
  MetricsSourceAdapter getSourceAdapter(String name) {
    return sources.get(name);
  }

  @VisibleForTesting
  public MetricsSinkAdapter getSinkAdapter(String name) {
    return sinks.get(name);
  }

  private InitMode initMode() {
    LOG.debug("from system property: "+ System.getProperty(MS_INIT_MODE_KEY));
    LOG.debug("from environment variable: "+ System.getenv(MS_INIT_MODE_KEY));
    String m = System.getProperty(MS_INIT_MODE_KEY);
    String m2 = m == null ? System.getenv(MS_INIT_MODE_KEY) : m;
    return InitMode.valueOf(
        StringUtils.toUpperCase((m2 == null ? InitMode.NORMAL.name() : m2)));
  }
}
