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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import javax.management.ObjectName;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.util.MathUtils;

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import static org.apache.hadoop.metrics2.impl.MetricsConfig.*;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.metrics2.util.Contracts;

/**
 * A base class for metrics system singletons
 */
public class MetricsSystemImpl implements MetricsSystem {

  private static final Log LOG = LogFactory.getLog(MetricsSystemImpl.class);
  static final String MS_CONTEXT = "metricssystem";
  static final String NUM_SOURCES_KEY = "num_sources";
  static final String NUM_SOURCES_DESC = "Number of metrics sources";
  static final String NUM_SINKS_KEY = "num_sinks";
  static final String NUM_SINKS_DESC = "Number of metrics sinks";
  static final String MS_NAME = "MetricsSystem";
  static final String MS_STATS_NAME = MS_NAME +",sub=Stats";
  static final String MS_STATS_DESC = "Metrics system metrics";
  static final String MS_CONTROL_NAME = MS_NAME +",sub=Control";

  private final Map<String, MetricsSourceAdapter> sources;
  private final Map<String, MetricsSinkAdapter> sinks;
  private final List<Callback> callbacks;
  private final MetricsBuilderImpl metricsBuilder;
  private final MetricMutableStat snapshotStat =
      new MetricMutableStat("snapshot", "snapshot stats", "ops", "time", true);
  private final MetricMutableStat publishStat =
      new MetricMutableStat("publish", "publishing stats", "ops", "time", true);
  private final MetricMutableCounterLong dropStat =
      new MetricMutableCounterLong("dropped_pub_all",
        "number of dropped updates by all sinks", 0L);
  private final List<MetricsTag> injectedTags;

  // Things that are changed by init()/start()/stop()
  private String prefix;
  private MetricsFilter sourceFilter;
  private MetricsConfig config;
  private Map<String, MetricsConfig> sourceConfigs, sinkConfigs;
  private boolean monitoring = false;
  private Timer timer;
  private int period; // seconds
  private long logicalTime; // number of timer invocations * period
  private ObjectName mbeanName;
  private boolean publishSelfMetrics = true;
  private MetricsSourceAdapter sysSource;

  /**
   * Construct the metrics system
   * @param prefix  for the system
   */
  public MetricsSystemImpl(String prefix) {
    this.prefix = prefix;
    sources = new LinkedHashMap<String, MetricsSourceAdapter>();
    sinks = new LinkedHashMap<String, MetricsSinkAdapter>();
    sourceConfigs = new HashMap<String, MetricsConfig>();
    sinkConfigs = new HashMap<String, MetricsConfig>();
    callbacks = new ArrayList<Callback>();
    injectedTags = new ArrayList<MetricsTag>();
    metricsBuilder = new MetricsBuilderImpl();
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
   */
  public synchronized void init(String prefix) {
    if (monitoring) {
      LOG.warn(this.prefix +" metrics system already initialized!");
      return;
    }
    this.prefix = Contracts.checkNotNull(prefix, "prefix");
    try { start(); }
    catch (MetricsConfigException e) {
      // Usually because hadoop-metrics2.properties is missing
      // We can always start the metrics system later via JMX.
      LOG.warn("Metrics system not started: "+ e.getMessage());
      LOG.debug("Stacktrace: ", e);
    }
    initSystemMBean();
  }

  @Override
  public synchronized void start() {
    Contracts.checkNotNull(prefix, "prefix");
    if (monitoring) {
      LOG.warn(prefix +" metrics system already started!",
               new MetricsException("Illegal start"));
      return;
    }
    for (Callback cb : callbacks) cb.preStart();
    configure(prefix);
    startTimer();
    monitoring = true;
    LOG.info(prefix +" metrics system started");
    for (Callback cb : callbacks) cb.postStart();
  }

  @Override
  public synchronized void stop() {
    if (!monitoring) {
      LOG.warn(prefix +" metrics system not yet started!",
               new MetricsException("Illegal stop"));
      return;
    }
    for (Callback cb : callbacks) cb.preStop();
    LOG.info("Stopping "+ prefix +" metrics system...");
    stopTimer();
    stopSources();
    stopSinks();
    clearConfigs();
    monitoring = false;
    LOG.info(prefix +" metrics system stopped.");
    for (Callback cb : callbacks) cb.postStop();
  }

  @Override
  public synchronized <T extends MetricsSource>
  T register(final String name, final String desc, final T source) {
    if (monitoring) {
      registerSource(name, desc, source);
    }
    // We want to re-register the source to pick up new config when the
    // metrics system restarts.
    register(new AbstractCallback() {

      @Override public void postStart() {
        registerSource(name, desc, source);
      }

    });
    return source;
  }

  synchronized void registerSource(String name, String desc,
                                   MetricsSource source) {
    Contracts.checkNotNull(config, "config");
    MetricsSourceAdapter sa = sources.get(name);
    if (sa != null) {
      LOG.warn("Source name "+name+" already exists!");
      return;
    }
    MetricsConfig conf = sourceConfigs.get(name);
    sa = conf != null
        ? new MetricsSourceAdapter(prefix, name, desc, source,
                                   injectedTags, period, conf)
        : new MetricsSourceAdapter(prefix, name, desc, source,
          injectedTags, period, config.subset(SOURCE_KEY));
    sources.put(name, sa);
    sa.start();
    LOG.debug("Registered source "+ name);
  }

  @Override
  public synchronized <T extends MetricsSink>
  T register(final String name, final String description, final T sink) {
    if (config != null) {
      registerSink(name, description, sink);
    }
    // We want to re-register the sink to pick up new config
    // when the metrics system restarts.
    register(new AbstractCallback() {

      @Override public void postStart() {
        registerSink(name, description, sink);
      }

    });
    return sink;
  }

  synchronized void registerSink(String name, String desc, MetricsSink sink) {
    Contracts.checkNotNull(config, "config");
    MetricsSinkAdapter sa = sinks.get(name);
    if (sa != null) {
      LOG.warn("Sink name "+name+" already exists!");
      return;
    }
    MetricsConfig conf = sinkConfigs.get(name);
    sa = conf != null
        ? newSink(name, desc, sink, conf)
        : newSink(name, desc, sink, config.subset(SINK_KEY));
    sinks.put(name, sa);
    sa.start();
    LOG.debug("Registered sink "+ name);
  }

  @Override
  public synchronized void register(final Callback callback) {
    callbacks.add((Callback) Proxy.newProxyInstance(
        callback.getClass().getClassLoader(), new Class<?>[] { Callback.class },
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
            try {
              return method.invoke(callback, args);
            }
            catch (Exception e) {
              LOG.warn("Caught exception in callback "+ method.getName(), e);
            }
            return null;
          }
        }));
  }

  @Override
  public synchronized void refreshMBeans() {
    for (MetricsSourceAdapter sa : sources.values()) {
      sa.refreshMBean();
    }
  }

  @Override
  public synchronized String currentConfig() {
    PropertiesConfiguration saver = new PropertiesConfiguration();
    StringWriter writer = new StringWriter();
    saver.copy(config);
    try { saver.save(writer); }
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
    long millis = period * 1000;
    timer = new Timer("Timer for '"+ prefix +"' metrics system", true);
    timer.scheduleAtFixedRate(new TimerTask() {
          public void run() {
            try {
              onTimerEvent();
            }
            catch (Exception e) {
              LOG.warn(e);
            }
          }
        }, millis, millis);
    LOG.info("Scheduled snapshot period at "+ period +" second(s).");
  }

  synchronized void onTimerEvent() {
    logicalTime += period;
    if (sinks.size() > 0) {
      publishMetrics(snapshotMetrics());
    }
  }

  /**
   * snapshot all the sources for a snapshot of metrics/tags
   * @return  the metrics buffer containing the snapshot
   */
  synchronized MetricsBuffer snapshotMetrics() {
    metricsBuilder.clear();
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
    long startTime = System.currentTimeMillis();
    bufferBuilder.add(sa.name(), sa.getMetrics(metricsBuilder, false));
    metricsBuilder.clear();
    snapshotStat.add(System.currentTimeMillis() - startTime);
    LOG.debug("Snapshotted source "+ sa.name());
  }

  /**
   * Publish a metrics snapshot to all the sinks
   * @param buffer  the metrics snapshot to publish
   */
  synchronized void publishMetrics(MetricsBuffer buffer) {
    int dropped = 0;
    for (MetricsSinkAdapter sa : sinks.values()) {
      long startTime = System.currentTimeMillis();
      dropped += sa.putMetrics(buffer, logicalTime) ? 0 : 1;
      publishStat.add(System.currentTimeMillis() - startTime);
    }
    dropStat.incr(dropped);
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
      LOG.info("Stopping metrics source "+ entry.getKey() +"("+
                sa.source().getClass().getName() +")");
      sa.stop();
    }
    sysSource.stop();
    sources.clear();
  }

  private synchronized void stopSinks() {
    for (Entry<String, MetricsSinkAdapter> entry : sinks.entrySet()) {
      MetricsSinkAdapter sa = entry.getValue();
      LOG.info("Stopping metrics sink "+ entry.getKey() +"("+
               sa.sink().getClass().getName() +")");
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
    injectedTags.add(new MetricsTag("hostName", "Local hostname",
                                    getHostname()));
  }

  private synchronized void configureSinks() {
    sinkConfigs = config.getInstanceConfigs(SINK_KEY);
    int confPeriod = 0;
    for (Entry<String, MetricsConfig> entry : sinkConfigs.entrySet()) {
      MetricsConfig conf = entry.getValue();
      int sinkPeriod = conf.getInt(PERIOD_KEY, PERIOD_DEFAULT);
      confPeriod = confPeriod == 0 ? sinkPeriod
                                   : MathUtils.gcd(confPeriod, sinkPeriod);
      String sinkName = entry.getKey();
      LOG.debug("sink "+ sinkName +" config:\n"+ conf);
      try {
        MetricsSinkAdapter sa = newSink(sinkName,
            conf.getString(DESC_KEY, sinkName), conf);
        // we allow config of later registered sinks
        if (sa != null) {
          sa.start();
          sinks.put(sinkName, sa);
        }
      }
      catch (Exception e) {
        LOG.warn("Error creating "+ sinkName, e);
      }
    }
    period = confPeriod > 0 ? confPeriod
                            : config.getInt(PERIOD_KEY, PERIOD_DEFAULT);
  }

  static MetricsSinkAdapter newSink(String name, String desc, MetricsSink sink,
                                    MetricsConfig conf) {
    return new MetricsSinkAdapter(name, desc, sink, conf.getString(CONTEXT_KEY),
        conf.getFilter(SOURCE_FILTER_KEY),
        conf.getFilter(RECORD_FILTER_KEY),
        conf.getFilter(METRIC_FILTER_KEY),
        conf.getInt(PERIOD_KEY, PERIOD_DEFAULT),
        conf.getInt(QUEUE_CAPACITY_KEY, QUEUE_CAPACITY_DEFAULT),
        conf.getInt(RETRY_DELAY_KEY, RETRY_DELAY_DEFAULT),
        conf.getFloat(RETRY_BACKOFF_KEY, RETRY_BACKOFF_DEFAULT),
        conf.getInt(RETRY_COUNT_KEY, RETRY_COUNT_DEFAULT));
  }

  static MetricsSinkAdapter newSink(String name, String desc,
                                    MetricsConfig conf) {
    MetricsSink sink = conf.getPlugin("");
    if (sink == null) return null;
    return newSink(name, desc, sink, conf);
  }

  private void configureSources() {
    sourceFilter = config.getFilter(PREFIX_DEFAULT + SOURCE_FILTER_KEY);
    Map<String, MetricsConfig> confs = config.getInstanceConfigs(SOURCE_KEY);
    for (Entry<String, MetricsConfig> entry : confs.entrySet()) {
     sourceConfigs.put(entry.getKey(), entry.getValue());
    }
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
    }
    catch (Exception e) {
      LOG.error("Error getting localhost name. Using 'localhost'...", e);
    }
    return "localhost";
  }

  private void registerSystemSource() {
    sysSource = new MetricsSourceAdapter(prefix, MS_STATS_NAME, MS_STATS_DESC,
        new MetricsSource() {
      @Override
      public void getMetrics(MetricsBuilder builder, boolean all) {
        int numSources, numSinks;
        synchronized(MetricsSystemImpl.this) {
          numSources = sources.size();
          numSinks = sinks.size();
        }
        MetricsRecordBuilder rb = builder.addRecord(MS_NAME)
            .setContext(MS_CONTEXT)
            .addGauge(NUM_SOURCES_KEY, NUM_SOURCES_DESC, numSources)
            .addGauge(NUM_SINKS_KEY, NUM_SINKS_DESC, numSinks);
        synchronized(MetricsSystemImpl.this) {
          for (MetricsSinkAdapter sa : sinks.values()) {
            sa.snapshot(rb, all);
          }
        }
        snapshotStat.snapshot(rb, all);
        publishStat.snapshot(rb, all);
        dropStat.snapshot(rb, all);
      }
    }, injectedTags, null, null, period);
    sysSource.start();
  }

  private void initSystemMBean() {
    Contracts.checkNotNull(prefix, "prefix should not be null here!");
    mbeanName = MBeans.register(prefix, MS_CONTROL_NAME, this);
  }

  @Override
  public synchronized void shutdown() {
    if (monitoring) {
      try { stop(); }
      catch (Exception e) {
        LOG.warn("Error stopping the metrics system", e);
      }
    }
    MBeans.unregister(mbeanName);
  }

}
