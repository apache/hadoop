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
package org.apache.hadoop.tracing;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.htrace.HTraceConfiguration;
import org.htrace.SpanReceiver;
import org.htrace.Trace;
 

/**
 * This class provides functions for reading the names of SpanReceivers from
 * the Hadoop configuration, adding those SpanReceivers to the Tracer,
 * and closing those SpanReceivers when appropriate.
 * This class does nothing If no SpanReceiver is configured.
 */
@InterfaceAudience.Private
public class SpanReceiverHost implements TraceAdminProtocol {
  public static final String SPAN_RECEIVERS_CONF_KEY =
    "hadoop.trace.spanreceiver.classes";
  private static final Log LOG = LogFactory.getLog(SpanReceiverHost.class);
  private final TreeMap<Long, SpanReceiver> receivers =
      new TreeMap<Long, SpanReceiver>();
  private Configuration config;
  private boolean closed = false;
  private long highestId = 1;

  private static enum SingletonHolder {
    INSTANCE;
    Object lock = new Object();
    SpanReceiverHost host = null;
  }

  public static SpanReceiverHost getInstance(Configuration conf) {
    if (SingletonHolder.INSTANCE.host != null) {
      return SingletonHolder.INSTANCE.host;
    }
    synchronized (SingletonHolder.INSTANCE.lock) {
      if (SingletonHolder.INSTANCE.host != null) {
        return SingletonHolder.INSTANCE.host;
      }
      SpanReceiverHost host = new SpanReceiverHost();
      host.loadSpanReceivers(conf);
      SingletonHolder.INSTANCE.host = host;
      ShutdownHookManager.get().addShutdownHook(new Runnable() {
          public void run() {
            SingletonHolder.INSTANCE.host.closeReceivers();
          }
        }, 0);
      return SingletonHolder.INSTANCE.host;
    }
  }

  private static List<ConfigurationPair> EMPTY = Collections.emptyList();

  /**
   * Reads the names of classes specified in the
   * "hadoop.trace.spanreceiver.classes" property and instantiates and registers
   * them with the Tracer as SpanReceiver's.
   *
   * The nullary constructor is called during construction, but if the classes
   * specified implement the Configurable interface, setConfiguration() will be
   * called on them. This allows SpanReceivers to use values from the Hadoop
   * configuration.
   */
  public synchronized void loadSpanReceivers(Configuration conf) {
    config = new Configuration(conf);
    String[] receiverNames =
        config.getTrimmedStrings(SPAN_RECEIVERS_CONF_KEY);
    if (receiverNames == null || receiverNames.length == 0) {
      return;
    }
    for (String className : receiverNames) {
      className = className.trim();
      try {
        SpanReceiver rcvr = loadInstance(className, EMPTY);
        Trace.addReceiver(rcvr);
        receivers.put(highestId++, rcvr);
        LOG.info("SpanReceiver " + className + " was loaded successfully.");
      } catch (IOException e) {
        LOG.error("Failed to load SpanReceiver", e);
      }
    }
  }

  private synchronized SpanReceiver loadInstance(String className,
      List<ConfigurationPair> extraConfig) throws IOException {
    Class<?> implClass = null;
    SpanReceiver impl;
    try {
      implClass = Class.forName(className);
      Object o = ReflectionUtils.newInstance(implClass, config);
      impl = (SpanReceiver)o;
      impl.configure(wrapHadoopConf(config, extraConfig));
    } catch (ClassCastException e) {
      throw new IOException("Class " + className +
          " does not implement SpanReceiver.");
    } catch (ClassNotFoundException e) {
      throw new IOException("Class " + className + " cannot be found.");
    } catch (SecurityException e) {
      throw new IOException("Got SecurityException while loading " +
          "SpanReceiver " + className);
    } catch (IllegalArgumentException e) {
      throw new IOException("Got IllegalArgumentException while loading " +
          "SpanReceiver " + className, e);
    } catch (RuntimeException e) {
      throw new IOException("Got RuntimeException while loading " +
          "SpanReceiver " + className, e);
    }
    return impl;
  }

  private static HTraceConfiguration wrapHadoopConf(final Configuration conf,
          List<ConfigurationPair> extraConfig) {
    final HashMap<String, String> extraMap = new HashMap<String, String>();
    for (ConfigurationPair pair : extraConfig) {
      extraMap.put(pair.getKey(), pair.getValue());
    }
    return new HTraceConfiguration() {
      public static final String HTRACE_CONF_PREFIX = "hadoop.";

      @Override
      public String get(String key) {
        if (extraMap.containsKey(key)) {
          return extraMap.get(key);
        }
        return conf.get(HTRACE_CONF_PREFIX + key);
      }

      @Override
      public String get(String key, String defaultValue) {
        if (extraMap.containsKey(key)) {
          return extraMap.get(key);
        }
        return conf.get(HTRACE_CONF_PREFIX + key, defaultValue);
      }
    };
  }

  /**
   * Calls close() on all SpanReceivers created by this SpanReceiverHost.
   */
  public synchronized void closeReceivers() {
    if (closed) return;
    closed = true;
    for (SpanReceiver rcvr : receivers.values()) {
      try {
        rcvr.close();
      } catch (IOException e) {
        LOG.warn("Unable to close SpanReceiver correctly: " + e.getMessage(), e);
      }
    }
    receivers.clear();
  }

  public synchronized SpanReceiverInfo[] listSpanReceivers()
      throws IOException {
    SpanReceiverInfo info[] = new SpanReceiverInfo[receivers.size()];
    int i = 0;

    for(Map.Entry<Long, SpanReceiver> entry : receivers.entrySet()) {
      info[i] = new SpanReceiverInfo(entry.getKey(),
          entry.getValue().getClass().getName());
      i++;
    }
    return info;
  }

  public synchronized long addSpanReceiver(SpanReceiverInfo info)
      throws IOException {
    StringBuilder configStringBuilder = new StringBuilder();
    String prefix = "";
    for (ConfigurationPair pair : info.configPairs) {
      configStringBuilder.append(prefix).append(pair.getKey()).
          append(" = ").append(pair.getValue());
      prefix = ", ";
    }
    SpanReceiver rcvr = null;
    try {
      rcvr = loadInstance(info.getClassName(), info.configPairs);
    } catch (IOException e) {
      LOG.info("Failed to add SpanReceiver " + info.getClassName() +
          " with configuration " + configStringBuilder.toString(), e);
      throw e;
    } catch (RuntimeException e) {
      LOG.info("Failed to add SpanReceiver " + info.getClassName() +
          " with configuration " + configStringBuilder.toString(), e);
      throw e;
    }
    Trace.addReceiver(rcvr);
    long newId = highestId++;
    receivers.put(newId, rcvr);
    LOG.info("Successfully added SpanReceiver " + info.getClassName() +
        " with configuration " + configStringBuilder.toString());
    return newId;
  }

  public synchronized void removeSpanReceiver(long spanReceiverId)
      throws IOException {
    SpanReceiver rcvr = receivers.remove(spanReceiverId);
    if (rcvr == null) {
      throw new IOException("There is no span receiver with id " + spanReceiverId);
    }
    Trace.removeReceiver(rcvr);
    rcvr.close();
    LOG.info("Successfully removed SpanReceiver " + spanReceiverId +
        " with class " + rcvr.getClass().getName());
  }
}
