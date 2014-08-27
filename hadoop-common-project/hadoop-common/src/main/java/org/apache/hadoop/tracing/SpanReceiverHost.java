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
import java.util.Collection;
import java.util.HashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
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
public class SpanReceiverHost {
  public static final String SPAN_RECEIVERS_CONF_KEY = "hadoop.trace.spanreceiver.classes";
  private static final Log LOG = LogFactory.getLog(SpanReceiverHost.class);
  private Collection<SpanReceiver> receivers = new HashSet<SpanReceiver>();
  private boolean closed = false;

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
  public void loadSpanReceivers(Configuration conf) {
    Class<?> implClass = null;
    String[] receiverNames = conf.getTrimmedStrings(SPAN_RECEIVERS_CONF_KEY);
    if (receiverNames == null || receiverNames.length == 0) {
      return;
    }
    for (String className : receiverNames) {
      className = className.trim();
      try {
        implClass = Class.forName(className);
        receivers.add(loadInstance(implClass, conf));
        LOG.info("SpanReceiver " + className + " was loaded successfully.");
      } catch (ClassNotFoundException e) {
        LOG.warn("Class " + className + " cannot be found.", e);
      } catch (IOException e) {
        LOG.warn("Load SpanReceiver " + className + " failed.", e);
      }
    }
    for (SpanReceiver rcvr : receivers) {
      Trace.addReceiver(rcvr);
    }
  }

  private SpanReceiver loadInstance(Class<?> implClass, Configuration conf)
      throws IOException {
    SpanReceiver impl;
    try {
      Object o = ReflectionUtils.newInstance(implClass, conf);
      impl = (SpanReceiver)o;
      impl.configure(wrapHadoopConf(conf));
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }

    return impl;
  }

  private static HTraceConfiguration wrapHadoopConf(final Configuration conf) {
    return new HTraceConfiguration() {
      public static final String HTRACE_CONF_PREFIX = "hadoop.";

      @Override
      public String get(String key) {
        return conf.get(HTRACE_CONF_PREFIX + key);
      }

      @Override
      public String get(String key, String defaultValue) {
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
    for (SpanReceiver rcvr : receivers) {
      try {
        rcvr.close();
      } catch (IOException e) {
        LOG.warn("Unable to close SpanReceiver correctly: " + e.getMessage(), e);
      }
    }
  }
}
