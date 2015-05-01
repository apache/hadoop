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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.SpanReceiverBuilder;
import org.apache.htrace.Trace;

/**
 * This class provides functions for reading the names of SpanReceivers from
 * the Hadoop configuration, adding those SpanReceivers to the Tracer,
 * and closing those SpanReceivers when appropriate.
 * This class does nothing If no SpanReceiver is configured.
 */
@InterfaceAudience.Private
public class SpanReceiverHost implements TraceAdminProtocol {
  public static final String SPAN_RECEIVERS_CONF_SUFFIX =
    "spanreceiver.classes";
  private static final Log LOG = LogFactory.getLog(SpanReceiverHost.class);
  private static final HashMap<String, SpanReceiverHost> hosts =
      new HashMap<String, SpanReceiverHost>(1);
  private final TreeMap<Long, SpanReceiver> receivers =
      new TreeMap<Long, SpanReceiver>();
  private final String confPrefix;
  private Configuration config;
  private boolean closed = false;
  private long highestId = 1;

  private final static String LOCAL_FILE_SPAN_RECEIVER_PATH_SUFFIX =
      "local-file-span-receiver.path";

  public static SpanReceiverHost get(Configuration conf, String confPrefix) {
    synchronized (SpanReceiverHost.class) {
      SpanReceiverHost host = hosts.get(confPrefix);
      if (host != null) {
        return host;
      }
      final SpanReceiverHost newHost = new SpanReceiverHost(confPrefix);
      newHost.loadSpanReceivers(conf);
      ShutdownHookManager.get().addShutdownHook(new Runnable() {
          public void run() {
            newHost.closeReceivers();
          }
        }, 0);
      hosts.put(confPrefix, newHost);
      return newHost;
    }
  }

  private static List<ConfigurationPair> EMPTY = Collections.emptyList();

  private static String getUniqueLocalTraceFileName() {
    String tmp = System.getProperty("java.io.tmpdir", "/tmp");
    String nonce = null;
    BufferedReader reader = null;
    try {
      // On Linux we can get a unique local file name by reading the process id
      // out of /proc/self/stat.  (There isn't any portable way to get the
      // process ID from Java.)
      reader = new BufferedReader(
          new InputStreamReader(new FileInputStream("/proc/self/stat"),
                                Charsets.UTF_8));
      String line = reader.readLine();
      if (line == null) {
        throw new EOFException();
      }
      nonce = line.split(" ")[0];
    } catch (IOException e) {
    } finally {
      IOUtils.cleanup(LOG, reader);
    }
    if (nonce == null) {
      // If we can't use the process ID, use a random nonce.
      nonce = UUID.randomUUID().toString();
    }
    return new File(tmp, nonce).getAbsolutePath();
  }

  private SpanReceiverHost(String confPrefix) {
    this.confPrefix = confPrefix;
  }

  /**
   * Reads the names of classes specified in the
   * "hadoop.htrace.spanreceiver.classes" property and instantiates and registers
   * them with the Tracer as SpanReceiver's.
   *
   * The nullary constructor is called during construction, but if the classes
   * specified implement the Configurable interface, setConfiguration() will be
   * called on them. This allows SpanReceivers to use values from the Hadoop
   * configuration.
   */
  public synchronized void loadSpanReceivers(Configuration conf) {
    config = new Configuration(conf);
    String receiverKey = confPrefix + SPAN_RECEIVERS_CONF_SUFFIX;
    String[] receiverNames = config.getTrimmedStrings(receiverKey);
    if (receiverNames == null || receiverNames.length == 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No span receiver names found in " + receiverKey + ".");
      }
      return;
    }
    // It's convenient to have each daemon log to a random trace file when
    // testing.
    String pathKey = confPrefix + LOCAL_FILE_SPAN_RECEIVER_PATH_SUFFIX;
    if (config.get(pathKey) == null) {
      String uniqueFile = getUniqueLocalTraceFileName();
      config.set(pathKey, uniqueFile);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Set " + pathKey + " to " + uniqueFile);
      }
    }
    for (String className : receiverNames) {
      try {
        SpanReceiver rcvr = loadInstance(className, EMPTY);
        Trace.addReceiver(rcvr);
        receivers.put(highestId++, rcvr);
        LOG.info("Loaded SpanReceiver " + className + " successfully.");
      } catch (IOException e) {
        LOG.error("Failed to load SpanReceiver", e);
      }
    }
  }

  private synchronized SpanReceiver loadInstance(String className,
      List<ConfigurationPair> extraConfig) throws IOException {
    SpanReceiverBuilder builder =
        new SpanReceiverBuilder(TraceUtils.
            wrapHadoopConf(confPrefix, config, extraConfig));
    SpanReceiver rcvr = builder.spanReceiverClass(className.trim()).build();
    if (rcvr == null) {
      throw new IOException("Failed to load SpanReceiver " + className);
    }
    return rcvr;
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
