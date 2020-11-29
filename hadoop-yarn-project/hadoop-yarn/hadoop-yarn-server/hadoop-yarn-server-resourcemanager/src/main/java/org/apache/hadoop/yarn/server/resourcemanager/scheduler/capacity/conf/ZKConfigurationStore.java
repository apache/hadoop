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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A Zookeeper-based implementation of {@link YarnConfigurationStore}.
 */
public class ZKConfigurationStore extends YarnConfigurationStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(ZKConfigurationStore.class);

  private long maxLogs;

  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(0, 1);
  private Configuration conf;

  private static final String ZK_VERSION_PATH = "VERSION";
  private static final String LOGS_PATH = "LOGS";
  private static final String CONF_STORE_PATH = "CONF_STORE";
  private static final String FENCING_PATH = "FENCING";
  private static final String CONF_VERSION_PATH = "CONF_VERSION";

  private String zkVersionPath;
  private String logsPath;
  private String confStorePath;
  private String fencingNodePath;
  private String confVersionPath;

  private ZKCuratorManager zkManager;
  private List<ACL> zkAcl;

  @Override
  public void initialize(Configuration config, Configuration schedConf,
      RMContext rmContext) throws Exception {
    this.conf = config;

    String znodeParentPath = conf.get(
        YarnConfiguration.RM_SCHEDCONF_STORE_ZK_PARENT_PATH,
        YarnConfiguration.DEFAULT_RM_SCHEDCONF_STORE_ZK_PARENT_PATH);

    this.maxLogs = conf.getLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS,
        YarnConfiguration.DEFAULT_RM_SCHEDCONF_ZK_MAX_LOGS);
    this.zkManager =
        rmContext.getResourceManager().createAndStartZKManager(conf);
    this.zkAcl = ZKCuratorManager.getZKAcls(conf);

    this.zkVersionPath = getNodePath(znodeParentPath, ZK_VERSION_PATH);
    this.logsPath = getNodePath(znodeParentPath, LOGS_PATH);
    this.confStorePath = getNodePath(znodeParentPath, CONF_STORE_PATH);
    this.fencingNodePath = getNodePath(znodeParentPath, FENCING_PATH);
    this.confVersionPath = getNodePath(znodeParentPath, CONF_VERSION_PATH);

    zkManager.createRootDirRecursively(znodeParentPath, zkAcl);
    zkManager.delete(fencingNodePath);

    if (createNewZkPath(logsPath)) {
      setZkData(logsPath, new LinkedList<LogMutation>());
    }

    if (createNewZkPath(confVersionPath)) {
      setZkData(confVersionPath, String.valueOf(0));
    }

    if (createNewZkPath(confStorePath)) {
      HashMap<String, String> mapSchedConf = new HashMap<>();
      for (Map.Entry<String, String> entry : schedConf) {
        mapSchedConf.put(entry.getKey(), entry.getValue());
      }
      setZkData(confStorePath, mapSchedConf);
      long configVersion = getConfigVersion() + 1L;
      setZkData(confVersionPath, String.valueOf(configVersion));
    }
  }

  @VisibleForTesting
  @Override
  protected LinkedList<LogMutation> getLogs() throws Exception {
    return unsafeCast(deserializeObject(getZkData(logsPath)));
  }

  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public Version getConfStoreVersion() throws Exception {
    if (zkManager.exists(zkVersionPath)) {
      byte[] data = getZkData(zkVersionPath);
      return new VersionPBImpl(YarnServerCommonProtos.VersionProto
          .parseFrom(data));
    }

    return null;
  }

  @Override
  public void format() throws Exception {
    zkManager.delete(confStorePath);
  }

  @Override
  public synchronized void storeVersion() throws Exception {
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();

    if (zkManager.exists(zkVersionPath)) {
      safeSetZkData(zkVersionPath, data);
    } else {
      safeCreateZkData(zkVersionPath, data);
    }
  }

  @Override
  public void logMutation(LogMutation logMutation) throws Exception {
    if (maxLogs > 0) {
      byte[] storedLogs = getZkData(logsPath);
      LinkedList<LogMutation> logs = new LinkedList<>();
      if (storedLogs != null) {
        logs = unsafeCast(deserializeObject(storedLogs));
      }
      logs.add(logMutation);
      if (logs.size() > maxLogs) {
        logs.remove(logs.removeFirst());
      }
      safeSetZkData(logsPath, logs);
    }
  }

  @Override
  public void confirmMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception {
    if (isValid) {
      Configuration storedConfigs = retrieve();
      Map<String, String> mapConf = new HashMap<>();
      for (Map.Entry<String, String> storedConf : storedConfigs) {
        mapConf.put(storedConf.getKey(), storedConf.getValue());
      }
      for (Map.Entry<String, String> confChange :
          pendingMutation.getUpdates().entrySet()) {
        if (confChange.getValue() == null || confChange.getValue().isEmpty()) {
          mapConf.remove(confChange.getKey());
        } else {
          mapConf.put(confChange.getKey(), confChange.getValue());
        }
      }
      safeSetZkData(confStorePath, mapConf);
      long configVersion = getConfigVersion() + 1L;
      setZkData(confVersionPath, String.valueOf(configVersion));

    }
  }

  @Override
  public synchronized Configuration retrieve() {
    byte[] serializedSchedConf;
    try {
      serializedSchedConf = getZkData(confStorePath);
    } catch (Exception e) {
      LOG.error("Failed to retrieve configuration from zookeeper store", e);
      return null;
    }
    try {
      Map<String, String> map =
          unsafeCast(deserializeObject(serializedSchedConf));
      Configuration c = new Configuration(false);
      for (Map.Entry<String, String> e : map.entrySet()) {
        c.set(e.getKey(), e.getValue());
      }
      return c;
    } catch (Exception e) {
      LOG.error("Exception while deserializing scheduler configuration " +
          "from store", e);
    }
    return null;
  }

  @Override
  public long getConfigVersion() throws Exception {
    String version = zkManager.getStringData(confVersionPath);
    if (version == null) {
      throw new IllegalStateException("Config version can not be properly " +
          "serialized. Check Zookeeper config version path to locate " +
          "the error!");
    }

    return Long.parseLong(version);
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    return null; // unimplemented
  }

  /**
   * Creates a new path in Zookeeper only, if it does not already exist.
   *
   * @param path Value of the Zookeeper path
   * @return <code>true</code>if the creation executed; <code>false</code>
   * otherwise.
   * @throws Exception
   */
  private boolean createNewZkPath(String path) throws Exception {
    if (!zkManager.exists(path)) {
      zkManager.create(path);
      return true;
    } else {
      return false;
    }
  }

  @VisibleForTesting
  protected byte[] getZkData(String path) throws Exception {
    return zkManager.getData(path);
  }

  @VisibleForTesting
  protected void setZkData(String path, byte[] data) throws Exception {
    zkManager.setData(path, data, -1);
  }

  private void setZkData(String path, Object data) throws Exception {
    setZkData(path, serializeObject(data));
  }

  private void setZkData(String path, String data) throws Exception {
    zkManager.setData(path, data, -1);
  }

  private void safeSetZkData(String path, byte[] data) throws Exception {
    zkManager.safeSetData(path, data, -1, zkAcl, fencingNodePath);
  }

  private void safeSetZkData(String path, Object data) throws Exception {
    safeSetZkData(path, serializeObject(data));
  }

  @VisibleForTesting
  protected void safeCreateZkData(String path, byte[] data) throws Exception {
    zkManager.safeCreate(path, data, zkAcl, CreateMode.PERSISTENT,
        zkAcl, fencingNodePath);
  }

  private static String getNodePath(String root, String nodeName) {
    return ZKCuratorManager.getNodePath(root, nodeName);
  }

  private static byte[] serializeObject(Object o) throws Exception {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);) {
      oos.writeObject(o);
      oos.flush();
      baos.flush();
      return baos.toByteArray();
    }
  }

  private static Object deserializeObject(byte[] bytes) throws Exception {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);) {
      return ois.readObject();
    }
  }

  /**
   * Casts an object of type Object to type T. It is essential to emphasize,
   * that it is an unsafe operation.
   *
   * @param o Object to be cast from
   * @param <T> Type to cast to
   * @return casted object of type T
   * @throws ClassCastException
   */
  @SuppressWarnings("unchecked")
  private static <T> T unsafeCast(Object o) throws ClassCastException {
    return (T)o;
  }

  @Override
  public void close() throws IOException {
    if (zkManager  != null) {
      zkManager.close();
    }
  }
}
