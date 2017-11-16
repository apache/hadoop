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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

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

  public static final Log LOG =
      LogFactory.getLog(ZKConfigurationStore.class);

  private long maxLogs;

  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(0, 1);
  private Configuration conf;
  private LogMutation pendingMutation;

  private String znodeParentPath;

  private static final String ZK_VERSION_PATH = "VERSION";
  private static final String LOGS_PATH = "LOGS";
  private static final String CONF_STORE_PATH = "CONF_STORE";
  private static final String FENCING_PATH = "FENCING";

  private String zkVersionPath;
  private String logsPath;
  private String confStorePath;
  private String fencingNodePath;

  @VisibleForTesting
  protected ZKCuratorManager zkManager;
  private List<ACL> zkAcl;

  @Override
  public void initialize(Configuration config, Configuration schedConf,
      RMContext rmContext) throws Exception {
    this.conf = config;
    this.maxLogs = conf.getLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS,
        YarnConfiguration.DEFAULT_RM_SCHEDCONF_ZK_MAX_LOGS);
    this.znodeParentPath =
        conf.get(YarnConfiguration.RM_SCHEDCONF_STORE_ZK_PARENT_PATH,
            YarnConfiguration.DEFAULT_RM_SCHEDCONF_STORE_ZK_PARENT_PATH);
    this.zkManager =
        rmContext.getResourceManager().createAndStartZKManager(conf);
    this.zkAcl = ZKCuratorManager.getZKAcls(conf);

    this.zkVersionPath = getNodePath(znodeParentPath, ZK_VERSION_PATH);
    this.logsPath = getNodePath(znodeParentPath, LOGS_PATH);
    this.confStorePath = getNodePath(znodeParentPath, CONF_STORE_PATH);
    this.fencingNodePath = getNodePath(znodeParentPath, FENCING_PATH);

    zkManager.createRootDirRecursively(znodeParentPath);
    zkManager.delete(fencingNodePath);

    if (!zkManager.exists(logsPath)) {
      zkManager.create(logsPath);
      zkManager.setData(logsPath,
          serializeObject(new LinkedList<LogMutation>()), -1);
    }

    if (!zkManager.exists(confStorePath)) {
      zkManager.create(confStorePath);
      HashMap<String, String> mapSchedConf = new HashMap<>();
      for (Map.Entry<String, String> entry : schedConf) {
        mapSchedConf.put(entry.getKey(), entry.getValue());
      }
      zkManager.setData(confStorePath, serializeObject(mapSchedConf), -1);
    }
  }

  @VisibleForTesting
  protected LinkedList<LogMutation> getLogs() throws Exception {
    return (LinkedList<LogMutation>)
        deserializeObject(zkManager.getData(logsPath));
  }

  // TODO: following version-related code is taken from ZKRMStateStore
  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public Version getConfStoreVersion() throws Exception {
    if (zkManager.exists(zkVersionPath)) {
      byte[] data = zkManager.getData(zkVersionPath);
      return new VersionPBImpl(YarnServerCommonProtos.VersionProto
          .parseFrom(data));
    }

    return null;
  }

  @Override
  public synchronized void storeVersion() throws Exception {
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();

    if (zkManager.exists(zkVersionPath)) {
      zkManager.safeSetData(zkVersionPath, data, -1, zkAcl, fencingNodePath);
    } else {
      zkManager.safeCreate(zkVersionPath, data, zkAcl, CreateMode.PERSISTENT,
          zkAcl, fencingNodePath);
    }
  }

  @Override
  public void logMutation(LogMutation logMutation) throws Exception {
    byte[] storedLogs = zkManager.getData(logsPath);
    LinkedList<LogMutation> logs = new LinkedList<>();
    if (storedLogs != null) {
      logs = (LinkedList<LogMutation>) deserializeObject(storedLogs);
    }
    logs.add(logMutation);
    if (logs.size() > maxLogs) {
      logs.remove(logs.removeFirst());
    }
    zkManager.safeSetData(logsPath, serializeObject(logs), -1, zkAcl,
        fencingNodePath);
    pendingMutation = logMutation;
  }

  @Override
  public void confirmMutation(boolean isValid)
      throws Exception {
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
      zkManager.safeSetData(confStorePath, serializeObject(mapConf), -1,
          zkAcl, fencingNodePath);
    }
    pendingMutation = null;
  }

  @Override
  public synchronized Configuration retrieve() {
    byte[] serializedSchedConf;
    try {
      serializedSchedConf = zkManager.getData(confStorePath);
    } catch (Exception e) {
      LOG.error("Failed to retrieve configuration from zookeeper store", e);
      return null;
    }
    try {
      Map<String, String> map =
          (HashMap<String, String>) deserializeObject(serializedSchedConf);
      Configuration c = new Configuration();
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
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    return null; // unimplemented
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
}
