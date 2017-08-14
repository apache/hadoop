/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.util.curator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that provides utility methods specific to ZK operations.
 */
@InterfaceAudience.Private
public final class ZKCuratorManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZKCuratorManager.class);

  /** Configuration for the ZooKeeper connection. */
  private final Configuration conf;

  /** Curator for ZooKeeper. */
  private CuratorFramework curator;


  public ZKCuratorManager(Configuration config) throws IOException {
    this.conf = config;
  }

  /**
   * Get the curator framework managing the ZooKeeper connection.
   * @return Curator framework.
   */
  public CuratorFramework getCurator() {
    return curator;
  }

  /**
   * Close the connection with ZooKeeper.
   */
  public void close() {
    if (curator != null) {
      curator.close();
    }
  }

  /**
   * Utility method to fetch the ZK ACLs from the configuration.
   * @throws java.io.IOException if the Zookeeper ACLs configuration file
   * cannot be read
   */
  public static List<ACL> getZKAcls(Configuration conf) throws IOException {
    // Parse authentication from configuration.
    String zkAclConf = conf.get(CommonConfigurationKeys.ZK_ACL,
        CommonConfigurationKeys.ZK_ACL_DEFAULT);
    try {
      zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
      return ZKUtil.parseACLs(zkAclConf);
    } catch (IOException | ZKUtil.BadAclFormatException e) {
      LOG.error("Couldn't read ACLs based on {}",
          CommonConfigurationKeys.ZK_ACL);
      throw e;
    }
  }

  /**
   * Utility method to fetch ZK auth info from the configuration.
   * @throws java.io.IOException if the Zookeeper ACLs configuration file
   * cannot be read
   */
  public static List<ZKUtil.ZKAuthInfo> getZKAuths(Configuration conf)
      throws IOException {
    String zkAuthConf = conf.get(CommonConfigurationKeys.ZK_AUTH);
    try {
      zkAuthConf = ZKUtil.resolveConfIndirection(zkAuthConf);
      if (zkAuthConf != null) {
        return ZKUtil.parseAuth(zkAuthConf);
      } else {
        return Collections.emptyList();
      }
    } catch (IOException | ZKUtil.BadAuthFormatException e) {
      LOG.error("Couldn't read Auth based on {}",
          CommonConfigurationKeys.ZK_AUTH);
      throw e;
    }
  }

  /**
   * Start the connection to the ZooKeeper ensemble.
   * @param conf Configuration for the connection.
   * @throws IOException If the connection cannot be started.
   */
  public void start() throws IOException {
    this.start(new ArrayList<AuthInfo>());
  }

  /**
   * Start the connection to the ZooKeeper ensemble.
   * @param conf Configuration for the connection.
   * @param authInfos List of authentication keys.
   * @throws IOException If the connection cannot be started.
   */
  public void start(List<AuthInfo> authInfos) throws IOException {

    // Connect to the ZooKeeper ensemble
    String zkHostPort = conf.get(CommonConfigurationKeys.ZK_ADDRESS);
    if (zkHostPort == null) {
      throw new IOException(
          CommonConfigurationKeys.ZK_ADDRESS + " is not configured.");
    }
    int numRetries = conf.getInt(CommonConfigurationKeys.ZK_NUM_RETRIES,
        CommonConfigurationKeys.ZK_NUM_RETRIES_DEFAULT);
    int zkSessionTimeout = conf.getInt(CommonConfigurationKeys.ZK_TIMEOUT_MS,
        CommonConfigurationKeys.ZK_TIMEOUT_MS_DEFAULT);
    int zkRetryInterval = conf.getInt(
        CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS,
        CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS_DEFAULT);
    RetryNTimes retryPolicy = new RetryNTimes(numRetries, zkRetryInterval);

    // Set up ZK auths
    List<ZKUtil.ZKAuthInfo> zkAuths = getZKAuths(conf);
    if (authInfos == null) {
      authInfos = new ArrayList<>();
    }
    for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
      authInfos.add(new AuthInfo(zkAuth.getScheme(), zkAuth.getAuth()));
    }

    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(zkHostPort)
        .sessionTimeoutMs(zkSessionTimeout)
        .retryPolicy(retryPolicy)
        .authorization(authInfos)
        .build();
    client.start();

    this.curator = client;
  }

  /**
   * Get ACLs for a ZNode.
   * @param path Path of the ZNode.
   * @return The list of ACLs.
   * @throws Exception
   */
  public List<ACL> getACL(final String path) throws Exception {
    return curator.getACL().forPath(path);
  }

  /**
   * Get the data in a ZNode.
   * @param path Path of the ZNode.
   * @param stat Output statistics of the ZNode.
   * @return The data in the ZNode.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public byte[] getData(final String path) throws Exception {
    return curator.getData().forPath(path);
  }

  /**
   * Get the data in a ZNode.
   * @param path Path of the ZNode.
   * @param stat Output statistics of the ZNode.
   * @return The data in the ZNode.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public String getSringData(final String path) throws Exception {
    byte[] bytes = getData(path);
    return new String(bytes, Charset.forName("UTF-8"));
  }

  /**
   * Set data into a ZNode.
   * @param path Path of the ZNode.
   * @param data Data to set.
   * @param version Version of the data to store.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public void setData(String path, byte[] data, int version) throws Exception {
    curator.setData().withVersion(version).forPath(path, data);
  }

  /**
   * Set data into a ZNode.
   * @param path Path of the ZNode.
   * @param data Data to set as String.
   * @param version Version of the data to store.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public void setData(String path, String data, int version) throws Exception {
    byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
    setData(path, bytes, version);
  }

  /**
   * Get children of a ZNode.
   * @param path Path of the ZNode.
   * @return The list of children.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public List<String> getChildren(final String path) throws Exception {
    return curator.getChildren().forPath(path);
  }

  /**
   * Check if a ZNode exists.
   * @param path Path of the ZNode.
   * @return If the ZNode exists.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public boolean exists(final String path) throws Exception {
    return curator.checkExists().forPath(path) != null;
  }

  /**
   * Create a ZNode.
   * @param path Path of the ZNode.
   * @return If the ZNode was created.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public boolean create(final String path) throws Exception {
    return create(path, null);
  }

  /**
   * Create a ZNode.
   * @param path Path of the ZNode.
   * @param zkAcl ACL for the node.
   * @return If the ZNode was created.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public boolean create(final String path, List<ACL> zkAcl) throws Exception {
    boolean created = false;
    if (!exists(path)) {
      curator.create()
          .withMode(CreateMode.PERSISTENT)
          .withACL(zkAcl)
          .forPath(path, null);
      created = true;
    }
    return created;
  }

  /**
   * Delete a ZNode.
   * @param path Path of the ZNode.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  public void delete(final String path) throws Exception {
    if (exists(path)) {
      curator.delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  /**
   * Get the path for a ZNode.
   * @param root Root of the ZNode.
   * @param nodeName Name of the ZNode.
   * @return Path for the ZNode.
   */
  public static String getNodePath(String root, String nodeName) {
    return root + "/" + nodeName;
  }
}