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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.util.VersionInfo;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for managing HDFS federation.
 */
public final class FederationUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationUtil.class);

  private FederationUtil() {
    // Utility Class
  }

  /**
   * Get a JMX data from a web endpoint.
   *
   * @param beanQuery JMX bean.
   * @param webAddress Web address of the JMX endpoint.
   * @param connectionFactory to open http/https connection.
   * @param scheme to use for URL connection.
   * @return JSON with the JMX data
   */
  public static JSONArray getJmx(String beanQuery, String webAddress,
      URLConnectionFactory connectionFactory, String scheme) {
    JSONArray ret = null;
    BufferedReader reader = null;
    try {
      String host = webAddress;
      int port = -1;
      if (webAddress.indexOf(":") > 0) {
        String[] webAddressSplit = webAddress.split(":");
        host = webAddressSplit[0];
        port = Integer.parseInt(webAddressSplit[1]);
      }
      URL jmxURL = new URL(scheme, host, port, "/jmx?qry=" + beanQuery);
      LOG.debug("JMX URL: {}", jmxURL);
      // Create a URL connection
      URLConnection conn = connectionFactory.openConnection(
          jmxURL, UserGroupInformation.isSecurityEnabled());
      conn.setConnectTimeout(5 * 1000);
      conn.setReadTimeout(5 * 1000);
      InputStream in = conn.getInputStream();
      InputStreamReader isr = new InputStreamReader(in, "UTF-8");
      reader = new BufferedReader(isr);

      StringBuilder sb = new StringBuilder();
      String line = null;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      String jmxOutput = sb.toString();

      // Parse JSON
      JSONObject json = new JSONObject(jmxOutput);
      ret = json.getJSONArray("beans");
    } catch (IOException e) {
      LOG.error("Cannot read JMX bean {} from server {}",
          beanQuery, webAddress, e);
    } catch (JSONException e) {
      // We shouldn't need more details if the JSON parsing fails.
      LOG.error("Cannot parse JMX output for {} from server {}: {}",
          beanQuery, webAddress, e.getMessage());
    } catch (Exception e) {
      LOG.error("Cannot parse JMX output for {} from server {}",
          beanQuery, webAddress, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Problem closing {}", webAddress, e);
        }
      }
    }
    return ret;
  }

  /**
   * Fetch the Hadoop version string for this jar.
   *
   * @return Hadoop version string, e.g., 3.0.1.
   */
  public static String getVersion() {
    return VersionInfo.getVersion();
  }

  /**
   * Fetch the build/compile information for this jar.
   *
   * @return String Compilation info.
   */
  public static String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() + " from "
        + VersionInfo.getBranch();
  }

  /**
   * Create an instance of an interface with a constructor using a context.
   *
   * @param conf Configuration for the class names.
   * @param context Context object to pass to the instance.
   * @param contextClass Type of the context passed to the constructor.
   * @param clazz Class of the object to return.
   * @return New instance of the specified class that implements the desired
   *         interface and a single parameter constructor containing a
   *         StateStore reference.
   */
  private static <T, R> T newInstance(final Configuration conf,
      final R context, final Class<R> contextClass, final Class<T> clazz) {
    try {
      if (contextClass == null) {
        if (conf == null) {
          // Default constructor if no context
          Constructor<T> constructor = clazz.getConstructor();
          return constructor.newInstance();
        } else {
          // Constructor with configuration but no context
          Constructor<T> constructor = clazz.getConstructor(
              Configuration.class);
          return constructor.newInstance(conf);
        }
      } else {
        // Constructor with context
        Constructor<T> constructor = clazz.getConstructor(
            Configuration.class, contextClass);
        return constructor.newInstance(conf, context);
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: {}", clazz.getSimpleName(), e);
      return null;
    }
  }

  /**
   * Creates an instance of a FileSubclusterResolver from the configuration.
   *
   * @param conf Configuration that defines the file resolver class.
   * @param router Router service.
   * @return New file subcluster resolver.
   */
  public static FileSubclusterResolver newFileSubclusterResolver(
      Configuration conf, Router router) {
    Class<? extends FileSubclusterResolver> clazz = conf.getClass(
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS_DEFAULT,
        FileSubclusterResolver.class);
    return newInstance(conf, router, Router.class, clazz);
  }

  /**
   * Creates an instance of an ActiveNamenodeResolver from the configuration.
   *
   * @param conf Configuration that defines the namenode resolver class.
   * @param stateStore State store passed to class constructor.
   * @return New active namenode resolver.
   */
  public static ActiveNamenodeResolver newActiveNamenodeResolver(
      Configuration conf, StateStoreService stateStore) {
    Class<? extends ActiveNamenodeResolver> clazz = conf.getClass(
        RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS_DEFAULT,
        ActiveNamenodeResolver.class);
    return newInstance(conf, stateStore, StateStoreService.class, clazz);
  }

  /**
   * Creates an instance of DelegationTokenSecretManager from the
   * configuration.
   *
   * @param conf Configuration that defines the token manager class.
   * @return New delegation token secret manager.
   */
  public static AbstractDelegationTokenSecretManager<DelegationTokenIdentifier>
      newSecretManager(Configuration conf) {
    Class<? extends AbstractDelegationTokenSecretManager> clazz =
        conf.getClass(
        RBFConfigKeys.DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS,
        RBFConfigKeys.DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS_DEFAULT,
        AbstractDelegationTokenSecretManager.class);
    return newInstance(conf, null, null, clazz);
  }

  /**
   * Add the number of children for an existing HdfsFileStatus object.
   * @param dirStatus HdfsfileStatus object.
   * @param children number of children to be added.
   * @return HdfsFileStatus with the number of children specified.
   */
  public static HdfsFileStatus updateMountPointStatus(HdfsFileStatus dirStatus,
      int children) {
    // Get flags to set in new FileStatus.
    EnumSet<HdfsFileStatus.Flags> flags =
        DFSUtil.getFlags(dirStatus.isEncrypted(), dirStatus.isErasureCoded(),
            dirStatus.isSnapshotEnabled(), dirStatus.hasAcl());
    EnumSet.noneOf(HdfsFileStatus.Flags.class);
    return new HdfsFileStatus.Builder().atime(dirStatus.getAccessTime())
        .blocksize(dirStatus.getBlockSize()).children(children)
        .ecPolicy(dirStatus.getErasureCodingPolicy())
        .feInfo(dirStatus.getFileEncryptionInfo()).fileId(dirStatus.getFileId())
        .group(dirStatus.getGroup()).isdir(dirStatus.isDir())
        .length(dirStatus.getLen()).mtime(dirStatus.getModificationTime())
        .owner(dirStatus.getOwner()).path(dirStatus.getLocalNameInBytes())
        .perm(dirStatus.getPermission()).replication(dirStatus.getReplication())
        .storagePolicy(dirStatus.getStoragePolicy())
        .symlink(dirStatus.getSymlinkInBytes()).flags(flags).build();
  }

  /**
   * Creates an instance of an RouterRpcFairnessPolicyController
   * from the configuration.
   *
   * @param conf Configuration that defines the fairness controller class.
   * @return Fairness policy controller.
   */
  public static RouterRpcFairnessPolicyController newFairnessPolicyController(
      Configuration conf) {
    Class<? extends RouterRpcFairnessPolicyController> clazz = conf.getClass(
        RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS_DEFAULT,
        RouterRpcFairnessPolicyController.class);
    return newInstance(conf, null, null, clazz);
  }

  /**
   * Collect all configured nameservices.
   *
   * @param conf the configuration object.
   * @return Set of name services in config.
   * @throws IllegalArgumentException if monitored namenodes are not correctly configured.
   */
  public static Set<String> getAllConfiguredNS(Configuration conf)
      throws IllegalArgumentException {
    // Get all name services configured
    Collection<String> namenodes = conf.getTrimmedStringCollection(
        DFS_ROUTER_MONITOR_NAMENODE);

    Set<String> nameservices = new HashSet();
    for (String namenode : namenodes) {
      String[] namenodeSplit = namenode.split("\\.");
      String nsId;
      if (namenodeSplit.length == 2) {
        nsId = namenodeSplit[0];
      } else if (namenodeSplit.length == 1) {
        nsId = namenode;
      } else {
        String errorMsg = "Wrong name service specified : " + namenode;
        throw new IllegalArgumentException(
            errorMsg);
      }
      nameservices.add(nsId);
    }
    return nameservices;
  }
}