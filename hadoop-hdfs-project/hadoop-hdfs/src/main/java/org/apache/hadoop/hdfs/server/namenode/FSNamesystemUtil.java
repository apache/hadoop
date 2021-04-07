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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.ECTopologyVerifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;

/**
 * This is a utility class for FSNamesystem functionality
 */
@InterfaceAudience.Private
public class FSNamesystemUtil {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(FSNamesystemUtil.class);

  /**
   * Check whether operation is supported.
   *
   * @param operationName the name of operation.
   * @param effectiveLayoutVersion layout version in effect.
   * @throws UnsupportedActionException throws UAE if not supported.
   */
  public static void checkErasureCodingSupported(String operationName,
      int effectiveLayoutVersion)
      throws UnsupportedActionException {
    if (!NameNodeLayoutVersion.supports(
        NameNodeLayoutVersion.Feature.ERASURE_CODING, effectiveLayoutVersion)) {
      throw new UnsupportedActionException(operationName + " not supported.");
    }
  }

  static String getFailedStorageCommand(String mode) {
    if (mode.equals("check")) {
      return "checkRestoreFailedStorage";
    } else if (mode.equals("true")) {
      return "enableRestoreFailedStorage";
    } else {
      return "disableRestoreFailedStorage";
    }
  }

  static String getQuotaCommand(long nsQuota, long dsQuota) {
    if (nsQuota == HdfsConstants.QUOTA_RESET
        && dsQuota == HdfsConstants.QUOTA_DONT_SET) {
      return "clearQuota";
    } else if (nsQuota == HdfsConstants.QUOTA_DONT_SET
        && dsQuota == HdfsConstants.QUOTA_RESET) {
      return "clearSpaceQuota";
    } else if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
      return "setQuota";
    } else {
      return "setSpaceQuota";
    }
  }

  static ECTopologyVerifierResult getEcTopologyVerifierResultForEnabledPolicies(
      BlockManager blockManager,
      ErasureCodingPolicyManager erasureCodingPolicyManager) {
    int numOfDataNodes =
        blockManager.getDatanodeManager().getNumOfDataNodes();
    int numOfRacks = blockManager.getDatanodeManager().getNetworkTopology()
        .getNumOfRacks();
    ErasureCodingPolicy[] enabledEcPolicies =
        erasureCodingPolicyManager.getCopyOfEnabledPolicies();
    return ECTopologyVerifier
        .getECTopologyVerifierResult(numOfRacks, numOfDataNodes,
            Arrays.asList(enabledEcPolicies));
  }

  static void enableAsyncAuditLog(Configuration conf, Log auditLog) {
    if (!(auditLog instanceof Log4JLogger)) {
      LOG.warn("Log4j is required to enable async auditlog");
      return;
    }
    Logger logger = ((Log4JLogger) auditLog).getLogger();
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0) instanceof AsyncAppender)) {
      AsyncAppender asyncAppender = new AsyncAppender();
      asyncAppender.setBlocking(conf.getBoolean(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_BLOCKING_KEY,
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_BLOCKING_DEFAULT
      ));
      asyncAppender.setBufferSize(conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_BUFFER_SIZE_KEY,
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_BUFFER_SIZE_DEFAULT
      ));
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      logger.addAppender(asyncAppender);
    }
  }

  static Supplier<String> getLockReportInfoSupplier(String src, String dst,
      FileStatus status) {
    return () -> {
      UserGroupInformation ugi = Server.getRemoteUser();
      String userName = ugi != null ? ugi.toString() : null;
      InetAddress addr = Server.getRemoteIp();
      StringBuilder sb = new StringBuilder();
      String s = escapeJava(src);
      String d = escapeJava(dst);
      sb.append("ugi=").append(userName).append(",")
          .append("ip=").append(addr).append(",")
          .append("src=").append(s).append(",")
          .append("dst=").append(d).append(",");
      if (null == status) {
        sb.append("perm=null");
      } else {
        sb.append("perm=")
            .append(status.getOwner()).append(":")
            .append(status.getGroup()).append(":")
            .append(status.getPermission());
      }
      return sb.toString();
    };
  }

  static Supplier<String> getLockReportInfoSupplier(String src, String dst,
      HdfsFileStatus stat) {
    FileStatus status = null;
    if (stat != null) {
      Path symlink = stat.isSymlink()
          ? new Path(DFSUtilClient.bytes2String(stat.getSymlinkInBytes()))
          : null;
      Path path = new Path(src);
      status = new FileStatus(stat.getLen(), stat.isDirectory(),
          stat.getReplication(), stat.getBlockSize(),
          stat.getModificationTime(),
          stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
          stat.getGroup(), symlink, path);
    }
    return getLockReportInfoSupplier(src, dst, status);
  }

  @VisibleForTesting
  static RetryCache initRetryCache(Configuration conf) {
    boolean enable = conf.getBoolean(DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY,
        DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT);
    LOG.info("Retry cache on namenode is " + (enable ? "enabled" : "disabled"));
    if (enable) {
      float heapPercent = conf.getFloat(
          DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY,
          DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT);
      long entryExpiryMillis = conf.getLong(
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY,
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT);
      LOG.info("Retry cache will use " + heapPercent
          + " of total heap and retry cache entry expiry time is "
          + entryExpiryMillis + " millis");
      long entryExpiryNanos = entryExpiryMillis * 1000 * 1000;
      return new RetryCache("NameNodeRetryCache", heapPercent,
          entryExpiryNanos);
    }
    return null;
  }

  /**
   * Convert string cookie to integer.
   */
  static int getIntCookie(String cookie) {
    int c;
    if (cookie == null) {
      c = 0;
    } else {
      try {
        c = Integer.parseInt(cookie);
      } catch (NumberFormatException e) {
        c = 0;
      }
    }
    c = Math.max(0, c);
    return c;
  }

  @VisibleForTesting
  static int getEffectiveLayoutVersion(boolean isRollingUpgrade, int storageLV,
      int minCompatLV, int currentLV) {
    if (isRollingUpgrade) {
      if (storageLV <= minCompatLV) {
        // The prior layout version satisfies the minimum compatible layout
        // version of the current software.  Keep reporting the prior layout
        // as the effective one.  Downgrade is possible.
        return storageLV;
      }
    }
    // The current software cannot satisfy the layout version of the prior
    // software.  Proceed with using the current layout version.
    return currentLV;
  }

  /**
   * Returns edit directories that are shared between primary and secondary.
   * @param conf configuration
   * @return collection of edit directories from {@code conf}
   */
  public static List<URI> getSharedEditsDirs(Configuration conf) {
    // don't use getStorageDirs here, because we want an empty default
    // rather than the dir in /tmp
    Collection<String> dirNames = conf.getTrimmedStringCollection(
        DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
    return Util.stringCollectionAsURIs(dirNames);
  }

  static Collection<URI> getStorageDirs(Configuration conf,
      String propertyName) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(propertyName);
    HdfsServerConstants.StartupOption startOpt =
        NameNode.getStartupOption(conf);
    if(startOpt == HdfsServerConstants.StartupOption.IMPORT) {
      // In case of IMPORT this will get rid of default directories
      // but will retain directories specified in hdfs-site.xml
      // When importing image from a checkpoint, the name-node can
      // start with empty set of storage directories.
      Configuration cE = new HdfsConfiguration(false);
      cE.addResource("core-default.xml");
      cE.addResource("core-site.xml");
      cE.addResource("hdfs-default.xml");
      Collection<String> dirNames2 =
          cE.getTrimmedStringCollection(propertyName);
      dirNames.removeAll(dirNames2);
      if(dirNames.isEmpty())
        LOG.warn("!!! WARNING !!!" +
          "\n\tThe NameNode currently runs without persistent storage." +
          "\n\tAny changes to the file system meta-data may be lost." +
          "\n\tRecommended actions:" +
          "\n\t\t- shutdown and restart NameNode with configured \""
          + propertyName + "\" in hdfs-site.xml;" +
          "\n\t\t- use Backup Node as a persistent and up-to-date storage " +
          "of the file system meta-data.");
    } else if (dirNames.isEmpty()) {
      dirNames = Collections.singletonList(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_DEFAULT);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, DFS_NAMENODE_NAME_DIR_KEY);
  }

  public static List<URI> getNamespaceEditsDirs(Configuration conf,
      boolean includeShared) throws IOException {
    // Use a LinkedHashSet so that order is maintained while we de-dup
    // the entries.
    LinkedHashSet<URI> editsDirs = new LinkedHashSet<URI>();

    if (includeShared) {
      List<URI> sharedDirs = getSharedEditsDirs(conf);

      // Fail until multiple shared edits directories are supported (HDFS-2782)
      if (sharedDirs.size() > 1) {
        throw new IOException(
            "Multiple shared edits directories are not yet supported");
      }

      // First add the shared edits dirs. It's critical that the shared dirs
      // are added first, since JournalSet syncs them in the order they are
      // listed, and we need to make sure all edits are in place in the shared
      // storage before they are replicated locally. See HDFS-2874.
      for (URI dir : sharedDirs) {
        if (!editsDirs.add(dir)) {
          LOG.warn("Edits URI " + dir + " listed multiple times in " +
              DFS_NAMENODE_SHARED_EDITS_DIR_KEY + ". Ignoring duplicates.");
        }
      }
    }
    // Now add the non-shared dirs.
    for (URI dir : getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_KEY)) {
      if (!editsDirs.add(dir)) {
        LOG.warn("Edits URI " + dir + " listed multiple times in " +
            DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " and " +
            DFS_NAMENODE_EDITS_DIR_KEY + ". Ignoring duplicates.");
      }
    }

    if (editsDirs.isEmpty()) {
      // If this is the case, no edit dirs have been explicitly configured.
      // Image dirs are to be used for edits too.
      return Lists.newArrayList(getNamespaceDirs(conf));
    } else {
      return Lists.newArrayList(editsDirs);
    }
  }

  /**
   * Return an ordered list of edits directories to write to.
   * The list is ordered such that all shared edits directories
   * are ordered before non-shared directories, and any duplicates
   * are removed. The order they are specified in the configuration
   * is retained.
   * @return Collection of shared edits directories.
   * @throws IOException if multiple shared edits directories are configured
   */
  public static List<URI> getNamespaceEditsDirs(Configuration conf)
      throws IOException {
    return getNamespaceEditsDirs(conf, true);
  }

  /**
   * Get all edits dirs which are required. If any shared edits dirs are
   * configured, these are also included in the set of required dirs.
   *
   * @param conf the HDFS configuration.
   * @return all required dirs.
   */
  public static Collection<URI> getRequiredNamespaceEditsDirs(
      Configuration conf) {
    Set<URI> ret = new HashSet<>();
    ret.addAll(getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY));
    ret.addAll(getSharedEditsDirs(conf));
    return ret;
  }

  /**
   * Check the supplied configuration for correctness.
   * @param conf Supplies the configuration to validate.
   * @throws IOException if the configuration could not be queried.
   * @throws IllegalArgumentException if the configuration is invalid.
   */
  static void checkConfiguration(Configuration conf)
      throws IOException {

    final Collection<URI> namespaceDirs =
        getNamespaceDirs(conf);
    final Collection<URI> editsDirs =
        getNamespaceEditsDirs(conf);
    final Collection<URI> requiredEditsDirs =
        getRequiredNamespaceEditsDirs(conf);
    final Collection<URI> sharedEditsDirs =
        getSharedEditsDirs(conf);

    for (URI u : requiredEditsDirs) {
      if (u.toString().compareTo(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_DEFAULT) == 0) {
        continue;
      }

      // Each required directory must also be in editsDirs or in
      // sharedEditsDirs.
      if (!editsDirs.contains(u) &&
          !sharedEditsDirs.contains(u)) {
        throw new IllegalArgumentException("Required edits directory " + u
            + " not found: "
            + DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY + "=" + editsDirs + "; "
            + DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY
            + "=" + requiredEditsDirs + "; "
            + DFS_NAMENODE_SHARED_EDITS_DIR_KEY
            + "=" + sharedEditsDirs);
      }
    }

    if (namespaceDirs.size() == 1) {
      LOG.warn("Only one image storage directory ("
          + DFS_NAMENODE_NAME_DIR_KEY + ") configured. Beware of data loss"
          + " due to lack of redundant storage directories!");
    }
    if (editsDirs.size() == 1) {
      LOG.warn("Only one namespace edits storage directory ("
          + DFS_NAMENODE_EDITS_DIR_KEY + ") configured. Beware of data loss"
          + " due to lack of redundant storage directories!");
    }
  }
}
