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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class HomeDirectoryAutoCreator extends Configured {

  public static final Logger LOG =
      LoggerFactory.getLogger(HomeDirectoryAutoCreator.class.getName());

  public static final String QUOTA_DONT_SET = "none";

  public static final String  DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED = "dfs.namenode.auto.create.user.home.enabled";
  public static final boolean DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION = "dfs.namenode.auto.create.user.home.permission";
  public static final String  DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION_DEFAULT = "0700";
  public static final String  DFS_NAMENODE_AUTO_CREATE_USER_HOME_GROUP = "dfs.namenode.auto.create.user.home.group";

  // [group:]nameQuota:spaceQuota,group:nameQuota:spaceQuota,
  // nameQuota example: 1000000
  // spaceQuota example: 100T, 100G, 100M
  // e.g. "100000:10G,services:1000000:100T"
  public static final String  DFS_NAMENODE_AUTO_CREATE_USER_HOME_QUOTA =  "dfs.namenode.auto.create.user.home.quota";
  public static final String  DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_MAX_SIZE =  "dfs.namenode.auto.create.user.home.cacheMaxSize";
  public static final long  DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_MAX_SIZE_DEFAULT = 1000L;
  public static final String  DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_TTL =  "dfs.namenode.auto.create.user.home.cacheTTL";
  public static final long  DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_TTL_DEFAULT = 0;

  private Cache<String, Boolean> cache;

  private NameNode nn;
  private FSNamesystem namesystem;
  private boolean enabled;
  private FsPermission permission;
  private volatile Map<String,QuotaValue> quotaMap;

  private String defaultGroup;
  private UserGroupInformation nameNodeUser;
  private long cacheMaxSize;
  private long cacheTTLms;

  private HomeDirMetrics metrics;

  public HomeDirectoryAutoCreator(Configuration conf, NameNode namenode) throws IOException {
    super(conf);
    this.nn = namenode;
    this.namesystem = namenode.getNamesystem();
    this.nameNodeUser = UserGroupInformation.getLoginUser();
    this.enabled = conf.getBoolean(DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED,
        DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED_DEFAULT);

    if (enabled) {
      permission = getHomeDirPermission(conf);
      quotaMap = QuotaValue.parseQuota(conf.get(DFS_NAMENODE_AUTO_CREATE_USER_HOME_QUOTA));

      defaultGroup = conf.get(DFS_NAMENODE_AUTO_CREATE_USER_HOME_GROUP);
      cacheMaxSize = conf.getLong(DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_MAX_SIZE,
          DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_MAX_SIZE_DEFAULT);
      cacheTTLms = conf.getTimeDuration(DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_TTL,
          DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_TTL_DEFAULT, TimeUnit.MILLISECONDS);
      CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().maximumSize(cacheMaxSize);
      if (cacheTTLms > 0) {
        builder.expireAfterWrite(cacheTTLms, TimeUnit.MILLISECONDS);
      }
      cache = builder.build();
      nn.getReconfigurableProperties().add(DFS_NAMENODE_AUTO_CREATE_USER_HOME_QUOTA);
      this.metrics = HomeDirMetrics.create(HomeDirectoryAutoCreator.class.getSimpleName(), this);
    }
    if (enabled) {
      LOG.info("Auto creation enabled (cacheMaxSize={}, cacheTTLms={}ms)",
          cacheMaxSize, cacheTTLms);
    } else {
      LOG.info("Auto creation for user home directory is disabled");
    }
  }

  public Cache<String,Boolean> getCache() {
    return cache;
  }
  public long getCacheMaxSize() {
    return cacheMaxSize;
  }
  public long getCacheTTLms() {
    return cacheTTLms;
  }

  public boolean isEnabled() {
    return enabled;
  }
  private String getGroup(UserGroupInformation ugi) throws IOException {
    String group = this.defaultGroup;
    if (org.apache.commons.lang3.StringUtils.isBlank(group)) {
      try {
        group = ugi.getPrimaryGroupName();
      } catch (IOException e) {
        metrics.failedGetPrimaryGroup.incr();
        throw e;
      }
    }
    return group;
  }

  @VisibleForTesting
  protected FsPermission getHomeDirPermission(Configuration conf) {
    short octal;
    String octalString = conf.get(DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION,
        DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION_DEFAULT);
    try {
      octal = Short.parseShort(octalString, 8);
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse user home permission, using default value: {}", e.toString());
      octal = Short.parseShort(DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION_DEFAULT, 8);
    }
    return new FsPermission(octal);
  }

  public String reconfigUserHomeQuota(final String newVal) {
    if (!enabled) {
      return newVal;
    }
    Map<String, QuotaValue> newQuotaMap = QuotaValue.parseQuota(newVal);
    if (newVal != null && !newVal.isEmpty() && newQuotaMap.isEmpty()) {
      throw new IllegalArgumentException(newVal + " is not valid");
    }
    quotaMap = newQuotaMap;
    LOG.info("RECONFIGURE* changed home directory quota to {}", newVal);
    return newVal;
  }

  @VisibleForTesting
  public static String toNameQuotaString(long quota) {
    if (quota == HdfsConstants.QUOTA_DONT_SET) {
      return QuotaValue.QUOTA_DONT_SET;
    }
    DecimalFormat df = new DecimalFormat("#,###");
    return df.format(quota);
  }

  @VisibleForTesting
  public static String toSpaceQuotaString(long quota) {
    if (quota == HdfsConstants.QUOTA_DONT_SET) {
      return QuotaValue.QUOTA_DONT_SET;
    }
    return StringUtils.byteDesc(quota);
  }

  /*
   * @return array size is always 2.
   * index 0 is nameQuota, index 1 is spaceQuota
   */
  private long[] getQuotas(String group) {
    long nameQuota = HdfsConstants.QUOTA_DONT_SET;
    long spaceQuota = HdfsConstants.QUOTA_DONT_SET;
    if (quotaMap != null && quotaMap.size() > 0) {
      QuotaValue quotaValue = quotaMap.get(group);
      if (quotaValue == null) {
        quotaValue = quotaMap.get(QuotaValue.DEFAULT_GROUP_KEY);
      }

      if (quotaValue != null) {
        nameQuota = quotaValue.getNameQuota();
        spaceQuota = quotaValue.getSpaceQuota();
      }
    }
    long[] quotas = new long[2];
    quotas[0] = nameQuota;
    quotas[1] = spaceQuota;
    return quotas;
  }

  private void setupHomeDirectory(String homeDir, UserGroupInformation ugi) throws IOException {
    String user = ugi.getShortUserName();
    String group = getGroup(ugi);

    // best-effort initial setup
    PermissionStatus permStatus = new PermissionStatus(user, group, permission);
    namesystem.mkdirs(homeDir, permStatus, true, nameNodeUser);

    long[] quotas = getQuotas(group);
    long nameQuota = quotas[0];
    long spaceQuota = quotas[1];
    if (nameQuota != HdfsConstants.QUOTA_DONT_SET || spaceQuota != HdfsConstants.QUOTA_DONT_SET) {
      namesystem.setQuota(homeDir, nameQuota, spaceQuota, null, nameNodeUser);
    }
    namesystem.setOwner(homeDir, user, group, nameNodeUser);
    LOG.info("Created home directory {} as owner={}:{} nameQuota={} spaceQuota={}",
        homeDir, user, group, toNameQuotaString(nameQuota), toSpaceQuotaString(spaceQuota));
  }

  private boolean existsDirectory(String dir) {
    try {
      return namesystem.getFSDirectory().getINode(dir, FSDirectory.DirOp.READ) != null;
    } catch (IOException e) {
      LOG.debug("existsDirectory failed for {}", dir, e);
      return false;
    }
  }

  public void ensureHomeDirectory(UserGroupInformation ugi) throws StandbyException {
    if (!enabled){
      return;
    }
    long start = Time.monotonicNowNanos();
    namesystem.checkOperation(NameNode.OperationCategory.READ);
    String user = ugi.getShortUserName();
    try {
      if (cache.getIfPresent(user) != null) {
        metrics.cacheHit.incr();
        return;
      }
      metrics.cacheMiss.incr();
    } finally {
      long elapsedNanos = Time.monotonicNowNanos() - start;
      metrics.cacheContainsNanosQuantiles.add(elapsedNanos);
      if (LOG.isDebugEnabled()) {
        double elapsedMs = elapsedNanos / 1_000_000.0;
        LOG.debug("cache contains(user={}) elapsed={}ms", user, String.format("%.6f", elapsedMs));
      }
    }

    String homeDir = DFSUtilClient.getHomeDirectory(getConf(), ugi);
    boolean cacheable = true;
    try {
      try {
        if (existsDirectory(homeDir)) {
          return;
        }
      } finally {
        long elapsedNanos = Time.monotonicNowNanos() - start;
        metrics.checkHomeDirNanosQuantiles.add(elapsedNanos);
        if (LOG.isDebugEnabled()) {
          double elapsedMs = elapsedNanos / 1_000_000.0;
          LOG.debug("checked home directory. elapsed={}ms homeDir={} ugi={} cacheSize={}",
              String.format("%.6f", elapsedMs), homeDir, ugi, cache.size());
        }
      }

      namesystem.checkOperation(NameNode.OperationCategory.WRITE);
      // multiple requests for single user can enter this logic
      // mkdir always succeeds even if already exists dir, except for permissions, etc issues.
      // so it does not require lock.
      setupHomeDirectory(homeDir, ugi);
      metrics.creatingHomeDirs.incr();
    } catch (StandbyException e) {
      cacheable = false;
      throw e;
    } catch (IOException e) {
      final String formatter = "Failed creating home directory." +
          " Will not attempt to create home directory in future. {}: {}";
      String errStr = e.toString();
      if (errStr != null && errStr.contains("There is no primary group")) {
        LOG.warn(formatter, homeDir, errStr);
      } else {
        LOG.error(formatter, homeDir, errStr, e);
      }
      metrics.failedCreatingHomeDirs.incr();
    } finally {
      if (cacheable) {
        cache.put(user, Boolean.TRUE);
      }
    }
  }

  public static class QuotaValue {
    public static final Logger LOG =
        LoggerFactory.getLogger(QuotaValue.class.getName());

    public static final String QUOTA_DONT_SET = "none";
    public static final String DEFAULT_GROUP_KEY = "";

    private String group;
    private long nameQuota;
    private long spaceQuota;

    public QuotaValue(String group, String strNameQuota, String strSpaceQuota) {
      this.group = group;
      setNameQuota(strNameQuota);
      setSpaceQuota(strSpaceQuota);
    }

    public String getGroup() {
      return group;
    }

    public long getNameQuota() {
      return nameQuota;
    }

    public long getSpaceQuota() {
      return spaceQuota;
    }

    public void setGroup(String group) {
      this.group = group;
    }

    public void setSpaceQuota(String strQuota) {
      if (QUOTA_DONT_SET.equalsIgnoreCase(strQuota)) {
        this.spaceQuota = HdfsConstants.QUOTA_DONT_SET;
      } else {
        try {
          long quota = StringUtils.TraditionalBinaryPrefix.string2long(strQuota);
          if (quota < 0) {
            quota = 0L;
          }
          this.spaceQuota = quota;
        } catch (IllegalArgumentException e) {
          this.spaceQuota = HdfsConstants.QUOTA_DONT_SET;
        }
      }
    }

    public void setNameQuota(String strQuota) {
      if (QUOTA_DONT_SET.equalsIgnoreCase(strQuota)) {
        this.nameQuota = HdfsConstants.QUOTA_DONT_SET;
      } else {
        try {
          long quota = StringUtils.TraditionalBinaryPrefix.string2long(strQuota);
          if (quota <= 0) {
            // for mkdir home, min quota is 1
            quota = 1;
          }
          this.nameQuota = quota;
        } catch (IllegalArgumentException e) {
          this.nameQuota = HdfsConstants.QUOTA_DONT_SET;
        }
      }
    }

    public static Map<String, QuotaValue> parseQuota(String confStr) {
      LOG.info("parse quota string \"{}\"", confStr);

      Map<String, QuotaValue> quotaMap = new ConcurrentHashMap<>();
      if (confStr == null || confStr.isEmpty()) {
        return quotaMap;
      }

      // [group:]nameQuota:spaceQuota,group:nameQuota:spaceQuota,
      String[] sp = confStr.split(",");
      for (String s : sp) {
        String[] quotas = s.split(":");
        if (quotas.length != 2 && quotas.length != 3) {
          LOG.warn("ignored quota string: \"{}\"", s);
          continue;
        }

        String group, nameQuota, spaceQuota;
        if (quotas.length == 3) {
          group = quotas[0];
          nameQuota = quotas[1];
          spaceQuota = quotas[2];
        } else {
          group = "";
          nameQuota = quotas[0];
          spaceQuota = quotas[1];
        }

        QuotaValue quotaValue = new QuotaValue(group, nameQuota, spaceQuota);
        quotaMap.put(group, quotaValue);
      }
      LOG.info("quotaMap={}", quotaMap);
      return quotaMap;
    }
  }

  @Metrics(name="HomeDirectory", about="NameNode metrics", context="dfs")
  public static class HomeDirMetrics {

    private final String name;
    private final Cache<String, Boolean> cache;
    private final long cacheMaxSize;
    private final long cacheTTLms;

    final MetricsRegistry registry = new MetricsRegistry("namenode");
    // metrics
    @Metric("Number of creating home directories")
    MutableCounterLong creatingHomeDirs;
    @Metric("Number of failed creating home directories")
    MutableCounterLong failedCreatingHomeDirs;
    @Metric("Number of failed getting primary group")
    MutableCounterLong failedGetPrimaryGroup;

    @Metric("Number of user cache size")
    public long cacheSize(){
      if (cache == null) {
        return 0;
      }
      return cache.size();
    }
    @Metric("Number of user cache max size")
    public long cacheMaxSize() {
      return cacheMaxSize;
    }
    @Metric("User cache TTL in milliseconds")
    public long cacheTTLms() {
      return cacheTTLms;
    }
    final MutableQuantiles cacheContainsNanosQuantiles;
    final MutableQuantiles checkHomeDirNanosQuantiles;
    @Metric("Number of cache hit")
    MutableCounterLong cacheHit;
    @Metric("Number of cache miss")
    MutableCounterLong cacheMiss;

    public HomeDirMetrics(String name, HomeDirectoryAutoCreator creator) {
      this.name = name;
      this.cache = creator.getCache();
      this.cacheMaxSize = creator.getCacheMaxSize();
      this.cacheTTLms = creator.getCacheTTLms();
      cacheContainsNanosQuantiles = registry.newQuantiles("cacheContains60s",
          "cache contains elapsed in ns", "ops", "latencyNanos", 60);
      checkHomeDirNanosQuantiles = registry.newQuantiles("checkHomeDir60s",
          "check home directory elapsed in ns", "ops", "latencyNanos", 60);
    }

    public void remove() {
      DefaultMetricsSystem.removeSourceName(name);
    }

    public static HomeDirMetrics create(String name, HomeDirectoryAutoCreator creator) {
      final MetricsSystem ms = DefaultMetricsSystem.instance();
      final HomeDirMetrics metrics = new HomeDirMetrics(name, creator);
      metrics.remove();
      return ms.register(name, null,  metrics);
    }
  }
}
