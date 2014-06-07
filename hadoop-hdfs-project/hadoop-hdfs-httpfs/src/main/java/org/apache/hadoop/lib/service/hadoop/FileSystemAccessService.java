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

package org.apache.hadoop.lib.service.hadoop;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.lib.server.BaseService;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.FileSystemAccess;
import org.apache.hadoop.lib.service.FileSystemAccessException;
import org.apache.hadoop.lib.service.Instrumentation;
import org.apache.hadoop.lib.service.Scheduler;
import org.apache.hadoop.lib.util.Check;
import org.apache.hadoop.lib.util.ConfigurationUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@InterfaceAudience.Private
public class FileSystemAccessService extends BaseService implements FileSystemAccess {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemAccessService.class);

  public static final String PREFIX = "hadoop";

  private static final String INSTRUMENTATION_GROUP = "hadoop";

  public static final String AUTHENTICATION_TYPE = "authentication.type";
  public static final String KERBEROS_KEYTAB = "authentication.kerberos.keytab";
  public static final String KERBEROS_PRINCIPAL = "authentication.kerberos.principal";
  public static final String FS_CACHE_PURGE_FREQUENCY = "filesystem.cache.purge.frequency";
  public static final String FS_CACHE_PURGE_TIMEOUT = "filesystem.cache.purge.timeout";

  public static final String NAME_NODE_WHITELIST = "name.node.whitelist";

  public static final String HADOOP_CONF_DIR = "config.dir";

  private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml"};

  private static final String FILE_SYSTEM_SERVICE_CREATED = "FileSystemAccessService.created";

  private static class CachedFileSystem {
    private FileSystem fs;
    private long lastUse;
    private long timeout;
    private int count;

    public CachedFileSystem(long timeout) {
      this.timeout = timeout;
      lastUse = -1;
      count = 0;
    }

    synchronized FileSystem getFileSytem(Configuration conf)
      throws IOException {
      if (fs == null) {
        fs = FileSystem.get(conf);
      }
      lastUse = -1;
      count++;
      return fs;
    }

    synchronized void release() throws IOException {
      count--;
      if (count == 0) {
        if (timeout == 0) {
          fs.close();
          fs = null;
          lastUse = -1;
        }
        else {
          lastUse = System.currentTimeMillis();
        }
      }
    }

    // to avoid race conditions in the map cache adding removing entries
    // an entry in the cache remains forever, it just closes/opens filesystems
    // based on their utilization. Worse case scenario, the penalty we'll
    // pay is that the amount of entries in the cache will be the total
    // number of users in HDFS (which seems a resonable overhead).
    synchronized boolean purgeIfIdle() throws IOException {
      boolean ret = false;
      if (count == 0 && lastUse != -1 &&
          (System.currentTimeMillis() - lastUse) > timeout) {
        fs.close();
        fs = null;
        lastUse = -1;
        ret = true;
      }
      return ret;
    }

  }

  public FileSystemAccessService() {
    super(PREFIX);
  }

  private Collection<String> nameNodeWhitelist;

  Configuration serviceHadoopConf;

  private AtomicInteger unmanagedFileSystems = new AtomicInteger();

  private ConcurrentHashMap<String, CachedFileSystem> fsCache =
    new ConcurrentHashMap<String, CachedFileSystem>();

  private long purgeTimeout;

  @Override
  protected void init() throws ServiceException {
    LOG.info("Using FileSystemAccess JARs version [{}]", VersionInfo.getVersion());
    String security = getServiceConfig().get(AUTHENTICATION_TYPE, "simple").trim();
    if (security.equals("kerberos")) {
      String defaultName = getServer().getName();
      String keytab = System.getProperty("user.home") + "/" + defaultName + ".keytab";
      keytab = getServiceConfig().get(KERBEROS_KEYTAB, keytab).trim();
      if (keytab.length() == 0) {
        throw new ServiceException(FileSystemAccessException.ERROR.H01, KERBEROS_KEYTAB);
      }
      String principal = defaultName + "/localhost@LOCALHOST";
      principal = getServiceConfig().get(KERBEROS_PRINCIPAL, principal).trim();
      if (principal.length() == 0) {
        throw new ServiceException(FileSystemAccessException.ERROR.H01, KERBEROS_PRINCIPAL);
      }
      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", "kerberos");
      UserGroupInformation.setConfiguration(conf);
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
      } catch (IOException ex) {
        throw new ServiceException(FileSystemAccessException.ERROR.H02, ex.getMessage(), ex);
      }
      LOG.info("Using FileSystemAccess Kerberos authentication, principal [{}] keytab [{}]", principal, keytab);
    } else if (security.equals("simple")) {
      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", "simple");
      UserGroupInformation.setConfiguration(conf);
      LOG.info("Using FileSystemAccess simple/pseudo authentication, principal [{}]", System.getProperty("user.name"));
    } else {
      throw new ServiceException(FileSystemAccessException.ERROR.H09, security);
    }

    String hadoopConfDirProp = getServiceConfig().get(HADOOP_CONF_DIR, getServer().getConfigDir());
    File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
    if (hadoopConfDir == null) {
      hadoopConfDir = new File(getServer().getConfigDir()).getAbsoluteFile();
    }
    if (!hadoopConfDir.exists()) {
      throw new ServiceException(FileSystemAccessException.ERROR.H10, hadoopConfDir);
    }
    try {
      serviceHadoopConf = loadHadoopConf(hadoopConfDir);
    } catch (IOException ex) {
      throw new ServiceException(FileSystemAccessException.ERROR.H11, ex.toString(), ex);
    }

    LOG.debug("FileSystemAccess FileSystem configuration:");
    for (Map.Entry entry : serviceHadoopConf) {
      LOG.debug("  {} = {}", entry.getKey(), entry.getValue());
    }
    setRequiredServiceHadoopConf(serviceHadoopConf);

    nameNodeWhitelist = toLowerCase(getServiceConfig().getTrimmedStringCollection(NAME_NODE_WHITELIST));
  }

  private Configuration loadHadoopConf(File dir) throws IOException {
    Configuration hadoopConf = new Configuration(false);
    for (String file : HADOOP_CONF_FILES) {
      File f = new File(dir, file);
      if (f.exists()) {
        hadoopConf.addResource(new Path(f.getAbsolutePath()));
      }
    }
    return hadoopConf;
  }

  @Override
  public void postInit() throws ServiceException {
    super.postInit();
    Instrumentation instrumentation = getServer().get(Instrumentation.class);
    instrumentation.addVariable(INSTRUMENTATION_GROUP, "unmanaged.fs", new Instrumentation.Variable<Integer>() {
      @Override
      public Integer getValue() {
        return unmanagedFileSystems.get();
      }
    });
    instrumentation.addSampler(INSTRUMENTATION_GROUP, "unmanaged.fs", 60, new Instrumentation.Variable<Long>() {
      @Override
      public Long getValue() {
        return (long) unmanagedFileSystems.get();
      }
    });
    Scheduler scheduler = getServer().get(Scheduler.class);
    int purgeInterval = getServiceConfig().getInt(FS_CACHE_PURGE_FREQUENCY, 60);
    purgeTimeout = getServiceConfig().getLong(FS_CACHE_PURGE_TIMEOUT, 60);
    purgeTimeout = (purgeTimeout > 0) ? purgeTimeout : 0;
    if (purgeTimeout > 0) {
      scheduler.schedule(new FileSystemCachePurger(),
                         purgeInterval, purgeInterval, TimeUnit.SECONDS);
    }
  }

  private class FileSystemCachePurger implements Runnable {

    @Override
    public void run() {
      int count = 0;
      for (CachedFileSystem cacheFs : fsCache.values()) {
        try {
          count += cacheFs.purgeIfIdle() ? 1 : 0;
        } catch (Throwable ex) {
          LOG.warn("Error while purging filesystem, " + ex.toString(), ex);
        }
      }
      LOG.debug("Purged [{}} filesystem instances", count);
    }
  }

  private Set<String> toLowerCase(Collection<String> collection) {
    Set<String> set = new HashSet<String>();
    for (String value : collection) {
      set.add(value.toLowerCase());
    }
    return set;
  }

  @Override
  public Class getInterface() {
    return FileSystemAccess.class;
  }

  @Override
  public Class[] getServiceDependencies() {
    return new Class[]{Instrumentation.class, Scheduler.class};
  }

  protected UserGroupInformation getUGI(String user) throws IOException {
    return UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
  }

  protected void setRequiredServiceHadoopConf(Configuration conf) {
    conf.set("fs.hdfs.impl.disable.cache", "true");
  }

  private static final String HTTPFS_FS_USER = "httpfs.fs.user";

  protected FileSystem createFileSystem(Configuration namenodeConf)
    throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    CachedFileSystem newCachedFS = new CachedFileSystem(purgeTimeout);
    CachedFileSystem cachedFS = fsCache.putIfAbsent(user, newCachedFS);
    if (cachedFS == null) {
      cachedFS = newCachedFS;
    }
    Configuration conf = new Configuration(namenodeConf);
    conf.set(HTTPFS_FS_USER, user);
    return cachedFS.getFileSytem(conf);
  }

  protected void closeFileSystem(FileSystem fs) throws IOException {
    if (fsCache.containsKey(fs.getConf().get(HTTPFS_FS_USER))) {
      fsCache.get(fs.getConf().get(HTTPFS_FS_USER)).release();
    }
  }

  protected void validateNamenode(String namenode) throws FileSystemAccessException {
    if (nameNodeWhitelist.size() > 0 && !nameNodeWhitelist.contains("*")) {
      if (!nameNodeWhitelist.contains(namenode.toLowerCase())) {
        throw new FileSystemAccessException(FileSystemAccessException.ERROR.H05, namenode, "not in whitelist");
      }
    }
  }

  protected void checkNameNodeHealth(FileSystem fileSystem) throws FileSystemAccessException {
  }

  @Override
  public <T> T execute(String user, final Configuration conf, final FileSystemExecutor<T> executor)
    throws FileSystemAccessException {
    Check.notEmpty(user, "user");
    Check.notNull(conf, "conf");
    Check.notNull(executor, "executor");
    if (!conf.getBoolean(FILE_SYSTEM_SERVICE_CREATED, false)) {
      throw new FileSystemAccessException(FileSystemAccessException.ERROR.H04);
    }
    if (conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) == null ||
        conf.getTrimmed(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY).length() == 0) {
      throw new FileSystemAccessException(FileSystemAccessException.ERROR.H06,
                                          CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    }
    try {
      validateNamenode(
        new URI(conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY)).
          getAuthority());
      UserGroupInformation ugi = getUGI(user);
      return ugi.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          FileSystem fs = createFileSystem(conf);
          Instrumentation instrumentation = getServer().get(Instrumentation.class);
          Instrumentation.Cron cron = instrumentation.createCron();
          try {
            checkNameNodeHealth(fs);
            cron.start();
            return executor.execute(fs);
          } finally {
            cron.stop();
            instrumentation.addCron(INSTRUMENTATION_GROUP, executor.getClass().getSimpleName(), cron);
            closeFileSystem(fs);
          }
        }
      });
    } catch (FileSystemAccessException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new FileSystemAccessException(FileSystemAccessException.ERROR.H03, ex);
    }
  }

  public FileSystem createFileSystemInternal(String user, final Configuration conf)
    throws IOException, FileSystemAccessException {
    Check.notEmpty(user, "user");
    Check.notNull(conf, "conf");
    if (!conf.getBoolean(FILE_SYSTEM_SERVICE_CREATED, false)) {
      throw new FileSystemAccessException(FileSystemAccessException.ERROR.H04);
    }
    try {
      validateNamenode(
        new URI(conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY)).getAuthority());
      UserGroupInformation ugi = getUGI(user);
      return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return createFileSystem(conf);
        }
      });
    } catch (IOException ex) {
      throw ex;
    } catch (FileSystemAccessException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new FileSystemAccessException(FileSystemAccessException.ERROR.H08, ex.getMessage(), ex);
    }
  }

  @Override
  public FileSystem createFileSystem(String user, final Configuration conf) throws IOException,
    FileSystemAccessException {
    unmanagedFileSystems.incrementAndGet();
    return createFileSystemInternal(user, conf);
  }

  @Override
  public void releaseFileSystem(FileSystem fs) throws IOException {
    unmanagedFileSystems.decrementAndGet();
    closeFileSystem(fs);
  }

  @Override
  public Configuration getFileSystemConfiguration() {
    Configuration conf = new Configuration(true);
    ConfigurationUtils.copy(serviceHadoopConf, conf);
    conf.setBoolean(FILE_SYSTEM_SERVICE_CREATED, true);

    // Force-clear server-side umask to make HttpFS match WebHDFS behavior
    conf.set(FsPermission.UMASK_LABEL, "000");

    return conf;
  }

}
