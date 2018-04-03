/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;


/**
 * A filesystem implementation of {@link YarnConfigurationStore}. Offer
 * configuration storage in FileSystem
 */
public class FSSchedulerConfigurationStore extends YarnConfigurationStore {
  public static final Log LOG = LogFactory.getLog(
      FSSchedulerConfigurationStore.class);

  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO
      = Version.newInstance(0, 1);

  private static final String TMP = ".tmp";

  private int maxVersion;
  private Path schedulerConfDir;
  private FileSystem fileSystem;
  private LogMutation pendingMutation;
  private PathFilter configFilePathFilter;
  private volatile Configuration schedConf;
  private volatile Configuration oldConf;
  private Path tempConfigPath;

  @Override
  public void initialize(Configuration conf, Configuration vSchedConf,
      RMContext rmContext) throws Exception {
    this.configFilePathFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        if (path == null) {
          return false;
        }
        String pathName = path.getName();
        return pathName.startsWith(YarnConfiguration.CS_CONFIGURATION_FILE)
            && !pathName.endsWith(TMP);
      }
    };

    String schedulerConfPathStr = conf.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH);
    if (schedulerConfPathStr == null || schedulerConfPathStr.isEmpty()) {
      throw new IOException(
          YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH
              + " must be set");
    }
    this.schedulerConfDir = new Path(schedulerConfPathStr);
    this.fileSystem = this.schedulerConfDir.getFileSystem(conf);
    this.maxVersion = conf.getInt(
        YarnConfiguration.SCHEDULER_CONFIGURATION_FS_MAX_VERSION,
        YarnConfiguration.DEFAULT_SCHEDULER_CONFIGURATION_FS_MAX_VERSION);
    LOG.info("schedulerConfDir=" + schedulerConfPathStr);
    LOG.info("capacity scheduler file max version = " + maxVersion);

    if (!fileSystem.exists(schedulerConfDir)) {
      if (!fileSystem.mkdirs(schedulerConfDir)) {
        throw new IOException("mkdir " + schedulerConfPathStr + " failed");
      }
    }

    // create capacity-schedule.xml.ts file if not existing
    if (this.getConfigFileInputStream() == null) {
      writeConfigurationToFileSystem(vSchedConf);
    }

    this.schedConf = this.getConfigurationFromFileSystem();
  }

  /**
   * Update and persist latest configuration in temp file.
   * @param logMutation configuration change to be persisted in write ahead log
   * @throws IOException throw IOE when write temp configuration file fail
   */
  @Override
  public void logMutation(LogMutation logMutation) throws IOException {
    pendingMutation = logMutation;
    LOG.info(new GsonBuilder().serializeNulls().create().toJson(logMutation));
    oldConf = new Configuration(schedConf);
    Map<String, String> mutations = pendingMutation.getUpdates();
    for (Map.Entry<String, String> kv : mutations.entrySet()) {
      if (kv.getValue() == null) {
        this.schedConf.unset(kv.getKey());
      } else {
        this.schedConf.set(kv.getKey(), kv.getValue());
      }
    }
    tempConfigPath = writeTmpConfig(schedConf);
  }

  /**
   * @param isValid if true, finalize temp configuration file
   *                if false, remove temp configuration file and rollback
   * @throws Exception throw IOE when write temp configuration file fail
   */
  @Override
  public void confirmMutation(boolean isValid) throws Exception {
    if (pendingMutation == null || tempConfigPath == null) {
      LOG.warn("pendingMutation or tempConfigPath is null, do nothing");
      return;
    }
    if (isValid) {
      finalizeFileSystemFile();
    } else {
      schedConf = oldConf;
      removeTmpConfigFile();
    }
    tempConfigPath = null;
  }

  private void finalizeFileSystemFile() throws IOException {
    // call confirmMutation() make sure tempConfigPath is not null
    Path finalConfigPath = getFinalConfigPath(tempConfigPath);
    fileSystem.rename(tempConfigPath, finalConfigPath);
    LOG.info("finalize temp configuration file successfully, finalConfigPath="
        + finalConfigPath);
  }

  private Path getFinalConfigPath(Path tempPath) {
    String tempConfigPathStr = tempPath.getName();
    if (!tempConfigPathStr.endsWith(TMP)) {
      LOG.warn(tempPath + " does not end with '"
          + TMP + "' return null");
      return null;
    }
    String finalConfigPathStr = tempConfigPathStr.substring(0,
        (tempConfigPathStr.length() - TMP.length()));
    return new Path(tempPath.getParent(), finalConfigPathStr);
  }

  private void removeTmpConfigFile() throws IOException {
    // call confirmMutation() make sure tempConfigPath is not null
    fileSystem.delete(tempConfigPath, true);
    LOG.info("delete temp configuration file: " + tempConfigPath);
  }

  private Configuration getConfigurationFromFileSystem() throws IOException {
    long start = Time.monotonicNow();

    Configuration conf = new Configuration(false);
    InputStream configInputStream = getConfigFileInputStream();
    if (configInputStream == null) {
      throw new IOException(
          "no capacity scheduler file in " + this.schedulerConfDir);
    }

    conf.addResource(configInputStream);
    Configuration result = new Configuration(false);
    for (Map.Entry<String, String> entry : conf) {
      result.set(entry.getKey(), entry.getValue());
    }
    LOG.info("upload conf from fileSystem took "
            + (Time.monotonicNow() - start) + " ms");

    //for ha transition, local schedConf may be old one.
    this.schedConf = result;
    return result;
  }

  private InputStream getConfigFileInputStream() throws IOException {
    Path lastestConfigPath = getLatestConfigPath();
    if (lastestConfigPath == null) {
      return null;
    }
    return fileSystem.open(lastestConfigPath);
  }

  private Path getLatestConfigPath() throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(this.schedulerConfDir,
        this.configFilePathFilter);

    if (fileStatuses == null || fileStatuses.length == 0) {
      return null;
    }
    Arrays.sort(fileStatuses);

    return fileStatuses[fileStatuses.length - 1].getPath();
  }

  @VisibleForTesting
  private Path writeTmpConfig(Configuration vSchedConf) throws IOException {
    long start = Time.monotonicNow();
    String tempSchedulerConfigFile = YarnConfiguration.CS_CONFIGURATION_FILE
        + "." + System.currentTimeMillis() + TMP;

    Path tempSchedulerConfigPath = new Path(this.schedulerConfDir,
        tempSchedulerConfigFile);

    try (FSDataOutputStream outputStream = fileSystem.create(
        tempSchedulerConfigPath)) {
      //clean configuration file when num exceed maxVersion
      cleanConfigurationFile();

      vSchedConf.writeXml(outputStream);
      LOG.info(
          "write temp capacity configuration successfully, schedulerConfigFile="
              + tempSchedulerConfigPath);
    } catch (IOException e) {
      LOG.info("write temp capacity configuration fail, schedulerConfigFile="
          + tempSchedulerConfigPath, e);
      throw e;
    }
    LOG.info("write temp configuration to fileSystem took "
        + (Time.monotonicNow() - start) + " ms");
    return tempSchedulerConfigPath;
  }

  @VisibleForTesting
  void writeConfigurationToFileSystem(Configuration vSchedConf)
      throws IOException {
    tempConfigPath = writeTmpConfig(vSchedConf);
    finalizeFileSystemFile();
  }

  private void cleanConfigurationFile() throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(this.schedulerConfDir,
        this.configFilePathFilter);

    if (fileStatuses == null || fileStatuses.length <= this.maxVersion) {
      return;
    }
    Arrays.sort(fileStatuses);
    int configFileNum = fileStatuses.length;
    if (fileStatuses.length > this.maxVersion) {
      for (int i = 0; i < configFileNum - this.maxVersion; i++) {
        fileSystem.delete(fileStatuses[i].getPath(), false);
        LOG.info("delete config file " + fileStatuses[i].getPath());
      }
    }
  }

  @Override
  public Configuration retrieve() throws IOException {
    return getConfigurationFromFileSystem();
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    // Unimplemented.
    return null;
  }

  @Override
  protected Version getConfStoreVersion() throws Exception {
    return null;
  }

  @Override
  protected void storeVersion() throws Exception {

  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  public void close() throws IOException {
    if (fileSystem != null) {
      fileSystem.close();
    }
  }
}
