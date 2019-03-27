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
package org.apache.hadoop.ozone.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Storage information file. This Class defines the methods to check
 * the consistency of the storage dir and the version file.
 * <p>
 * Local storage information is stored in a separate file VERSION.
 * It contains type of the node,
 * the storage layout version, the SCM id, and
 * the OM/SCM state creation time.
 *
 */
@InterfaceAudience.Private
public abstract class Storage {
  private static final Logger LOG = LoggerFactory.getLogger(Storage.class);

  public static final String STORAGE_DIR_CURRENT = "current";
  protected static final String STORAGE_FILE_VERSION = "VERSION";
  public static final String CONTAINER_DIR = "containerDir";

  private final NodeType nodeType;
  private final File root;
  private final File storageDir;

  private StorageState state;
  private StorageInfo storageInfo;


  /**
   * Determines the state of the Version file.
   */
  public enum StorageState {
    NON_EXISTENT, NOT_INITIALIZED, INITIALIZED
  }

  public Storage(NodeType type, File root, String sdName)
      throws IOException {
    this.nodeType = type;
    this.root = root;
    this.storageDir = new File(root, sdName);
    this.state = getStorageState();
    if (state == StorageState.INITIALIZED) {
      this.storageInfo = new StorageInfo(type, getVersionFile());
    } else {
      this.storageInfo = new StorageInfo(
          nodeType, StorageInfo.newClusterID(), Time.now());
      setNodeProperties();
    }
  }

  /**
   * Gets the path of the Storage dir.
   * @return Stoarge dir path
   */
  public String getStorageDir() {
    return storageDir.getAbsoluteFile().toString();
  }

  /**
   * Gets the state of the version file.
   * @return the state of the Version file
   */
  public StorageState getState() {
    return state;
  }

  public NodeType getNodeType() {
    return storageInfo.getNodeType();
  }

  public String getClusterID() {
    return storageInfo.getClusterID();
  }

  public long getCreationTime() {
    return storageInfo.getCreationTime();
  }

  public void setClusterId(String clusterId) throws IOException {
    if (state == StorageState.INITIALIZED) {
      throw new IOException(
          "Storage directory " + storageDir + " already initialized.");
    } else {
      storageInfo.setClusterId(clusterId);
    }
  }

  /**
   * Retreives the storageInfo instance to read/write the common
   * version file properties.
   * @return the instance of the storageInfo class
   */
  protected StorageInfo getStorageInfo() {
    return storageInfo;
  }

  abstract protected Properties getNodeProperties();

  /**
   * Sets the Node properties spaecific to OM/SCM.
   */
  private void setNodeProperties() {
    Properties nodeProperties = getNodeProperties();
    if (nodeProperties != null) {
      for (String key : nodeProperties.stringPropertyNames()) {
        storageInfo.setProperty(key, nodeProperties.getProperty(key));
      }
    }
  }

  /**
   * Directory {@code current} contains latest files defining
   * the file system meta-data.
   *
   * @return the directory path
   */
  public File getCurrentDir() {
    return new File(storageDir, STORAGE_DIR_CURRENT);
  }

  /**
   * File {@code VERSION} contains the following fields:
   * <ol>
   * <li>node type</li>
   * <li>OM/SCM state creation time</li>
   * <li>other fields specific for this node type</li>
   * </ol>
   * The version file is always written last during storage directory updates.
   * The existence of the version file indicates that all other files have
   * been successfully written in the storage directory, the storage is valid
   * and does not need to be recovered.
   *
   * @return the version file path
   */
  private File getVersionFile() {
    return new File(getCurrentDir(), STORAGE_FILE_VERSION);
  }


  /**
   * Check to see if current/ directory is empty. This method is used
   * before determining to format the directory.
   * @throws IOException if unable to list files under the directory.
   */
  private void checkEmptyCurrent() throws IOException {
    File currentDir = getCurrentDir();
    if (!currentDir.exists()) {
      // if current/ does not exist, it's safe to format it.
      return;
    }
    try (DirectoryStream<Path> dirStream = Files
        .newDirectoryStream(currentDir.toPath())) {
      if (dirStream.iterator().hasNext()) {
        throw new InconsistentStorageStateException(getCurrentDir(),
            "Can't initialize the storage directory because the current "
                + "it is not empty.");
      }
    }
  }

  /**
   * Check consistency of the storage directory.
   *
   * @return state {@link StorageState} of the storage directory
   * @throws IOException
   */
  private StorageState getStorageState() throws IOException {
    assert root != null : "root is null";
    String rootPath = root.getCanonicalPath();
    try { // check that storage exists
      if (!root.exists()) {
        // storage directory does not exist
        LOG.warn("Storage directory " + rootPath + " does not exist");
        return StorageState.NON_EXISTENT;
      }
      // or is inaccessible
      if (!root.isDirectory()) {
        LOG.warn(rootPath + "is not a directory");
        return StorageState.NON_EXISTENT;
      }
      if (!FileUtil.canWrite(root)) {
        LOG.warn("Cannot access storage directory " + rootPath);
        return StorageState.NON_EXISTENT;
      }
    } catch (SecurityException ex) {
      LOG.warn("Cannot access storage directory " + rootPath, ex);
      return StorageState.NON_EXISTENT;
    }

    // check whether current directory is valid
    File versionFile = getVersionFile();
    boolean hasCurrent = versionFile.exists();

    if (hasCurrent) {
      return StorageState.INITIALIZED;
    } else {
      checkEmptyCurrent();
      return StorageState.NOT_INITIALIZED;
    }
  }

  /**
   * Creates the Version file if not present,
   * otherwise returns with IOException.
   * @throws IOException
   */
  public void initialize() throws IOException {
    if (state == StorageState.INITIALIZED) {
      throw new IOException("Storage directory already initialized.");
    }
    if (!getCurrentDir().mkdirs()) {
      throw new IOException("Cannot create directory " + getCurrentDir());
    }
    storageInfo.writeTo(getVersionFile());
  }

  /**
   * Persists current StorageInfo to file system..
   * @throws IOException
   */
  public void persistCurrentState() throws IOException {
    if (!getCurrentDir().exists()) {
      throw new IOException("Metadata dir doesn't exist, dir: " +
          getCurrentDir());
    }
    storageInfo.writeTo(getVersionFile());
  }

}

