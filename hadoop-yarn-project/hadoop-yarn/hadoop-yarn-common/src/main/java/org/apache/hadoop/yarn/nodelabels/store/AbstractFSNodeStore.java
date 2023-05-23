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
package org.apache.hadoop.yarn.nodelabels.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.nodelabels.store.op.FSNodeStoreLogOp;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.store.FSStoreOpHandler.StoreType;

import java.io.EOFException;
import java.io.IOException;

/**
 * Abstract class for File System based store.
 *
 * @param <M> manager filesystem store.Currently nodelabel will use
 *           CommonNodeLabelManager.
 */
public abstract class AbstractFSNodeStore<M> {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractFSNodeStore.class);

  private StoreType storeType;
  private FSDataOutputStream editlogOs;

  private Path editLogPath;
  private int replication;
  private StoreSchema schema;

  protected M manager;
  protected Path fsWorkingPath;
  protected FileSystem fs;

  public AbstractFSNodeStore(StoreType storeType) {
    this.storeType = storeType;
  }

  protected void initStore(Configuration conf, Path fsStorePath,
      StoreSchema schma, M mgr) throws IOException {
    this.schema = schma;
    this.fsWorkingPath = fsStorePath;
    this.manager = mgr;
    initFileSystem(conf);
    initNodeStoreRootDirectory(conf);
    this.replication = conf.getInt(YarnConfiguration.FS_STORE_FILE_REPLICATION,
        YarnConfiguration.DEFAULT_FS_STORE_FILE_REPLICATION);
  }

  private void initNodeStoreRootDirectory(Configuration conf) throws IOException {
    // mkdir of root dir path with retry logic
    int maxRetries = conf.getInt(YarnConfiguration.NODE_STORE_ROOT_DIR_NUM_RETRIES,
        YarnConfiguration.NODE_STORE_ROOT_DIR_NUM_DEFAULT_RETRIES);
    int retryCount = 0;
    boolean success = false;

    while (!success && retryCount <= maxRetries) {
      try {
        success = fs.mkdirs(fsWorkingPath);
      } catch (IOException e) {
        retryCount++;
        if (retryCount > maxRetries) {
          throw e;
        }
        try {
          Thread.sleep(conf.getInt(YarnConfiguration.NODE_STORE_ROOT_DIR_RETRY_INTERVAL,
              YarnConfiguration.NODE_STORE_ROOT_DIR_RETRY_DEFAULT_INTERVAL));
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    }
    LOG.info("Created store directory :" + fsWorkingPath);
  }

  /**
   * Filesystem store schema define the log name and mirror name.
   */
  public static class StoreSchema {
    private String editLogName;
    private String mirrorName;

    public StoreSchema(String editLogName, String mirrorName) {
      this.editLogName = editLogName;
      this.mirrorName = mirrorName;
    }
  }

  public void initFileSystem(Configuration conf) throws IOException {
    Configuration confCopy = new Configuration(conf);
    fs = fsWorkingPath.getFileSystem(confCopy);
    // if it's local file system, use RawLocalFileSystem instead of
    // LocalFileSystem, the latter one doesn't support append.
    if (fs.getScheme().equals("file")) {
      fs = ((LocalFileSystem) fs).getRaw();
    }
  }

  protected void writeToLog(FSNodeStoreLogOp op) throws IOException {
    try {
      ensureAppendEditLogFile();
      editlogOs.writeInt(op.getOpCode());
      op.write(editlogOs, manager);
    } finally {
      ensureCloseEditlogFile();
    }
  }

  protected void ensureAppendEditLogFile() throws IOException {
    editlogOs = fs.append(editLogPath);
  }

  protected void ensureCloseEditlogFile() throws IOException {
    editlogOs.close();
  }

  protected void loadFromMirror(Path newMirrorPath, Path oldMirrorPath)
      throws IOException {
    // If mirror.new exists, read from mirror.new
    Path mirrorToRead = fs.exists(newMirrorPath) ?
        newMirrorPath :
        fs.exists(oldMirrorPath) ? oldMirrorPath : null;
    if (mirrorToRead != null) {
      try (FSDataInputStream is = fs.open(mirrorToRead)) {
        StoreOp op = FSStoreOpHandler.getMirrorOp(storeType);
        op.recover(is, manager);
      }
    }
  }

  protected StoreType getStoreType() {
    return storeType;
  }

  public Path getFsWorkingPath() {
    return fsWorkingPath;
  }

  protected void recoverFromStore() throws IOException {
        /*
     * Steps of recover
     * 1) Read from last mirror (from mirror or mirror.old)
     * 2) Read from last edit log, and apply such edit log
     * 3) Write new mirror to mirror.writing
     * 4) Rename mirror to mirror.old
     * 5) Move mirror.writing to mirror
     * 6) Remove mirror.old
     * 7) Remove edit log and create a new empty edit log
     */

    // Open mirror from serialized file
    Path mirrorPath = new Path(fsWorkingPath, schema.mirrorName);
    Path oldMirrorPath = new Path(fsWorkingPath, schema.mirrorName + ".old");

    loadFromMirror(mirrorPath, oldMirrorPath);

    // Open and process editlog
    editLogPath = new Path(fsWorkingPath, schema.editLogName);

    loadManagerFromEditLog(editLogPath);

    // Serialize current mirror to mirror.writing
    Path writingMirrorPath =
        new Path(fsWorkingPath, schema.mirrorName + ".writing");

    try(FSDataOutputStream os = fs.create(writingMirrorPath, true)){
      StoreOp op = FSStoreOpHandler.getMirrorOp(storeType);
      op.write(os, manager);
    }
    checkAvailability(writingMirrorPath);
    // Move mirror to mirror.old
    if (fs.exists(mirrorPath)) {
      fs.delete(oldMirrorPath, false);
      fs.rename(mirrorPath, oldMirrorPath);
    }

    // move mirror.writing to mirror
    fs.rename(writingMirrorPath, mirrorPath);
    fs.delete(writingMirrorPath, false);

    // remove mirror.old
    fs.delete(oldMirrorPath, false);

    // create a new editlog file
    editlogOs = fs.create(editLogPath, true);
    editlogOs.close();
    checkAvailability(editLogPath);
    LOG.info("Finished write mirror at:" + mirrorPath.toString());
    LOG.info("Finished create editlog file at:" + editLogPath.toString());
  }

  /**
   * Make sure replica is highly available. It will avoid setting replication,
   * if the value configured for
   * {@link YarnConfiguration#FS_STORE_FILE_REPLICATION} is 0.
   */
  private void checkAvailability(Path file) throws IOException {
    try {
      if (replication != 0
          && fs.getFileStatus(file).getReplication() < replication) {
        fs.setReplication(file, (short) replication);
      }
    } catch (UnsupportedOperationException e) {
      LOG.error("Failed set replication for a file : {}", file);
    }
  }

  protected void loadManagerFromEditLog(Path editPath) throws IOException {
    if (!fs.exists(editPath)) {
      return;
    }
    try (FSDataInputStream is = fs.open(editPath)) {
      while (true) {
        try {
          StoreOp storeOp = FSStoreOpHandler.get(is.readInt(), storeType);
          storeOp.recover(is, manager);
        } catch (EOFException e) {
          // EOF hit, break
          break;
        }
      }
    }
  }

  public FileSystem getFs() {
    return fs;
  }

  public void setFs(FileSystem fs) {
    this.fs = fs;
  }

  protected void closeFSStore() {
    IOUtils.closeStreams(fs, editlogOs);
  }
}
