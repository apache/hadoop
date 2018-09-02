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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.nodelabels.store.op.FSNodeStoreLogOp;
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

  protected static final Log LOG = LogFactory.getLog(AbstractFSNodeStore.class);

  private StoreType storeType;
  private FSDataOutputStream editlogOs;

  private Path editLogPath;
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
    // mkdir of root dir path
    fs.mkdirs(fsWorkingPath);
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

    LOG.info("Finished write mirror at:" + mirrorPath.toString());
    LOG.info("Finished create editlog file at:" + editLogPath.toString());
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
