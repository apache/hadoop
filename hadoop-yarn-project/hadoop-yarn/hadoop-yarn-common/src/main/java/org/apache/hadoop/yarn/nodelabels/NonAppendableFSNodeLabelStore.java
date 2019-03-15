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

package org.apache.hadoop.yarn.nodelabels;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.store.FSStoreOpHandler;
import org.apache.hadoop.yarn.nodelabels.store.StoreOp;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Store implementation for Non Appendable File Store.
 */
public class NonAppendableFSNodeLabelStore extends FileSystemNodeLabelsStore {
  protected static final Logger LOG =
      LoggerFactory.getLogger(NonAppendableFSNodeLabelStore.class);

  @Override
  public void close() throws IOException {
  }


  @Override
  public void recover() throws YarnException,
      IOException {
    Path newMirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME + ".new");
    Path oldMirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME);
    loadFromMirror(newMirrorPath, oldMirrorPath);

    // if new mirror exists, remove old mirror and rename new mirror
    if (fs.exists(newMirrorPath)) {
      // remove old mirror
      try {
        fs.delete(oldMirrorPath, false);
      } catch (IOException e) {
        // do nothing
        LOG.debug("Exception while removing old mirror", e);
      }
      
      // rename new to old
      fs.rename(newMirrorPath, oldMirrorPath);
    }

    LOG.info("Node label store recover is completed");
  }
  
  @Override
  public void updateNodeToLabelsMappings(
      Map<NodeId, Set<String>> nodeToLabels) throws IOException {
    writeNewMirror();
  }

  @Override
  public void storeNewClusterNodeLabels(List<NodeLabel> labels)
      throws IOException {
    writeNewMirror();
  }

  @Override
  public void removeClusterNodeLabels(Collection<String> labels)
      throws IOException {
    writeNewMirror();
  }

  private void writeNewMirror() throws IOException {
    ReentrantReadWriteLock.ReadLock readLock = manager.readLock;
    // Acquire readlock to make sure we get cluster node labels and
    // node-to-labels mapping atomically.
    readLock.lock();
    try {
      // Write mirror to mirror.new.tmp file
      Path newTmpPath = new Path(fsWorkingPath, MIRROR_FILENAME + ".new.tmp");
      try (FSDataOutputStream os = fs.create(newTmpPath, true)) {
        StoreOp op = FSStoreOpHandler.getMirrorOp(getStoreType());
        op.write(os, manager);
      }
      
      // Rename mirror.new.tmp to mirror.new (will remove .new if it's existed)
      Path newPath = new Path(fsWorkingPath, MIRROR_FILENAME + ".new"); 
      fs.delete(newPath, false);
      fs.rename(newTmpPath, newPath);
      
      // Remove existing mirror and rename mirror.new to mirror
      Path mirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME);
      fs.delete(mirrorPath, false);
      fs.rename(newPath, mirrorPath);
    } finally {
      readLock.unlock();
    }
  }
}
