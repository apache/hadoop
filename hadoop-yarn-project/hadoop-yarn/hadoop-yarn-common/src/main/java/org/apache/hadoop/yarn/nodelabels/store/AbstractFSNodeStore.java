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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.nodelabels.store.op.AddClusterLabelOp;
import org.apache.hadoop.yarn.nodelabels.store.op.NodeToLabelOp;
import org.apache.hadoop.yarn.nodelabels.store.op.RemoveClusterLabelOp;
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
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract class for File System based store.
 *
 * @param <M> manager filesystem store.Currently nodelabel will use
 *           CommonNodeLabelManager.
 */
public abstract class AbstractFSNodeStore<M> {

  /**
   * Lightweight state class for merging mirror and edit log.
   * Used to collect final state without triggering manager events.
   */
  protected static class NodeLabelMergeState {
    private Map<String, NodeLabel> labels = new HashMap<>();
    private Map<NodeId, Set<String>> nodeToLabels = new HashMap<>();
    private boolean centralizedConfig = true;

    void addLabel(NodeLabel label) {
      labels.put(label.getName(), label);
    }

    void removeLabel(String labelName) {
      labels.remove(labelName);
    }

    void replaceNodeToLabels(Map<NodeId, Set<String>> newNodeToLabels) {
      for (Map.Entry<NodeId, Set<String>> entry : newNodeToLabels.entrySet()) {
        this.nodeToLabels.put(entry.getKey(), entry.getValue());
      }
    }

    List<NodeLabel> getClusterNodeLabels() {
      return new ArrayList<>(labels.values());
    }

    void setNodeLabels(Map<NodeId, Set<String>> newNodeToLabels) {
      this.nodeToLabels = newNodeToLabels;
    }

    Map<NodeId, Set<String>> getNodeLabels() {
      return nodeToLabels;
    }

    boolean isCentralizedConfiguration() {
      return centralizedConfig;
    }
  }

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

  /**
   * Merge mirror and edit log into a new mirror file.
   * This reads from mirror (or mirror.old), then applies edit log operations,
   * and writes the result back to mirror.
   * No events are triggered during this process.
   */
  protected void mergeMirrorAndEditLog() throws IOException {
    Path mirrorPath = new Path(fsWorkingPath, schema.mirrorName);
    Path oldMirrorPath = new Path(fsWorkingPath, schema.mirrorName + ".old");
    editLogPath = new Path(fsWorkingPath, schema.editLogName);

    // Create merge state to collect final result
    NodeLabelMergeState mergeState = new NodeLabelMergeState();

    // Parse mirror file to get initial labels and node-to-labels
    Path mirrorToRead = fs.exists(mirrorPath) ? mirrorPath :
        fs.exists(oldMirrorPath) ? oldMirrorPath : null;
    if (mirrorToRead != null) {
      parseMirrorToState(mirrorToRead, mergeState);
    }

    // Parse edit log and apply operations to state
    if (fs.exists(editLogPath)) {
      parseEditLogToState(editLogPath, mergeState);
    }

    // Write new mirror using merge state
    try (FSDataOutputStream os = fs.create(mirrorPath, true)) {
      writeMirrorFromState(os, mergeState);
    }

    // Create new empty editlog file
    editlogOs = fs.create(editLogPath, true);
    editlogOs.close();
    editlogOs = null;

    LOG.info("Merged mirror and edit log to: " + mirrorPath);
  }

  /**
   * Parse mirror file and populate merge state.
   */
  private void parseMirrorToState(Path mirrorPath, NodeLabelMergeState state)
      throws IOException {
    try (FSDataInputStream is = fs.open(mirrorPath)) {
      // Parse cluster node labels
      List<NodeLabel> labels = new AddToClusterNodeLabelsRequestPBImpl(
          YarnServerResourceManagerServiceProtos
              .AddToClusterNodeLabelsRequestProto
              .parseDelimitedFrom(is)).getNodeLabels();
      for (NodeLabel label : labels) {
        state.addLabel(label);
      }

      // Parse node-to-labels if exists (check if more data available)
      try {
        Map<NodeId, Set<String>> nodeToLabels = new ReplaceLabelsOnNodeRequestPBImpl(
            YarnServerResourceManagerServiceProtos
                .ReplaceLabelsOnNodeRequestProto
                .parseDelimitedFrom(is)).getNodeToLabels();
        state.setNodeLabels(nodeToLabels);
      } catch (EOFException e) {
        // No node-to-labels section, that's ok
      }
    }
  }

  /**
   * Parse edit log and apply operations to merge state.
   */
  private void parseEditLogToState(Path editLogFilePath, NodeLabelMergeState state)
      throws IOException {
    try (FSDataInputStream is = fs.open(editLogFilePath)) {
      while (true) {
        try {
          int opCode = is.readInt();
          if (opCode == AddClusterLabelOp.OPCODE) {
            List<NodeLabel> labels = new AddToClusterNodeLabelsRequestPBImpl(
                YarnServerResourceManagerServiceProtos
                    .AddToClusterNodeLabelsRequestProto
                    .parseDelimitedFrom(is)).getNodeLabels();
            for (NodeLabel label : labels) {
              state.addLabel(label);
            }
          } else if (opCode == RemoveClusterLabelOp.OPCODE) {
            Set<String> labelsToRemove = new HashSet<>(
                YarnServerResourceManagerServiceProtos
                    .RemoveFromClusterNodeLabelsRequestProto
                    .parseDelimitedFrom(is).getNodeLabelsList());
            for (String labelName : labelsToRemove) {
              state.removeLabel(labelName);
            }
          } else if (opCode == NodeToLabelOp.OPCODE) {
            Map<NodeId, Set<String>> nodeToLabels = new ReplaceLabelsOnNodeRequestPBImpl(
                YarnServerResourceManagerServiceProtos
                    .ReplaceLabelsOnNodeRequestProto
                    .parseDelimitedFrom(is)).getNodeToLabels();
            state.replaceNodeToLabels(nodeToLabels);
          }
        } catch (EOFException e) {
          break;
        }
      }
    }
  }

  /**
   * Write mirror file from merge state.
   */
  private void writeMirrorFromState(FSDataOutputStream os,
      NodeLabelMergeState state) throws IOException {
    // Write cluster node labels
    ((AddToClusterNodeLabelsRequestPBImpl)
        AddToClusterNodeLabelsRequest.newInstance(state.getClusterNodeLabels()))
        .getProto().writeDelimitedTo(os);

    // Write node-to-labels if centralized config, filtering out labels
    // that no longer exist in the cluster
    if (state.isCentralizedConfiguration()) {
      Map<NodeId, Set<String>> filteredNodeToLabels = new HashMap<>();
      Set<String> validLabels = new HashSet<>();
      for (NodeLabel label : state.getClusterNodeLabels()) {
        validLabels.add(label.getName());
      }
      for (Map.Entry<NodeId, Set<String>> entry : state.getNodeLabels().entrySet()) {
        Set<String> filteredLabels = new HashSet<>();
        for (String label : entry.getValue()) {
          if (validLabels.contains(label)) {
            filteredLabels.add(label);
          }
        }
        if (!filteredLabels.isEmpty()) {
          filteredNodeToLabels.put(entry.getKey(), filteredLabels);
        }
      }
      ((ReplaceLabelsOnNodeRequestPBImpl)
          ReplaceLabelsOnNodeRequest.newInstance(filteredNodeToLabels))
          .getProto().writeDelimitedTo(os);
    }
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
