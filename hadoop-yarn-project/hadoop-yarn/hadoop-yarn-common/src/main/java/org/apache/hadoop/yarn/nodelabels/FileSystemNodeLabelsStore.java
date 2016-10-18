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

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;

import com.google.common.collect.Sets;

public class FileSystemNodeLabelsStore extends NodeLabelsStore {
  protected static final Log LOG = LogFactory.getLog(FileSystemNodeLabelsStore.class);

  protected static final String DEFAULT_DIR_NAME = "node-labels";
  protected static final String MIRROR_FILENAME = "nodelabel.mirror";
  protected static final String EDITLOG_FILENAME = "nodelabel.editlog";
  
  protected enum SerializedLogType {
    ADD_LABELS, NODE_TO_LABELS, REMOVE_LABELS
  }

  Path fsWorkingPath;
  FileSystem fs;
  private FSDataOutputStream editlogOs;
  private Path editLogPath;
  
  private String getDefaultFSNodeLabelsRootDir() throws IOException {
    // default is in local: /tmp/hadoop-yarn-${user}/node-labels/
    return "file:///tmp/hadoop-yarn-"
        + UserGroupInformation.getCurrentUser().getShortUserName() + "/"
        + DEFAULT_DIR_NAME;
  }

  @Override
  public void init(Configuration conf) throws Exception {
    fsWorkingPath =
        new Path(conf.get(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
            getDefaultFSNodeLabelsRootDir()));

    setFileSystem(conf);

    // mkdir of root dir path
    if (!fs.exists(fsWorkingPath)) {
      fs.mkdirs(fsWorkingPath);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(LOG, fs, editlogOs);
  }

  void setFileSystem(Configuration conf) throws IOException {
    Configuration confCopy = new Configuration(conf);
    fs = fsWorkingPath.getFileSystem(confCopy);

    // if it's local file system, use RawLocalFileSystem instead of
    // LocalFileSystem, the latter one doesn't support append.
    if (fs.getScheme().equals("file")) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
  }
  
  private void ensureAppendEditlogFile() throws IOException {
    editlogOs = fs.append(editLogPath);
  }
  
  private void ensureCloseEditlogFile() throws IOException {
    editlogOs.close();
  }

  @Override
  public void updateNodeToLabelsMappings(
      Map<NodeId, Set<String>> nodeToLabels) throws IOException {
    try {
      ensureAppendEditlogFile();
      editlogOs.writeInt(SerializedLogType.NODE_TO_LABELS.ordinal());
      ((ReplaceLabelsOnNodeRequestPBImpl) ReplaceLabelsOnNodeRequest
          .newInstance(nodeToLabels)).getProto().writeDelimitedTo(editlogOs);
    } finally {
      ensureCloseEditlogFile();
    }
  }

  @Override
  public void storeNewClusterNodeLabels(List<NodeLabel> labels)
      throws IOException {
    try {
      ensureAppendEditlogFile();
      editlogOs.writeInt(SerializedLogType.ADD_LABELS.ordinal());
      ((AddToClusterNodeLabelsRequestPBImpl) AddToClusterNodeLabelsRequest
          .newInstance(labels)).getProto().writeDelimitedTo(editlogOs);
    } finally {
      ensureCloseEditlogFile();
    }
  }

  @Override
  public void removeClusterNodeLabels(Collection<String> labels)
      throws IOException {
    try {
      ensureAppendEditlogFile();
      editlogOs.writeInt(SerializedLogType.REMOVE_LABELS.ordinal());
      ((RemoveFromClusterNodeLabelsRequestPBImpl) RemoveFromClusterNodeLabelsRequest.newInstance(Sets
          .newHashSet(labels.iterator()))).getProto().writeDelimitedTo(editlogOs);
    } finally {
      ensureCloseEditlogFile();
    }
  }
  
  protected void loadFromMirror(Path newMirrorPath, Path oldMirrorPath)
      throws IOException {
    // If mirror.new exists, read from mirror.new,
    FSDataInputStream is = null;
    if (fs.exists(newMirrorPath)) {
      is = fs.open(newMirrorPath);
    } else if (fs.exists(oldMirrorPath)) {
      is = fs.open(oldMirrorPath);
    }

    if (null != is) {
      List<NodeLabel> labels = new AddToClusterNodeLabelsRequestPBImpl(
          AddToClusterNodeLabelsRequestProto.parseDelimitedFrom(is))
              .getNodeLabels();
      mgr.addToCluserNodeLabels(labels);

      if (mgr.isCentralizedConfiguration()) {
        // Only load node to labels mapping while using centralized configuration
        Map<NodeId, Set<String>> nodeToLabels =
            new ReplaceLabelsOnNodeRequestPBImpl(
                ReplaceLabelsOnNodeRequestProto.parseDelimitedFrom(is))
                  .getNodeToLabels();
        mgr.replaceLabelsOnNode(nodeToLabels);
      }
      is.close();
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.nodelabels.NodeLabelsStore#recover(boolean)
   */
  @Override
  public void recover() throws YarnException,
      IOException {
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
    Path mirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME);
    Path oldMirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME + ".old");
    
    loadFromMirror(mirrorPath, oldMirrorPath);

    // Open and process editlog
    editLogPath = new Path(fsWorkingPath, EDITLOG_FILENAME);
    if (fs.exists(editLogPath)) {
      FSDataInputStream is = fs.open(editLogPath);

      while (true) {
        try {
          // read edit log one by one
          SerializedLogType type = SerializedLogType.values()[is.readInt()];
          
          switch (type) {
          case ADD_LABELS: {
            List<NodeLabel> labels =
                new AddToClusterNodeLabelsRequestPBImpl(
                    AddToClusterNodeLabelsRequestProto.parseDelimitedFrom(is))
                    .getNodeLabels();
            mgr.addToCluserNodeLabels(labels);
            break;
          }
          case REMOVE_LABELS: {
            Collection<String> labels =
                RemoveFromClusterNodeLabelsRequestProto.parseDelimitedFrom(is)
                    .getNodeLabelsList();
            mgr.removeFromClusterNodeLabels(labels);
            break;
          }
          case NODE_TO_LABELS: {
            Map<NodeId, Set<String>> map =
                new ReplaceLabelsOnNodeRequestPBImpl(
                    ReplaceLabelsOnNodeRequestProto.parseDelimitedFrom(is))
                    .getNodeToLabels();
            if (mgr.isCentralizedConfiguration()) {
              /*
               * In case of Distributed NodeLabels setup,
               * ignoreNodeToLabelsMappings will be set to true and recover will
               * be invoked. As RM will collect the node labels from NM through
               * registration/HB
               */
              mgr.replaceLabelsOnNode(map);
            }
            break;
          }
          }
        } catch (EOFException e) {
          // EOF hit, break
          break;
        }
      }
    }

    // Serialize current mirror to mirror.writing
    Path writingMirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME + ".writing");
    FSDataOutputStream os = fs.create(writingMirrorPath, true);
    ((AddToClusterNodeLabelsRequestPBImpl) AddToClusterNodeLabelsRequestPBImpl
        .newInstance(mgr.getClusterNodeLabels())).getProto().writeDelimitedTo(os);
    ((ReplaceLabelsOnNodeRequestPBImpl) ReplaceLabelsOnNodeRequest
        .newInstance(mgr.getNodeLabels())).getProto().writeDelimitedTo(os);
    os.close();
    
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
}
