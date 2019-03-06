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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.store.AbstractFSNodeStore;

import org.apache.hadoop.yarn.nodelabels.store.op.AddClusterLabelOp;
import org.apache.hadoop.yarn.nodelabels.store.FSStoreOpHandler.StoreType;
import org.apache.hadoop.yarn.nodelabels.store.op.NodeToLabelOp;
import org.apache.hadoop.yarn.nodelabels.store.op.RemoveClusterLabelOp;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FileSystemNodeLabelsStore for storing node labels.
 */
public class FileSystemNodeLabelsStore
    extends AbstractFSNodeStore<CommonNodeLabelsManager>
    implements NodeLabelsStore {
  protected static final Logger LOG =
      LoggerFactory.getLogger(FileSystemNodeLabelsStore.class);

  protected static final String DEFAULT_DIR_NAME = "node-labels";
  protected static final String MIRROR_FILENAME = "nodelabel.mirror";
  protected static final String EDITLOG_FILENAME = "nodelabel.editlog";

  FileSystemNodeLabelsStore() {
    super(StoreType.NODE_LABEL_STORE);
  }

  private String getDefaultFSNodeLabelsRootDir() throws IOException {
    // default is in local: /tmp/hadoop-yarn-${user}/node-labels/
    return "file:///tmp/hadoop-yarn-" + UserGroupInformation.getCurrentUser()
        .getShortUserName() + "/" + DEFAULT_DIR_NAME;
  }

  @Override
  public void init(Configuration conf, CommonNodeLabelsManager mgr)
      throws Exception {
    StoreSchema schema = new StoreSchema(EDITLOG_FILENAME, MIRROR_FILENAME);
    initStore(conf, new Path(
        conf.get(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
            getDefaultFSNodeLabelsRootDir())), schema, mgr);
  }

  @Override
  public void close() throws IOException {
    super.closeFSStore();
  }

  @Override
  public void updateNodeToLabelsMappings(Map<NodeId, Set<String>> nodeToLabels)
      throws IOException {
    NodeToLabelOp op = new NodeToLabelOp();
    writeToLog(op.setNodeToLabels(nodeToLabels));
  }

  @Override
  public void storeNewClusterNodeLabels(List<NodeLabel> labels)
      throws IOException {
    AddClusterLabelOp op = new AddClusterLabelOp();
    writeToLog(op.setLabels(labels));
  }

  @Override
  public void removeClusterNodeLabels(Collection<String> labels)
      throws IOException {
    RemoveClusterLabelOp op = new RemoveClusterLabelOp();
    writeToLog(op.setLabels(labels));
  }

  /* (non-Javadoc)
     * @see org.apache.hadoop.yarn.nodelabels.NodeLabelsStore#recover(boolean)
     */
  @Override
  public void recover() throws YarnException, IOException {
    super.recoverFromStore();
  }
}
