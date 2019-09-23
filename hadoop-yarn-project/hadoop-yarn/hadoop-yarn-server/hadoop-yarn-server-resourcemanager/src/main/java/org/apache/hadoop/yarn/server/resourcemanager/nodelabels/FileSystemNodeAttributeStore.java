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
package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.NodeAttributeStore;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.nodelabels.store.AbstractFSNodeStore;
import org.apache.hadoop.yarn.nodelabels.store.FSStoreOpHandler;
import org.apache.hadoop.yarn.nodelabels.store.op.AddNodeToAttributeLogOp;
import org.apache.hadoop.yarn.nodelabels.store.op.RemoveNodeToAttributeLogOp;
import org.apache.hadoop.yarn.nodelabels.store.op.ReplaceNodeToAttributeLogOp;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;

import java.io.IOException;
import java.util.List;

/**
 * File system node attribute implementation.
 */
public class FileSystemNodeAttributeStore
    extends AbstractFSNodeStore<NodeAttributesManager>
    implements NodeAttributeStore {

  protected static final Logger LOG =
      LoggerFactory.getLogger(FileSystemNodeAttributeStore.class);

  protected static final String DEFAULT_DIR_NAME = "node-attribute";
  protected static final String MIRROR_FILENAME = "nodeattribute.mirror";
  protected static final String EDITLOG_FILENAME = "nodeattribute.editlog";

  public FileSystemNodeAttributeStore() {
    super(FSStoreOpHandler.StoreType.NODE_ATTRIBUTE);
  }

  private String getDefaultFSNodeAttributeRootDir() throws IOException {
    // default is in local: /tmp/hadoop-yarn-${user}/node-attribute/
    return "file:///tmp/hadoop-yarn-" + UserGroupInformation.getCurrentUser()
        .getShortUserName() + "/" + DEFAULT_DIR_NAME;
  }

  @Override
  public void init(Configuration conf, NodeAttributesManager mgr)
      throws Exception {
    StoreSchema schema = new StoreSchema(EDITLOG_FILENAME, MIRROR_FILENAME);
    initStore(conf, new Path(
        conf.get(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_ROOT_DIR,
            getDefaultFSNodeAttributeRootDir())), schema, mgr);
  }

  @Override
  public void replaceNodeAttributes(List<NodeToAttributes> nodeToAttribute)
      throws IOException {
    ReplaceNodeToAttributeLogOp op = new ReplaceNodeToAttributeLogOp();
    writeToLog(op.setAttributes(nodeToAttribute));
  }

  @Override
  public void addNodeAttributes(List<NodeToAttributes> nodeAttributeMapping)
      throws IOException {
    AddNodeToAttributeLogOp op = new AddNodeToAttributeLogOp();
    writeToLog(op.setAttributes(nodeAttributeMapping));
  }

  @Override
  public void removeNodeAttributes(List<NodeToAttributes> nodeAttributeMapping)
      throws IOException {
    RemoveNodeToAttributeLogOp op = new RemoveNodeToAttributeLogOp();
    writeToLog(op.setAttributes(nodeAttributeMapping));
  }

  @Override
  public void recover() throws IOException, YarnException {
    super.recoverFromStore();
  }

  @Override
  public void close() throws IOException {
    super.closeFSStore();
  }
}
