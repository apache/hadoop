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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.InnerNode;
import org.apache.hadoop.net.InnerNodeImpl;

/**
 * An InnerNode implementation to support weighted choosing.
 * <p>
 * The core modification of this class is to override the getNodeCount method
 * to return the weight of the node.
 * </p>
 *
 * @see DFSNetworkTopologyWithWeight
 */
public class DFSTopologyNodeImplWithWeight extends DFSTopologyNodeImpl {

  static final class Factory extends InnerNodeImpl.Factory {

    private final DataNodeWeightSupplier weightSupplier;

    public Factory(DataNodeWeightSupplier weightSupplier) {
      this.weightSupplier = weightSupplier;
    }

    @Override
    public InnerNodeImpl newInnerNode(String path) {
      return new DFSTopologyNodeImplWithWeight(path, weightSupplier);
    }
  }

  private final DataNodeWeightSupplier weightSupplier;

  public DFSTopologyNodeImplWithWeight(String path, DataNodeWeightSupplier weightSupplier) {
    super(path);
    this.weightSupplier = weightSupplier;
  }

  public DFSTopologyNodeImplWithWeight(String name, String location, InnerNode parent, int level,
      DataNodeWeightSupplier weightSupplier) {
    super(name, location, parent, level);
    this.weightSupplier = weightSupplier;
  }

  @Override
  protected DFSTopologyNodeImpl createParentNode(String parentName) {
    return new DFSTopologyNodeImplWithWeight(
        parentName, getPath(this), this, this.getLevel() + 1, weightSupplier);
  }

  @Override
  protected int getNodeCount(DatanodeDescriptor dn) {
    return weightSupplier.resolve(dn);
  }

}