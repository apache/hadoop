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

import static org.apache.hadoop.yarn.nodelabels.store.FSStoreOpHandler.StoreType.NODE_ATTRIBUTE;
import static org.apache.hadoop.yarn.nodelabels.store.FSStoreOpHandler.StoreType.NODE_LABEL_STORE;
import org.apache.hadoop.yarn.nodelabels.store.op.AddClusterLabelOp;
import org.apache.hadoop.yarn.nodelabels.store.op.AddNodeToAttributeLogOp;
import org.apache.hadoop.yarn.nodelabels.store.op.FSNodeStoreLogOp;
import org.apache.hadoop.yarn.nodelabels.store.op.NodeAttributeMirrorOp;
import org.apache.hadoop.yarn.nodelabels.store.op.NodeLabelMirrorOp;
import org.apache.hadoop.yarn.nodelabels.store.op.NodeToLabelOp;
import org.apache.hadoop.yarn.nodelabels.store.op.RemoveClusterLabelOp;
import org.apache.hadoop.yarn.nodelabels.store.op.RemoveNodeToAttributeLogOp;
import org.apache.hadoop.yarn.nodelabels.store.op.ReplaceNodeToAttributeLogOp;

import java.util.HashMap;
import java.util.Map;

/**
 * File system store op handler.
 */
public class FSStoreOpHandler {

  private static Map<StoreType, Map<Integer, Class<? extends FSNodeStoreLogOp>>>
      editLogOp;
  private static Map<StoreType, Class<? extends FSNodeStoreLogOp>> mirrorOp;

  /**
   * Store Type enum to hold label and attribute.
   */
  public enum StoreType {
    NODE_LABEL_STORE,
    NODE_ATTRIBUTE
  }

  static {
    editLogOp = new HashMap<>();
    mirrorOp = new HashMap<>();

    // registerLog edit log operation

    //Node Label Operations
    registerLog(NODE_LABEL_STORE, AddClusterLabelOp.OPCODE,
        AddClusterLabelOp.class);
    registerLog(NODE_LABEL_STORE, NodeToLabelOp.OPCODE, NodeToLabelOp.class);
    registerLog(NODE_LABEL_STORE, RemoveClusterLabelOp.OPCODE,
        RemoveClusterLabelOp.class);

    //NodeAttribute operation
    registerLog(NODE_ATTRIBUTE, AddNodeToAttributeLogOp.OPCODE,
        AddNodeToAttributeLogOp.class);
    registerLog(NODE_ATTRIBUTE, RemoveNodeToAttributeLogOp.OPCODE,
        RemoveNodeToAttributeLogOp.class);
    registerLog(NODE_ATTRIBUTE, ReplaceNodeToAttributeLogOp.OPCODE,
        ReplaceNodeToAttributeLogOp.class);

    // registerLog Mirror op

    // Node label mirror operation
    registerMirror(NODE_LABEL_STORE, NodeLabelMirrorOp.class);
    //Node attribute mirror operation
    registerMirror(NODE_ATTRIBUTE, NodeAttributeMirrorOp.class);

  }

  private static void registerMirror(StoreType type,
      Class<? extends FSNodeStoreLogOp> clazz) {
    mirrorOp.put(type, clazz);
  }

  private static void registerLog(StoreType type, int opcode,
      Class<? extends FSNodeStoreLogOp> clazz) {
    Map<Integer, Class<? extends FSNodeStoreLogOp>> ops = editLogOp.get(type);
    Integer code = Integer.valueOf(opcode);
    if (ops == null) {
      Map<Integer, Class<? extends FSNodeStoreLogOp>> newOps = new HashMap<>();
      newOps.put(code, clazz);
      editLogOp.put(type, newOps);
    } else {
      ops.put(code, clazz);
    }
  }

  /**
   * Get mirror operation of store Type.
   *
   * @param storeType storeType.
   * @return instance of FSNodeStoreLogOp.
   */
  public static FSNodeStoreLogOp getMirrorOp(StoreType storeType) {
    return newInstance(mirrorOp.get(storeType));
  }

  /**
   * Will return StoreOp instance based on opCode and StoreType.
   * @param opCode opCode.
   * @param storeType storeType.
   * @return instance of FSNodeStoreLogOp.
   */
  public static FSNodeStoreLogOp get(int opCode, StoreType storeType) {
    return newInstance(editLogOp.get(storeType).get(opCode));
  }

  private static <T extends FSNodeStoreLogOp> T newInstance(Class<T> clazz) {
    FSNodeStoreLogOp instance = null;
    if (clazz != null) {
      try {
        instance = clazz.newInstance();
      } catch (Exception ex) {
        throw new RuntimeException("Failed to instantiate " + clazz, ex);
      }
    }
    return (T) instance;
  }
}
