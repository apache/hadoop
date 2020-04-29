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
package org.apache.hadoop.yarn.nodelabels.store.op;

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.nodelabels.store.StoreOp;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines all FileSystem editlog operation. All node label and attribute
 * store write or read operation will be defined in this class.
 *
 * @param <M> Manager used for each operation.
 */
public abstract class FSNodeStoreLogOp<M>
    implements StoreOp<OutputStream, InputStream, M> {

  public abstract int getOpCode();

  protected Map<String, Set<NodeAttribute>> getNodeToAttributesMap(
      NodesToAttributesMappingRequest request) {
    List<NodeToAttributes> attributes = request.getNodesToAttributes();
    Map<String, Set<NodeAttribute>> nodeToAttrMap = new HashMap<>();
    attributes.forEach((v) -> nodeToAttrMap
        .put(v.getNode(), new HashSet<>(v.getNodeAttributes())));
    return nodeToAttrMap;
  }
}
