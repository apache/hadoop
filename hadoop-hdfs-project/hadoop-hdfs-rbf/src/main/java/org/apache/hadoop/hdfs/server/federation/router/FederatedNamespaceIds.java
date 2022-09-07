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

package org.apache.hadoop.hdfs.server.federation.router;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdfs.NamespaceStateId;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterFederatedStateProto;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.thirdparty.protobuf.ByteString;


/**
 * Collection of last-seen namespace state Ids for a set of namespaces.
 * A single NamespaceStateId is shared by all outgoing connections to a particular namespace.
 * Router clients share and query the entire collection.
 */
public class FederatedNamespaceIds {
  private final ConcurrentHashMap<String, NamespaceStateId> namespaceIdMap =
      new ConcurrentHashMap<>();

  public void setResponseHeaderState(RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder) {
    if (namespaceIdMap.isEmpty()) {
      return;
    }
    RouterFederatedStateProto.Builder federatedStateBuilder =
        RouterFederatedStateProto.newBuilder();
    namespaceIdMap.forEach((k, v) -> federatedStateBuilder.putNamespaceStateIds(k, v.get()));
    headerBuilder.setRouterFederatedState(federatedStateBuilder.build().toByteString());
  }

  public NamespaceStateId getNamespaceId(String nsId) {
    return namespaceIdMap.computeIfAbsent(nsId, key -> new NamespaceStateId());
  }

  public void removeNamespaceId(String nsId) {
    namespaceIdMap.remove(nsId);
  }

  /**
   * Utility function to parse routerFederatedState field in RPC headers.
   */
  public static Map<String, Long> getRouterFederatedStateMap(ByteString byteString) {
    if (byteString != null) {
      RouterFederatedStateProto federatedState;
      try {
        federatedState = RouterFederatedStateProto.parseFrom(byteString);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      return federatedState.getNamespaceStateIdsMap();
    } else {
      return Collections.emptyMap();
    }
  }

  public int size() {
    return namespaceIdMap.size();
  }
}
