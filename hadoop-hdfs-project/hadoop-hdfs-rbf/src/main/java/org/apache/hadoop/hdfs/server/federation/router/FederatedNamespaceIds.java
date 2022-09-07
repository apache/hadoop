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
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.classification.VisibleForTesting;
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
  private final Map<String, NamespaceStateId> namespaceIdMap = new ConcurrentHashMap<>();
  private final ReentrantLock lock = new ReentrantLock();

  public void updateStateUsingRequestHeader(RpcHeaderProtos.RpcRequestHeaderProto header) {
    if (header.hasRouterFederatedState()) {
      RouterFederatedStateProto federatedState;
      try {
        federatedState = RouterFederatedStateProto.parseFrom(header.getRouterFederatedState());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      lock.lock();
      try {
        federatedState.getNamespaceStateIdsMap().forEach((nsId, stateId) -> {
          if (!namespaceIdMap.containsKey(nsId)) {
            namespaceIdMap.putIfAbsent(nsId, new NamespaceStateId());
          }
          namespaceIdMap.get(nsId).update(stateId);
        });
      } finally {
        lock.unlock();
      }

    }
  }

  public void setResponseHeaderState(RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder) {
    RouterFederatedStateProto.Builder federatedStateBuilder =
        RouterFederatedStateProto.newBuilder();
    lock.lock();
    try {
      namespaceIdMap.forEach((k, v) -> federatedStateBuilder.putNamespaceStateIds(k, v.get()));
    } finally {
      lock.unlock();
    }
    headerBuilder.setRouterFederatedState(federatedStateBuilder.build().toByteString());
  }

  public NamespaceStateId getNamespaceId(String nsId) {
    lock.lock();
    try {
      namespaceIdMap.putIfAbsent(nsId, new NamespaceStateId());
    } finally {
      lock.unlock();
    }
    return namespaceIdMap.get(nsId);
  }

  public void removeNamespaceId(String nsId) {
    lock.lock();
    try {
      namespaceIdMap.remove(nsId);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Utility function to view state of routerFederatedState field in RPC headers.
   */
  @VisibleForTesting
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
}
