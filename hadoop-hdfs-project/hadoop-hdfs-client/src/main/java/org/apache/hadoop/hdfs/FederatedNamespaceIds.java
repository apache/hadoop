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

package org.apache.hadoop.hdfs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.ipc.NameServiceStateIdMode;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.NameserviceStateIdProto;


/** Collection of last-seen namespace state Ids for a set of namespaces. */
public class FederatedNamespaceIds {

  private final Map<String, NamespaceStateId> namespaceIdMap = new ConcurrentHashMap<>();
  private NameServiceStateIdMode mode;

  public FederatedNamespaceIds(NameServiceStateIdMode mode) {
    this.mode = mode;
  }

  public void updateStateUsingRequestHeader(RpcRequestHeaderProto header) {
    mode = RpcClientUtil.toStateIdMode(header);
    header.getNameserviceStateIdsContext().getNameserviceStateIdsList()
        .forEach(this::updateNameserviceState);
  }

  public void updateStateUsingResponseHeader(RpcResponseHeaderProto header) {
    header.getNameserviceStateIdsContext().getNameserviceStateIdsList()
        .forEach(this::updateNameserviceState);
  }

  public void updateNameserviceState(NameserviceStateIdProto proto) {
    namespaceIdMap.computeIfAbsent(proto.getNsId(), n -> new NamespaceStateId());
    namespaceIdMap.get(proto.getNsId()).update(proto.getStateId());
  }

  public void updateNameserviceState(String nsId, long stateId) {
    namespaceIdMap.computeIfAbsent(nsId, n -> new NamespaceStateId());
    namespaceIdMap.get(nsId).update(stateId);
  }

  public void setRequestHeaderState(RpcRequestHeaderProto.Builder headerBuilder) {
    headerBuilder.getNameserviceStateIdsContextBuilder()
        .setMode(RpcClientUtil.toNameServiceStateIdModeProto(mode));
    namespaceIdMap.forEach((k, v) -> headerBuilder.getNameserviceStateIdsContextBuilder()
        .addNameserviceStateIds(
            NameserviceStateIdProto.newBuilder()
                .setNsId(k)
                .setStateId(v.get())
                .build())
    );
  }

  public void setRequestHeaderState(RpcRequestHeaderProto.Builder headerBuilder, String nsId) {
    NamespaceStateId namespaceStateId = namespaceIdMap.get(nsId);
    long stateId = (namespaceStateId == null) ? NamespaceStateId.DEFAULT : namespaceStateId.get();
    headerBuilder.setStateId(stateId);
  }

  public void setResponseHeaderState(RpcResponseHeaderProto.Builder headerBuilder) {
    headerBuilder.getNameserviceStateIdsContextBuilder()
        .setMode(RpcClientUtil.toNameServiceStateIdModeProto(mode));
    namespaceIdMap.forEach((k, v) -> headerBuilder.getNameserviceStateIdsContextBuilder()
        .addNameserviceStateIds(
            NameserviceStateIdProto.newBuilder()
                .setNsId(k)
                .setStateId(v.get())
                .build())
    );
  }

  public NamespaceStateId getNamespaceId(String nsId, boolean useDefault) {
    if (useDefault) {
      namespaceIdMap.computeIfAbsent(nsId, n -> new NamespaceStateId());
    }
    return namespaceIdMap.get(nsId);
  }

  public boolean isProxyMode() {
    return mode == NameServiceStateIdMode.PROXY;
  }

  public boolean isTransmissionMode() {
    return mode == NameServiceStateIdMode.TRANSMISSION;
  }

  public boolean isDisable() {
    return mode == NameServiceStateIdMode.DISABLE;
  }

  public NameServiceStateIdMode getMode() {
    return mode;
  }

  public boolean contains(String nsId) {
    return this.namespaceIdMap.containsKey(nsId);
  }
}
