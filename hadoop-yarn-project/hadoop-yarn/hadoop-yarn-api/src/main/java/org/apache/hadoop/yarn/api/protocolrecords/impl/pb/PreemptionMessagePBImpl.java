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
package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.PreemptionContract;
import org.apache.hadoop.yarn.api.protocolrecords.PreemptionMessage;
import org.apache.hadoop.yarn.api.protocolrecords.StrictPreemptionContract;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.PreemptionContractProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.PreemptionMessageProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.PreemptionMessageProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StrictPreemptionContractProto;

public class PreemptionMessagePBImpl implements PreemptionMessage {

  PreemptionMessageProto proto = PreemptionMessageProto.getDefaultInstance();
  PreemptionMessageProto.Builder builder = null;

  boolean viaProto = false;
  private StrictPreemptionContract strict;
  private PreemptionContract contract;

  public PreemptionMessagePBImpl() {
    builder = PreemptionMessageProto.newBuilder();
  }

  public PreemptionMessagePBImpl(PreemptionMessageProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized PreemptionMessageProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (strict != null) {
      builder.setStrictContract(convertToProtoFormat(strict));
    }
    if (contract != null) {
      builder.setContract(convertToProtoFormat(contract));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PreemptionMessageProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized StrictPreemptionContract getStrictContract() {
    PreemptionMessageProtoOrBuilder p = viaProto ? proto : builder;
    if (strict != null) {
      return strict;
    }
    if (!p.hasStrictContract()) {
      return null;
    }
    strict = convertFromProtoFormat(p.getStrictContract());
    return strict;
  }

  @Override
  public synchronized void setStrictContract(StrictPreemptionContract strict) {
    maybeInitBuilder();
    if (null == strict) {
      builder.clearStrictContract();
    }
    this.strict = strict;
  }

  @Override
  public synchronized PreemptionContract getContract() {
    PreemptionMessageProtoOrBuilder p = viaProto ? proto : builder;
    if (contract != null) {
      return contract;
    }
    if (!p.hasContract()) {
      return null;
    }
    contract = convertFromProtoFormat(p.getContract());
    return contract;
  }

  @Override
  public synchronized void setContract(final PreemptionContract c) {
    maybeInitBuilder();
    if (null == c) {
      builder.clearContract();
    }
    this.contract = c;
  }

  private StrictPreemptionContractPBImpl convertFromProtoFormat(
      StrictPreemptionContractProto p) {
    return new StrictPreemptionContractPBImpl(p);
  }

  private StrictPreemptionContractProto convertToProtoFormat(
      StrictPreemptionContract t) {
    return ((StrictPreemptionContractPBImpl)t).getProto();
  }

  private PreemptionContractPBImpl convertFromProtoFormat(
      PreemptionContractProto p) {
    return new PreemptionContractPBImpl(p);
  }

  private PreemptionContractProto convertToProtoFormat(
      PreemptionContract t) {
    return ((PreemptionContractPBImpl)t).getProto();
  }

}
