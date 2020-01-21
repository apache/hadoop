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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncreaseContainersResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncreaseContainersResourceRequestProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class IncreaseContainersResourceRequestPBImpl extends
    IncreaseContainersResourceRequest {
  IncreaseContainersResourceRequestProto proto =
      IncreaseContainersResourceRequestProto.getDefaultInstance();
  IncreaseContainersResourceRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<Token> containersToIncrease = null;

  public IncreaseContainersResourceRequestPBImpl() {
    builder = IncreaseContainersResourceRequestProto.newBuilder();
  }

  public IncreaseContainersResourceRequestPBImpl(
      IncreaseContainersResourceRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public IncreaseContainersResourceRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.containersToIncrease != null) {
      addIncreaseContainersToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = IncreaseContainersResourceRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<Token> getContainersToIncrease() {
    if (containersToIncrease != null) {
      return containersToIncrease;
    }
    IncreaseContainersResourceRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<TokenProto> list = p.getIncreaseContainersList();
    containersToIncrease = new ArrayList<>();
    for (TokenProto c : list) {
      containersToIncrease.add(convertFromProtoFormat(c));
    }
    return containersToIncrease;
  }

  @Override
  public void setContainersToIncrease(List<Token> containersToIncrease) {
    maybeInitBuilder();
    if (containersToIncrease == null) {
      builder.clearIncreaseContainers();
    }
    this.containersToIncrease = containersToIncrease;
  }

  private void addIncreaseContainersToProto() {
    maybeInitBuilder();
    builder.clearIncreaseContainers();
    if (this.containersToIncrease == null) {
      return;
    }
    Iterable<TokenProto> iterable = new Iterable<TokenProto>() {
      @Override
      public Iterator<TokenProto> iterator() {
        return new Iterator<TokenProto>() {
          Iterator<Token> iter = containersToIncrease.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TokenProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllIncreaseContainers(iterable);
  }

  private Token convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }
}
