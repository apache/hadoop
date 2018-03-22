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

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerUpdateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerUpdateRequestProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>An implementation of <code>ContainerUpdateRequest</code>.</p>
 *
 * @see ContainerUpdateRequest
 */
@Private
@Unstable
public class ContainerUpdateRequestPBImpl extends ContainerUpdateRequest {
  private ContainerUpdateRequestProto proto =
      ContainerUpdateRequestProto.getDefaultInstance();
  private ContainerUpdateRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private List<Token> containersToUpdate = null;

  public ContainerUpdateRequestPBImpl() {
    builder = ContainerUpdateRequestProto.newBuilder();
  }

  public ContainerUpdateRequestPBImpl(ContainerUpdateRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<Token> getContainersToUpdate() {
    if (containersToUpdate != null) {
      return containersToUpdate;
    }
    ContainerUpdateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<TokenProto> list = p.getUpdateContainerTokenList();
    containersToUpdate = new ArrayList<>();
    for (TokenProto c : list) {
      containersToUpdate.add(convertFromProtoFormat(c));
    }
    return containersToUpdate;
  }

  @Override
  public void setContainersToUpdate(List<Token> containersToUpdate) {
    maybeInitBuilder();
    if (containersToUpdate == null) {
      builder.clearUpdateContainerToken();
    }
    this.containersToUpdate = containersToUpdate;
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

  public ContainerUpdateRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private Token convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerUpdateRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.containersToUpdate != null) {
      addUpdateContainersToProto();
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

  private void addUpdateContainersToProto() {
    maybeInitBuilder();
    builder.clearUpdateContainerToken();
    if (this.containersToUpdate == null) {
      return;
    }
    Iterable<TokenProto> iterable = new Iterable<TokenProto>() {
      @Override
      public Iterator<TokenProto> iterator() {
        return new Iterator<TokenProto>() {
          private Iterator<Token> iter = containersToUpdate.iterator();

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
    builder.addAllUpdateContainerToken(iterable);
  }
}
