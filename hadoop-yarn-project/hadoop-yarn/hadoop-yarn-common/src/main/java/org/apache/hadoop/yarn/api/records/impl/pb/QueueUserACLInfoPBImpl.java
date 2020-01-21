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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueACLProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueUserACLInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueUserACLInfoProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class QueueUserACLInfoPBImpl extends QueueUserACLInfo {

  QueueUserACLInfoProto proto = QueueUserACLInfoProto.getDefaultInstance();
  QueueUserACLInfoProto.Builder builder = null;
  boolean viaProto = false;

  List<QueueACL> userAclsList;

  public QueueUserACLInfoPBImpl() {
    builder = QueueUserACLInfoProto.newBuilder();
  }
  
  public QueueUserACLInfoPBImpl(QueueUserACLInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public String getQueueName() {
    QueueUserACLInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueName()) ? p.getQueueName() : null;
  }

  @Override
  public List<QueueACL> getUserAcls() {
    initLocalQueueUserAclsList();
    return this.userAclsList;
  }

  @Override
  public void setQueueName(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueueName();
      return;
    }
    builder.setQueueName(queueName);
  }

  @Override
  public void setUserAcls(List<QueueACL> userAclsList) {
    if (userAclsList == null) {
      builder.clearUserAcls();
    }
    this.userAclsList = userAclsList;
  }

  public QueueUserACLInfoProto getProto() {
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
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void initLocalQueueUserAclsList() {
    if (this.userAclsList != null) {
      return;
    }
    QueueUserACLInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<QueueACLProto> list = p.getUserAclsList();
    userAclsList = new ArrayList<QueueACL>();

    for (QueueACLProto a : list) {
      userAclsList.add(convertFromProtoFormat(a));
    }
  }

  private void addQueueACLsToProto() {
    maybeInitBuilder();
    builder.clearUserAcls();
    if (userAclsList == null)
      return;
    Iterable<QueueACLProto> iterable = new Iterable<QueueACLProto>() {
      @Override
      public Iterator<QueueACLProto> iterator() {
        return new Iterator<QueueACLProto>() {
  
          Iterator<QueueACL> iter = userAclsList.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public QueueACLProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllUserAcls(iterable);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueueUserACLInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.userAclsList != null) {
      addQueueACLsToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private QueueACL convertFromProtoFormat(QueueACLProto q) {
    return ProtoUtils.convertFromProtoFormat(q);
  }
  
  private QueueACLProto convertToProtoFormat(QueueACL queueAcl) {
    return ProtoUtils.convertToProtoFormat(queueAcl);
  }

}
