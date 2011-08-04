package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueACLProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueUserACLInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueUserACLInfoProtoOrBuilder;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class QueueUserACLInfoPBImpl extends ProtoBase<QueueUserACLInfoProto> 
implements QueueUserACLInfo {

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

  @Override
  public QueueUserACLInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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
