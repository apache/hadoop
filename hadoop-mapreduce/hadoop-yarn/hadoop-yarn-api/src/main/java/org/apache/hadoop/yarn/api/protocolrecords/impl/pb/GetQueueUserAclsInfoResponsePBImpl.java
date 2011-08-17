package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.QueueUserACLInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueUserACLInfoProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoResponseProtoOrBuilder;

public class GetQueueUserAclsInfoResponsePBImpl extends 
ProtoBase<GetQueueUserAclsInfoResponseProto> 
implements GetQueueUserAclsInfoResponse {

  List<QueueUserACLInfo> queueUserAclsInfoList;

  GetQueueUserAclsInfoResponseProto proto = 
    GetQueueUserAclsInfoResponseProto.getDefaultInstance();
  GetQueueUserAclsInfoResponseProto.Builder builder = null;
  boolean viaProto = false;

  public GetQueueUserAclsInfoResponsePBImpl() {
    builder = GetQueueUserAclsInfoResponseProto.newBuilder();
  }
  
  public GetQueueUserAclsInfoResponsePBImpl(
      GetQueueUserAclsInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<QueueUserACLInfo> getUserAclsInfoList() {
    initLocalQueueUserAclsList();
    return queueUserAclsInfoList;
  }

  @Override
  public void setUserAclsInfoList(List<QueueUserACLInfo> queueUserAclsList) {
    if (queueUserAclsList == null) {
      builder.clearQueueUserAcls();
    }
    this.queueUserAclsInfoList = queueUserAclsList;
  }

  @Override
  public GetQueueUserAclsInfoResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.queueUserAclsInfoList != null) {
      addLocalQueueUserACLInfosToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetQueueUserAclsInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  //Once this is called. containerList will never be null - untill a getProto is called.
  private void initLocalQueueUserAclsList() {
    if (this.queueUserAclsInfoList != null) {
      return;
    }
    GetQueueUserAclsInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<QueueUserACLInfoProto> list = p.getQueueUserAclsList();
    queueUserAclsInfoList = new ArrayList<QueueUserACLInfo>();

    for (QueueUserACLInfoProto a : list) {
      queueUserAclsInfoList.add(convertFromProtoFormat(a));
    }
  }

  private void addLocalQueueUserACLInfosToProto() {
    maybeInitBuilder();
    builder.clearQueueUserAcls();
    if (queueUserAclsInfoList == null)
      return;
    Iterable<QueueUserACLInfoProto> iterable = new Iterable<QueueUserACLInfoProto>() {
      @Override
      public Iterator<QueueUserACLInfoProto> iterator() {
        return new Iterator<QueueUserACLInfoProto>() {

          Iterator<QueueUserACLInfo> iter = queueUserAclsInfoList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public QueueUserACLInfoProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllQueueUserAcls(iterable);
  }

  private QueueUserACLInfoPBImpl convertFromProtoFormat(QueueUserACLInfoProto p) {
    return new QueueUserACLInfoPBImpl(p);
  }

  private QueueUserACLInfoProto convertToProtoFormat(QueueUserACLInfo t) {
    return ((QueueUserACLInfoPBImpl)t).getProto();
  }

}
