package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.records.Application;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllApplicationsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllApplicationsResponseProtoOrBuilder;

public class GetAllApplicationsResponsePBImpl 
extends ProtoBase<GetAllApplicationsResponseProto> implements 
GetAllApplicationsResponse {

  GetAllApplicationsResponseProto proto = 
    GetAllApplicationsResponseProto.getDefaultInstance();
  GetAllApplicationsResponseProto.Builder builder = null;
  boolean viaProto = false;

  List<Application> applicationList;
  
  public GetAllApplicationsResponsePBImpl() {
    builder = GetAllApplicationsResponseProto.newBuilder();
  }
  
  public GetAllApplicationsResponsePBImpl(GetAllApplicationsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<Application> getApplicationList() {    
    initLocalApplicationsList();
    return this.applicationList;
  }

  @Override
  public void setApplicationList(List<Application> applications) {
    maybeInitBuilder();
    if (applications == null) 
      builder.clearApplications();
    this.applicationList = applications;
  }

  @Override
  public GetAllApplicationsResponseProto getProto() {    
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationList != null) {
      addLocalApplicationsToProto();
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
      builder = GetAllApplicationsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  //Once this is called. containerList will never be null - untill a getProto is called.
  private void initLocalApplicationsList() {
    if (this.applicationList != null) {
      return;
    }
    GetAllApplicationsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationProto> list = p.getApplicationsList();
    applicationList = new ArrayList<Application>();

    for (ApplicationProto a : list) {
      applicationList.add(convertFromProtoFormat(a));
    }
  }

  private void addLocalApplicationsToProto() {
    maybeInitBuilder();
    builder.clearApplications();
    if (applicationList == null)
      return;
    Iterable<ApplicationProto> iterable = new Iterable<ApplicationProto>() {
      @Override
      public Iterator<ApplicationProto> iterator() {
        return new Iterator<ApplicationProto>() {

          Iterator<Application> iter = applicationList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ApplicationProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllApplications(iterable);
  }

  private ApplicationPBImpl convertFromProtoFormat(ApplicationProto p) {
    return new ApplicationPBImpl(p);
  }

  private ApplicationProto convertToProtoFormat(Application t) {
    return ((ApplicationPBImpl)t).getProto();
  }

}
