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
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GetApplicationsResponsePBImpl
extends GetApplicationsResponse {

  GetApplicationsResponseProto proto =
    GetApplicationsResponseProto.getDefaultInstance();
  GetApplicationsResponseProto.Builder builder = null;
  boolean viaProto = false;

  List<ApplicationReport> applicationList;

  public GetApplicationsResponsePBImpl() {
    builder = GetApplicationsResponseProto.newBuilder();
  }

  public GetApplicationsResponsePBImpl(GetApplicationsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<ApplicationReport> getApplicationList() {
    initLocalApplicationsList();
    return this.applicationList;
  }

  @Override
  public void setApplicationList(List<ApplicationReport> applications) {
    maybeInitBuilder();
    if (applications == null)
      builder.clearApplications();
    this.applicationList = applications;
  }

  public GetApplicationsResponseProto getProto() {
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
      builder = GetApplicationsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // Once this is called. containerList will never be null - until a getProto
  // is called.
  private void initLocalApplicationsList() {
    if (this.applicationList != null) {
      return;
    }
    GetApplicationsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationReportProto> list = p.getApplicationsList();
    applicationList = new ArrayList<ApplicationReport>();

    for (ApplicationReportProto a : list) {
      applicationList.add(convertFromProtoFormat(a));
    }
  }

  private void addLocalApplicationsToProto() {
    maybeInitBuilder();
    builder.clearApplications();
    if (applicationList == null)
      return;
    Iterable<ApplicationReportProto> iterable = new Iterable<ApplicationReportProto>() {
      @Override
      public Iterator<ApplicationReportProto> iterator() {
        return new Iterator<ApplicationReportProto>() {

          Iterator<ApplicationReport> iter = applicationList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ApplicationReportProto next() {
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

  private ApplicationReportPBImpl convertFromProtoFormat(ApplicationReportProto p) {
    return new ApplicationReportPBImpl(p);
  }

  private ApplicationReportProto convertToProtoFormat(ApplicationReport t) {
    return ((ApplicationReportPBImpl)t).getProto();
  }

}
