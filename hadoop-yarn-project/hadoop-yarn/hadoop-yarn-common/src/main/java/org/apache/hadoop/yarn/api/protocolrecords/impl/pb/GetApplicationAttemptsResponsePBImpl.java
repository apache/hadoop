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
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GetApplicationAttemptsResponsePBImpl extends
    GetApplicationAttemptsResponse {

  GetApplicationAttemptsResponseProto proto =
      GetApplicationAttemptsResponseProto.getDefaultInstance();
  GetApplicationAttemptsResponseProto.Builder builder = null;
  boolean viaProto = false;

  List<ApplicationAttemptReport> applicationAttemptList;

  public GetApplicationAttemptsResponsePBImpl() {
    builder = GetApplicationAttemptsResponseProto.newBuilder();
  }

  public GetApplicationAttemptsResponsePBImpl(
      GetApplicationAttemptsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<ApplicationAttemptReport> getApplicationAttemptList() {
    initLocalApplicationAttemptsList();
    return this.applicationAttemptList;
  }

  @Override
  public void setApplicationAttemptList(
      List<ApplicationAttemptReport> applicationAttempts) {
    maybeInitBuilder();
    if (applicationAttempts == null) {
      builder.clearApplicationAttempts();
    }
    this.applicationAttemptList = applicationAttempts;
  }

  public GetApplicationAttemptsResponseProto getProto() {
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
    if (this.applicationAttemptList != null) {
      addLocalApplicationAttemptsToProto();
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
      builder = GetApplicationAttemptsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // Once this is called. containerList will never be null - until a getProto
  // is called.
  private void initLocalApplicationAttemptsList() {
    if (this.applicationAttemptList != null) {
      return;
    }
    GetApplicationAttemptsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationAttemptReportProto> list = p.getApplicationAttemptsList();
    applicationAttemptList = new ArrayList<ApplicationAttemptReport>();

    for (ApplicationAttemptReportProto a : list) {
      applicationAttemptList.add(convertFromProtoFormat(a));
    }
  }

  private void addLocalApplicationAttemptsToProto() {
    maybeInitBuilder();
    builder.clearApplicationAttempts();
    if (applicationAttemptList == null) {
      return;
    }
    Iterable<ApplicationAttemptReportProto> iterable =
        new Iterable<ApplicationAttemptReportProto>() {
          @Override
          public Iterator<ApplicationAttemptReportProto> iterator() {
            return new Iterator<ApplicationAttemptReportProto>() {

              Iterator<ApplicationAttemptReport> iter = applicationAttemptList
                .iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ApplicationAttemptReportProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();

              }
            };

          }
        };
    builder.addAllApplicationAttempts(iterable);
  }

  private ApplicationAttemptReportPBImpl convertFromProtoFormat(
      ApplicationAttemptReportProto p) {
    return new ApplicationAttemptReportPBImpl(p);
  }

  private ApplicationAttemptReportProto convertToProtoFormat(
      ApplicationAttemptReport t) {
    return ((ApplicationAttemptReportPBImpl) t).getProto();
  }

}
