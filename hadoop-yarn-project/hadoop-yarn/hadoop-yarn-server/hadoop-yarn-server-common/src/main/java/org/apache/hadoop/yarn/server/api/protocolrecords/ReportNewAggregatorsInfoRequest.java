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
package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.records.AppAggregatorsMap;
import org.apache.hadoop.yarn.util.Records;

@Private
public abstract class ReportNewAggregatorsInfoRequest {
  
  public static ReportNewAggregatorsInfoRequest newInstance(
      List<AppAggregatorsMap> appAggregatorsList) {
    ReportNewAggregatorsInfoRequest request =
        Records.newRecord(ReportNewAggregatorsInfoRequest.class);
    request.setAppAggregatorsList(appAggregatorsList);
    return request;
  }
  
  public static ReportNewAggregatorsInfoRequest newInstance(
      ApplicationId id, String aggregatorAddr) {
    ReportNewAggregatorsInfoRequest request =
        Records.newRecord(ReportNewAggregatorsInfoRequest.class);
    request.setAppAggregatorsList(
        Arrays.asList(AppAggregatorsMap.newInstance(id, aggregatorAddr)));
    return request;
  }
  
  public abstract List<AppAggregatorsMap> getAppAggregatorsList();
  
  public abstract void setAppAggregatorsList(
      List<AppAggregatorsMap> appAggregatorsList);
  
}
