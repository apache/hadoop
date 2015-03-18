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
package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoResponse;

/**
 * <p>The protocol between an <code>TimelineAggregatorsCollection</code> and a 
 * <code>NodeManager</code> to report a new application aggregator get launched.
 * </p>
 * 
 */
@Private
public interface AggregatorNodemanagerProtocol {

  /**
   * 
   * <p>
   * The <code>TimelineAggregatorsCollection</code> provides a list of mapping
   * between application and aggregator's address in 
   * {@link ReportNewAggregatorsInfoRequest} to a <code>NodeManager</code> to
   * <em>register</em> aggregator's info, include: applicationId and REST URI to 
   * access aggregator. NodeManager will add them into registered aggregators 
   * and register them into <code>ResourceManager</code> afterwards.
   * </p>
   * 
   * @param request the request of registering a new aggregator or a list of aggregators
   * @return 
   * @throws YarnException
   * @throws IOException
   */
  ReportNewAggregatorsInfoResponse reportNewAggregatorInfo(
      ReportNewAggregatorsInfoRequest request)
      throws YarnException, IOException;
  
}
