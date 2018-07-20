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

package org.apache.hadoop.ozone.client.rest;

import org.apache.hadoop.ozone.om.helpers.ServiceInfo;

import java.util.List;

/**
 * The implementor of this interface should select the REST server which will
 * be used by the client to connect to Ozone Cluster, given list of
 * REST Servers/DataNodes (DataNodes are the ones which hosts REST Service).
 */
public interface RestServerSelector {

  /**
   * Returns the REST Service which will be used by the client for connection.
   *
   * @param restServices list of available REST servers
   * @return ServiceInfo
   */
  ServiceInfo getRestServer(List<ServiceInfo> restServices);

}
