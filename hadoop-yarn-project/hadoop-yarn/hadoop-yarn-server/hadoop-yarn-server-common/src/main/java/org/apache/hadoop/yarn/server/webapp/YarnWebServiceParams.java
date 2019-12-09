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

package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Common web service parameters which could be used in
 * RM/NM/AHS WebService.
 *
 */
@InterfaceAudience.LimitedPrivate({"YARN"})
public interface YarnWebServiceParams {

  // the params used in container-log related web services
  String CONTAINER_ID = "containerid";
  String CONTAINER_LOG_FILE_NAME = "filename";
  String RESPONSE_CONTENT_FORMAT = "format";
  String RESPONSE_CONTENT_SIZE = "size";
  String NM_ID = "nm.id";
  String REDIRECTED_FROM_NODE = "redirected_from_node";
  String CLUSTER_ID = "clusterid";
}
