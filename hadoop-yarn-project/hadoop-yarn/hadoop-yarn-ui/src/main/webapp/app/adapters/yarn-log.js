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

import AbstractAdapter from './abstract';

function createEmptyContainerLogInfo(location) {
  return {
    containerLogsInfo: {
      containerLogInfo: [{
        fileName: "",
        fileSize: "",
        lastModifiedTime: "",
        redirectedUrl: location
      }]
    }
  };
}

export default AbstractAdapter.extend({
  address: "timelineWebAddress",
  restNameSpace: "timelineV2Log",
  serverName: "ATS",

  urlForQuery(query/*, modelName*/) {
    var url = this._buildURL();
    var containerId = query['containerId'];
    var clusterId = this.get("env.app.clusterId");
    delete query.containerId;
    return url + '/containers/' + containerId + '/logs' + '?clusterid=' + clusterId + '?manual_redirection=true';
  },

  handleResponse(status, headers, payload, requestData) {
    if (headers['location'] !== undefined && headers['location'] !== null) {
      return createEmptyContainerLogInfo(headers['location']);
    } else {
      return payload;
    }
  }

});
