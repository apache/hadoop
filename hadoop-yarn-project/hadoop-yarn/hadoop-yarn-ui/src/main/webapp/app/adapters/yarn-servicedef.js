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

import RESTAbstractAdapter from './restabstract';

export default RESTAbstractAdapter.extend({
  address: "rmWebAddress",
  restNameSpace: "dashService",
  serverName: "DASH",

  normalizeErrorResponse(status, headers, payload) {
    if (payload && typeof payload === 'object' && payload.errors) {
      return payload.errors;
    } else {
      return [
        payload
      ];
    }
  },

  deployService(request, user) {
    var url = this.buildURL();
    if(user) {
      url += "/?user.name=" + user;
    }
    return this.ajax(url, "POST", {data: request});
  },

  stopService(serviceName, user) {
    var url = this.buildURL();
    url += "/" + serviceName;
    url += "/?user.name=" + user;
    var data = {"state": "STOPPED", "name": serviceName};
    return this.ajax(url, "PUT", {data: data});
  },

  deleteService(serviceName, user) {
    var url = this.buildURL();
    url += "/" + serviceName;
    url += "/?user.name=" + user;
    return this.ajax(url, "DELETE", {data: {}});
  }
});
