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

export default AbstractAdapter.extend({

  address: "localBaseAddress",
  restNameSpace: "node",
  serverName: "NM",

  urlForQuery(query) {
    var url = this._buildURL();
    url = url.replace("{nodeAddress}", query.nodeHttpAddr) + "/containers";
    return url;
  },

  urlForQueryRecord(query) {
    var url = this._buildURL();
    url = url.replace("{nodeAddress}", query.nodeHttpAddr) + "/containers/" + query.containerId;
    return url;
  },

  query: function (store, type, query) {
    var url = this.urlForQuery(query);
    // Query params not required.
    query = null;
    return this.ajax(url, 'GET', { data: query });
  },

  queryRecord: function (store, type, query) {
    var url = this.urlForQueryRecord(query);
    // Query params not required.
    query = null;
    console.log(url);
    return this.ajax(url, 'GET', { data: query });
  },

});
