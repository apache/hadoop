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
  restNameSpace: "cluster",
  serverName: "RM",

  urlForQuery(/*query, modelName*/) {
    var url = this._buildURL();
    url = url + '/apps';
    return url;
  },

  urlForFindRecord(id/*, modelName, snapshot*/) {
    var url = this._buildURL();
    url = url + '/apps/' + id;
    return url;
  },

  pathForType(/*modelName*/) {
    return 'apps'; // move to some common place, return path by modelname.
  },

  sendKillApplication(id) {
    var url = this._buildURL();
    url += '/apps/' + id + '/state';
    var data = {
      "state": "KILLED"
    };
    return this.ajax(url, "PUT", { data: data });
  }
});
