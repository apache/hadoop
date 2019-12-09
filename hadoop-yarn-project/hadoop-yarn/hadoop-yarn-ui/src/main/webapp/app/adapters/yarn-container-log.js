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

import DS from 'ember-data';
import Ember from 'ember';
import Converter from 'yarn-ui/utils/converter';

/**
 * REST URL's response when fetching container logs will be
 * in plain text format and not JSON.
 */
export default DS.RESTAdapter.extend({
  headers: {
    Accept: 'text/plain'
  },

  host: Ember.computed("address", function () {
    return this.get(`hosts.localBaseAddress`);
  }),

  namespace: Ember.computed("restNameSpace", function () {
    return this.get(`env.app.namespaces.node`);
  }),

  urlForFindRecord(id/*, modelName, snapshot*/) {
    var splits = Converter.splitForContainerLogs(id);
    var nodeHttpAddr = splits[0];
    var containerId = splits[1];
    var filename = splits[2];
    var url = this._buildURL();
    url = url.replace("{nodeAddress}", nodeHttpAddr)  + "/containerlogs/"
           + containerId + "/" + filename;
    return url;
  },

  ajax(url, method, hash) {
    hash = hash || {};
    hash.crossDomain = true;
    hash.xhrFields = {withCredentials: true};
    hash.targetServer = "NM";
    return this._super(url, method, hash);
  },

  /**
   * Override options so that result is not expected to be JSON
   */
  ajaxOptions: function (url, type, options) {
    var hash = options || {};
    hash.url = url;
    hash.type = type;
    // Make sure jQuery does not try to convert response to JSON.
    hash.dataType = 'text';
    hash.context = this;

    var headers = Ember.get(this, 'headers');
    if (headers !== undefined) {
      hash.beforeSend = function (xhr) {
        Object.keys(headers).forEach(function (key) {
          return xhr.setRequestHeader(key, headers[key]);
        });
      };
    }
    return hash;
  },
});
