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

  handleResponse(status, headers, payload, requestData) {
    // If the user is not authorized to signal a threaddump for a container,
    // the response contains a RemoteException with a 403 (Forbidden) status
    // code. Extract out the error message from the RemoteException in this
    // case.
    // If the status is '0' or empty, it is symptomatic of the YARN role not
    // available or able to respond or a network timeout/firewall issue.
    if (status === 403)  {
      if (payload
          && typeof payload === 'object'
          && payload.RemoteException
          && payload.RemoteException.message) {
        return new Error(payload.RemoteException.message);
      }
    } else if (status === 0 && payload === "") {
      return new Error("Not able to connect to YARN!");
    }

    return payload;
  },

  /**
   * Set up the POST request to use the signalToContainer REST API
   * to signal a thread dump for a running container to RM.
   */
  signalContainerForThreaddump(request, containerId, user) {
    var url = this.buildURL();
    if (user && containerId) {
      url += "/containers" + "/" + containerId + "/signal"
            + "/OUTPUT_THREAD_DUMP" + "?user.name=" + user;
    }
    return this.ajax(url, "POST", {data: request});
  },

  ajax(url, method, hash) {
    hash = {};
    hash.crossDomain = true;
    hash.xhrFields = {withCredentials: true};
    hash.targetServer = "RM";
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
  }
});
