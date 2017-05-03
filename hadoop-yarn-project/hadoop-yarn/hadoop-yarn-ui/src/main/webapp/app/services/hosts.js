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

import Ember from 'ember';

export default Ember.Service.extend({

  env: Ember.inject.service("env"),

  attachProtocolScheme: function (url) {
    var localProto = this.get("env.app.hosts.protocolScheme");

    if(localProto === "") {
      localProto = "http:";
    }

    if(url.match("://")) {
      url = url.substr(url.indexOf("://") + 3);
    }

    return localProto + "//" + url;
  },

  normalizeURL: function (url) {
    var address;

    // If localBaseAddress is configured, then normalized URL has to
    // start with this address. For eg: when used with CORS proxy.
    // In any case, this fn will return with correct proto scheme.
    address = this.localAddress() + url;

    // Remove trailing slash
    if(address && address.charAt(address.length - 1) === '/') {
      address = address.slice(0, -1);
    }
    return address;
  },

  localAddress: function () {
    var localBaseAddressProto = "";

    if (this.get("env.app.hosts.localBaseAddress").length > 0) {
      localBaseAddressProto = this.get("env.app.hosts.localBaseAddress") + '/';
    }
    return this.attachProtocolScheme(localBaseAddressProto);
  },

  localBaseAddress: Ember.computed(function () {
    var url = this.localAddress();
    if (url.endsWith('/')) {
      url = url.slice(0, -1);
    }
    return url;
  }),

  timelineWebAddress: Ember.computed(function () {
    return this.normalizeURL(this.get("env.app.hosts.timelineWebAddress"));
  }),

  rmWebAddress: Ember.computed(function () {
    return this.normalizeURL(this.get("env.app.hosts.rmWebAddress"));
  }),

  dashWebAddress: Ember.computed(function () {
    return this.normalizeURL(this.get("env.app.hosts.dashWebAddress"));
  })
});
