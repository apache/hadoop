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

/* globals ENV: true */

import Ember from 'ember';

function getConfigFromYarn(rmhost, application, config) {
  var httpUrl = window.location.protocol + '//' +
    (ENV.hosts.localBaseAddress? ENV.hosts.localBaseAddress + '/' : '') + rmhost;

  httpUrl += '/conf?name=' + config;
  Ember.Logger.log("The RM URL is: " + httpUrl);

  var configValue = "";
    $.ajax({
      type: 'GET',
      dataType: 'json',
      async: false,
      context: this,
      url: httpUrl,
      success: function(data) {
        configValue = data.property.value;
        Ember.Logger.log("Value of the config returned from RM: " + configValue);

        application.advanceReadiness();
      },
      error: function() {
        application.advanceReadiness();
      }
    });
  return configValue;
}

function getJHSURL(rmhost, application, isHttpsSchemeEnabled) {
  Ember.Logger.log("getJHSURL, params:rmhost=" + rmhost + ",application=" + application + ",isHttpsSchemeEnabled=" + isHttpsSchemeEnabled);
  var config = '';
  if (isHttpsSchemeEnabled) {
    config = 'mapreduce.jobhistory.webapp.https.address';
  } else {
    config = 'mapreduce.jobhistory.webapp.address';
  }
  return getConfigFromYarn(rmhost, application, config);
}

function getYarnHttpProtocolScheme(rmhost, application) {
  return getConfigFromYarn(rmhost, application, 'yarn.http.policy');
}

function getTimeLineURL(rmhost, isHttpsSchemeEnabled) {
  var url = window.location.protocol + '//' +
    (ENV.hosts.localBaseAddress? ENV.hosts.localBaseAddress + '/' : '') + rmhost;

  if(isHttpsSchemeEnabled) {
    url += '/conf?name=yarn.timeline-service.reader.webapp.https.address';
  } else {
    url += '/conf?name=yarn.timeline-service.reader.webapp.address';
  }

  Ember.Logger.log("Get Timeline V2 Address URL: " + url);
  return url;
}

function getTimeLineV1URL(rmhost, isHttpsSchemeEnabled) {
  var url = window.location.protocol + '//' +
    (ENV.hosts.localBaseAddress? ENV.hosts.localBaseAddress + '/' : '') + rmhost;

  if(isHttpsSchemeEnabled) {
    url += '/conf?name=yarn.timeline-service.webapp.https.address';
  } else {
    url += '/conf?name=yarn.timeline-service.webapp.address';
  }

  Ember.Logger.log("Get Timeline V1 Address URL: " + url);
  return url;
}

function getSecurityURL(rmhost) {
  var url = window.location.protocol + '//' +
    (ENV.hosts.localBaseAddress? ENV.hosts.localBaseAddress + '/' : '') + rmhost;

  url += '/conf?name=hadoop.security.authentication';
  Ember.Logger.log("Server security mode url is: " + url);
  return url;
}

function getClusterIdFromYARN(rmhost, application) {
  var httpUrl = window.location.protocol + '//' +
    (ENV.hosts.localBaseAddress? ENV.hosts.localBaseAddress + '/' : '') + rmhost;

  httpUrl += '/conf?name=yarn.resourcemanager.cluster-id';
  Ember.Logger.log("Get cluster-id URL is: " + httpUrl);

  var clusterId = "";
  $.ajax({
    type: 'GET',
    dataType: 'json',
    async: false,
    context: this,
    url: httpUrl,
    success: function(data) {
      clusterId = data.property.value;
      Ember.Logger.log("Cluster Id from RM: " + clusterId);
      application.advanceReadiness();
    },
    error: function() {
      application.advanceReadiness();
    }
  });
  return clusterId;
}

function getNodeManagerPort(rmhost, application) {
  var httpUrl = window.location.protocol + "//" +
    (ENV.hosts.localBaseAddress ? ENV.hosts.localBaseAddress + '/' : '') + rmhost
    + "/conf?name=yarn.nodemanager.webapp.address";

  var port = "8042";
  $.ajax({
    type: 'GET',
    dataType: 'json',
    async: false,
    context: this,
    url: httpUrl,
    success: function(data) {
      port = data.property.value.split(":")[1];
      application.advanceReadiness();
    },
    error: function() {
      port = "8042";
      application.advanceReadiness();
    }
  });
  return port;
}

function transformURL(url, hostname) {
  // Deleting the scheme from the beginning of the url
  url = url.replace(/(^\w+:|^)\/\//, '');

  var address = url.split(":")[0];
  var port = url.split(":")[1];
  // Instead of localhost, use the name of the host
  if (address === "0.0.0.0" || address === "localhost") {
    url = hostname + ":" + port;
  }

  Ember.Logger.log("The transformed URL is: " + url);
  return url;
}

function updateConfigs(application) {
  var hostname = window.location.hostname;
  var rmhost = hostname + (window.location.port ? ':' + window.location.port: '') +
    skipTrailingSlash(window.location.pathname);

  window.ENV = window.ENV || {};
  window.ENV.hosts = window.ENV.hosts || {};

  if(!ENV.hosts.rmWebAddress) {
    ENV.hosts.rmWebAddress = rmhost;
    ENV.hosts.protocolScheme = window.location.protocol;
  } else {
    rmhost = ENV.hosts.rmWebAddress;
  }

  Ember.Logger.log("RM Address: " + rmhost);

  var protocolSchemeFromRM = getYarnHttpProtocolScheme(rmhost, application);
  Ember.Logger.log("Is protocol scheme https? " + (protocolSchemeFromRM == "HTTPS_ONLY"));
  var isHttpsSchemeEnabled = (protocolSchemeFromRM == "HTTPS_ONLY");

  var clusterIdFromYARN = getClusterIdFromYARN(rmhost, application);
  ENV.clusterId = clusterIdFromYARN;

  var nodeManagerPort = getNodeManagerPort(rmhost, application);
  Ember.Logger.log("NodeMananger port: " + nodeManagerPort);
  ENV.nodeManagerPort = nodeManagerPort;

  if (!ENV.hosts.jhsAddress) {
    var jhsAddress = getJHSURL(rmhost, application, isHttpsSchemeEnabled);
    jhsAddress = transformURL(jhsAddress, hostname);
    Ember.Logger.log("The JHS address is " + jhsAddress);
    ENV.hosts.jhsAddress = jhsAddress;
  }

  if(!ENV.hosts.timelineWebAddress) {
    var timelinehost = "";
    $.ajax({
      type: 'GET',
      dataType: 'json',
      async: false,
      context: this,
      url: getTimeLineURL(rmhost, isHttpsSchemeEnabled),
      success: function(data) {
        timelinehost = data.property.value;
        timelinehost = transformURL(timelinehost, hostname);
        ENV.hosts.timelineWebAddress = timelinehost;
        Ember.Logger.log("Timeline Address from RM: " + timelinehost);

        application.advanceReadiness();
      },
      error: function() {
        application.advanceReadiness();
      }
    });
  } else {
    Ember.Logger.log("Timeline Address: " + ENV.hosts.timelineWebAddress);
    application.advanceReadiness();
  }

  if(!ENV.hosts.timelineV1WebAddress) {
    var timelinehost = "";
    $.ajax({
      type: 'GET',
      dataType: 'json',
      async: false,
      context: this,
      url: getTimeLineV1URL(rmhost, isHttpsSchemeEnabled),
      success: function(data) {
        timelinehost = data.property.value;
        timelinehost = transformURL(timelinehost, hostname);
        ENV.hosts.timelineV1WebAddress = timelinehost;
        Ember.Logger.log("Timeline V1 Address from RM: " + timelinehost);

        application.advanceReadiness();
      },
      error: function() {
        application.advanceReadiness();
      }
    });
  } else {
    Ember.Logger.log("Timeline V1 Address: " + ENV.hosts.timelineV1WebAddress);
    application.advanceReadiness();
  }

  if(!ENV.hosts.isSecurityEnabled) {
    var isSecurityEnabled = "";
    $.ajax({
      type: 'GET',
      dataType: 'json',
      async: false,
      context: this,
      url: getSecurityURL(rmhost),
      success: function(data) {
        isSecurityEnabled = data.property.value;
        ENV.hosts.isSecurityEnabled = isSecurityEnabled;
        Ember.Logger.log("Security mode is : " + isSecurityEnabled);
        application.advanceReadiness();
      },
      error: function() {
        application.advanceReadiness();
      }
    });
  } else {
    Ember.Logger.log("Security mode is: " + ENV.hosts.isSecurityEnabled);
    application.advanceReadiness();
  }
}

export function initialize( application ) {
  application.deferReadiness();
  updateConfigs(application);
}

export default {
  name: 'loader',
  before: 'env',
  initialize
};

const skipTrailingSlash = function(path) {
  path = path.replace('index.html', '');
  path = path.replace('ui2/', '');
  path = path.replace(/\/$/, '');
  console.log('base url:' + path)
  if(path.includes("redirect")) {
    var to = path.lastIndexOf('/');
    to = to == -1 ? path.length : to + 1;
    path = path.substring(0, to);
    console.log('base url after redirect:' + path)
  }
  return path;
};
