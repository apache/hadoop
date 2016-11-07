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


function getTimeLineURL(parameters) {
  return '/conf?name=yarn.timeline-service.webapp.address';
}

function updateConfigs(application) {
  var hostname = window.location.hostname;
  var rmhost = hostname +
    (window.location.port ? ':' + window.location.port: '');

  Ember.Logger.log("RM Address:" + rmhost);

  if(!ENV.hosts.rmWebAddress) {
    ENV = {
       hosts: {
          rmWebAddress: rmhost,
        },
    };
  }

  if(!ENV.hosts.timelineWebAddress) {
    var result = [];
    var timelinehost = "";
    $.ajax({
      type: 'GET',
      dataType: 'json',
      async: true,
      context: this,
      url: getTimeLineURL(),
      success: function(data) {
        timelinehost = data.property.value;
        ENV.hosts.timelineWebAddress = timelinehost;

        var address = timelinehost.split(":")[0];
        var port = timelinehost.split(":")[1];

        Ember.Logger.log("Timeline Address from RM:" + address + ":" + port);

        if(address == "0.0.0.0" || address == "localhost") {
          var updatedAddress =  hostname + ":" + port;

          /* Timeline v2 is not supporting CORS, so make as default*/
          ENV = {
             hosts: {
                rmWebAddress: rmhost,
                timelineWebAddress: updatedAddress,
              },
          };
          Ember.Logger.log("Timeline Updated Address:" + updatedAddress);
        }
        application.advanceReadiness();
      },
    });
  } else {
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
