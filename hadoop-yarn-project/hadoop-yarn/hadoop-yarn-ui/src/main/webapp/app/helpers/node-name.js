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

export function nodeName(params/*, hash*/) {
  // Place a menu within a panel inside col-md-2 container.
  console.log('nodes-uid', params[0]);
  var nodeIdSplitAtPort = params[0];
  var portIndex = nodeIdSplitAtPort.indexOf(':');
  if (portIndex != -1) {
    nodeIdSplitAtPort = nodeIdSplitAtPort.substring(0, portIndex) +
        ':&#8203;' + nodeIdSplitAtPort.substring(portIndex + 1);
  }
  var normalizedNodeId = '';
  var splitsAlongDots = nodeIdSplitAtPort.split('.');
  if (splitsAlongDots) {
    var len = splitsAlongDots.length;
    for (var i = 0; i < len; i++) {
      normalizedNodeId = normalizedNodeId + splitsAlongDots[i];
      if (i != len - 1) {
        normalizedNodeId = normalizedNodeId + '.&#8203;';
      }
    }
  } else {
    normalizedNodeId = nodeIdSplitAtPort;
  }
  return Ember.String.htmlSafe(normalizedNodeId);
}

export default Ember.Helper.helper(nodeName);
