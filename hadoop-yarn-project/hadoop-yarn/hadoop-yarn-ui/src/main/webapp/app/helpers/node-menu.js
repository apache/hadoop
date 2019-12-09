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

/**
 * Create left hand side node manager menu with menu item activated based
 * on page being accessed.
 */
export default Ember.Helper.helper(function(params,hash) {
  // Place a menu within a panel inside col-md-2 container.
  var nodeIdSplitAtPort = hash.nodeId;
  var portIndex = nodeIdSplitAtPort.indexOf(':');
  if (portIndex !== -1) {
    nodeIdSplitAtPort = nodeIdSplitAtPort.substring(0, portIndex) +
        ':&#8203;' + nodeIdSplitAtPort.substring(portIndex + 1);
  }
  var normalizedNodeId = '';
  var splitsAlongDots = nodeIdSplitAtPort.split('.');
  if (splitsAlongDots) {
    var len = splitsAlongDots.length;
    for (var i = 0; i < len; i++) {
      normalizedNodeId = normalizedNodeId + splitsAlongDots[i];
      if (i !== len - 1) {
        normalizedNodeId = normalizedNodeId + '.&#8203;';
      }
    }
  } else {
    normalizedNodeId = nodeIdSplitAtPort;
  }

  var html = '<div class="col-md-2 container-fluid"><div class="panel panel-default">'+
      '<div class="panel-heading"><h4>Node Manager<br>(' + normalizedNodeId + ')</h4></div>'+
      '<div class="panel-body"><ul class="nav nav-pills nav-stacked" id="stacked-menu">' +
      '<ul class="nav nav-pills nav-stacked collapse in"><li';
  if (hash.path === 'yarn-node') {
    html = html + ' class="active"';
  }
  html = html + '><a href="#/yarn-node/' + hash.nodeId + '/' + hash.nodeAddr +
      '">Node Information</a></li><li';
  if (hash.path === 'yarn-node-apps') {
    html = html + ' class="active"';
  }
  html = html + '><a href="#/yarn-node-apps/' + hash.nodeId + '/' + hash.nodeAddr +
      '">List of Applications on this Node</a></li><li';
  if (hash.path === 'yarn-node-containers') {
    html = html + ' class="active"';
  }
  html = html + '><a href="#/yarn-node-containers/' +hash.nodeId + '/' + hash.nodeAddr +
      '">List of Containers on this Node</a></li></ul></ul></div>';
  return Ember.String.htmlSafe(html);
});
