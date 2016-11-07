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
 * Represent log files as comma separated list.
 */
export default Ember.Helper.helper(function(params,hash) {
  var logFiles = hash.logFiles;
  if (logFiles == null) {
    return "";
  }
  var logFilesLen = logFiles.length;
  if (logFilesLen == 0) {
    return "";
  }
  var nodeId = hash.nodeId;
  var nodeAddr = hash.nodeAddr;
  var containerId = hash.containerId;
  var html = '<td>';
  var logFilesCommaSeparated = "";
  for (var i = 0; i < logFilesLen; i++) {
    html = html + '<a href="#/yarn-container-log/' + nodeId + '/' +
        nodeAddr + '/' + containerId + '/' + logFiles[i] + '">' + logFiles[i] +
        '</a>';
    if (i != logFilesLen - 1) {
      html = html + ",";
    }
  }
  html = html + '</td>';
  return Ember.String.htmlSafe(html);
});
