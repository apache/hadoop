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

export default DS.Model.extend({
  containerId: DS.attr('string'),
  component: DS.attr('string'),
  instanceName: DS.attr('string'),
  state: DS.attr('number'),
  createdTimestamp: DS.attr('number'),
  startedTimestamp: DS.attr('number'),
  host: DS.attr('string'),
  node: DS.attr('string'),
  hostUrl: DS.attr('string'),
  ipAddr: DS.attr('string'),
  exposedPorts: DS.attr('string'),
  exitStatusCode: DS.attr('string'),

  createdDate: Ember.computed('createdTimestamp', function() {
    var timestamp = this.get('createdTimestamp');
    if (timestamp > 0) {
      return Converter.timeStampToDate(timestamp);
    }
    return 'N/A';
  }),

  startedDate: Ember.computed('startedTimestamp', function() {
    var timestamp = this.get('startedTimestamp');
    if (timestamp > 0) {
      return Converter.timeStampToDate(timestamp);
    }
    return 'N/A';
  }),

  termLink: Ember.computed('node', 'containerId', function() {
    var protocol = window.location.protocol;
    var node = this.get('node');
    var port = this.get('env.app.nodeManagerPort');
    var containerId = this.get('containerId');
    var url = protocol + "//" + node + ":" + port +
       "/terminal/terminal.template?container=" + containerId;
    return url;
  }),

  containerLogURL: Ember.computed('containerId', function() {
    const containerId = this.get('containerId');
    const attemptId = Converter.containerIdToAttemptId(containerId);
    const appId = Converter.attemptIdToAppId(attemptId);
    return `#/yarn-app/${appId}/logs?attempt=${attemptId}&containerid=${containerId}`;
  })
});
