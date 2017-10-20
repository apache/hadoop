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
import Converter from 'yarn-ui/utils/converter';

export default Ember.Controller.extend({
  flowUid: function() {
    return this.get('model.flowUid');
  }.property('model.flowUid'),

  flowLastExecutionDate: function() {
    if (this.get('model.lastFlowExecutionInfo')) {
      return this.get('model.lastFlowExecutionInfo').get('lastExecDate');
    } else {
      return '';
    }
  }.property('model.lastFlowExecutionInfo'),

  flowInfo: function() {
    var info = {};
    var firstRunObj = this.get('model.flowRuns').get('firstObject');
    info.flowUid = this.get('flowUid');
    info.flowName = firstRunObj.get('flowName');
    info.user = firstRunObj.get('user');
    info.lastExecutionDate = this.get('flowLastExecutionDate');
    info.firstRunStarted = this.get('earliestStartTime');
    info.lastRunFinished = this.get('latestFinishTime');
    return info;
  }.property('model.flowRuns', 'flowLastExecutionDate'),

  earliestStartTime: function() {
    var earliestStart = Number.MAX_VALUE;
    this.get('model.flowRuns').forEach(function(flowrun) {
      if (flowrun.get('createTimeRaw') < earliestStart) {
        earliestStart = flowrun.get('createTimeRaw');
      }
    });
    return Converter.timeStampToDate(earliestStart);
  }.property('model.flowRuns'),

  latestFinishTime: function() {
    var latestFinish = 0;
    this.get('model.flowRuns').forEach(function(flowrun) {
      if (flowrun.get('endTimeRaw') > latestFinish) {
        latestFinish = flowrun.get('endTimeRaw');
      }
    });
    return Converter.timeStampToDate(latestFinish);
  }.property('model.flowRuns')
});
