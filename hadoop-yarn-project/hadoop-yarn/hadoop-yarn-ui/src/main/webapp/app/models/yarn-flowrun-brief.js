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
import Converter from 'yarn-ui/utils/converter';

export default DS.Model.extend({
  flowName: DS.attr('string'),
  runid: DS.attr('string'),
  shownid: DS.attr('string'),
  type: DS.attr('string'),
  createTime: DS.attr('string'),
  createTimeRaw: DS.attr(),
  endTime: DS.attr('string'),
  endTimeRaw: DS.attr(),
  user: DS.attr('string'),
  uid: DS.attr('string'),
  cpuVCores: DS.attr('number'),
  memoryUsed: DS.attr('number'),

  runDurationTs: function() {
    var duration = this.get('endTimeRaw') - this.get('createTimeRaw');
    if (duration <= 0) {
      duration = Date.now() - this.get('createTimeRaw');
    }
    return duration;
  }.property('createTimeRaw', 'endTimeRaw'),

  getElapsedTimeVizDataForBarChart: function() {
    return {
      label: this.get('runid'),
      value: this.get('runDurationTs'),
      tooltip: this.get("shownid") + "<br>" + Converter.msToElapsedTimeUnit(this.get('runDurationTs')),
      flowrunUid: this.get('uid')
    };
  },

  getCpuVCoresVizDataForBarChart: function() {
    return {
      label: this.get('runid'),
      value: this.get('cpuVCores'),
      tooltip: this.get("shownid") + "<br>" + 'CPU VCores: ' + this.get('cpuVCores'),
      flowrunUid: this.get('uid')
    };
  },

  getMemoryVizDataForBarChart: function() {
    return {
      label: this.get('runid'),
      value: this.get('memoryUsed'),
      tooltip: this.get("shownid") + "<br>" + 'Memory Used: ' + Converter.memoryBytesToMB(this.get('memoryUsed')),
      flowrunUid: this.get('uid')
    };
  }
});
