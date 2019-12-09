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
  name: DS.attr('string'),
  capacity: DS.attr('number'),
  usedCapacity: DS.attr('number'),
  state: DS.attr('string'),
  minQueueMemoryCapacity: DS.attr('number'),
  maxQueueMemoryCapacity: DS.attr('number'),
  numNodes: DS.attr('number'),
  usedNodeCapacity: DS.attr('number'),
  availNodeCapacity: DS.attr('number'),
  totalNodeCapacity: DS.attr('number'),
  numContainers: DS.attr('number'),
  type: DS.attr('string'),

  capacitiesBarChartData: function() {
    var floatToFixed = Converter.floatToFixed;
    return [
      {
        label: "Available Capacity",
        value: floatToFixed(this.get("availNodeCapacity"))
      },
      {
        label: "Used Capacity",
        value: floatToFixed(this.get("usedNodeCapacity"))
      },
      {
        label: "Total Capacity",
        value: floatToFixed(this.get("totalNodeCapacity"))
      }
    ];
  }.property("availNodeCapacity", "usedNodeCapacity", "totalNodeCapacity")

});
