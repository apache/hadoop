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

export default DS.Model.extend({
  name: DS.attr('string'),
  queueName: DS.attr('string'),
  usedMemoryMB: DS.attr('number'),
  usedVCore: DS.attr('number'),
  maxMemoryMB: DS.attr('number'),
  maxVCore: DS.attr('number'),
  amUsedMemoryMB: DS.attr('number'),
  amUsedVCore: DS.attr('number'),
  maxAMMemoryMB: DS.attr('number'),
  maxAMVCore: DS.attr('number'),
  userWeight: DS.attr('string'),
  activeApps: DS.attr('number'),
  pendingApps: DS.attr('number')
});