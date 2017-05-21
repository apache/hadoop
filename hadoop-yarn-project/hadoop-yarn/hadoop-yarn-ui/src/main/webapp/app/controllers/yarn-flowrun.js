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

export default Ember.Controller.extend({
  breadcrumbs: Ember.computed('model.flowrun_uid', 'model.parentFlowUid', function() {
    var flowRunId = this.get('model.flowrun_uid');
    var parentFlowUid = this.get('model.parentFlowUid');
    var crumbs = [{
      text: "Home",
      routeName: 'application'
    }, {
      text: "Flow Activities",
      routeName: 'yarn-flow-activity'
    }];
    if (parentFlowUid) {
      crumbs.push({
        text: `Flow Info [${parentFlowUid}]`,
        routeName: 'yarn-flow.info',
        model: parentFlowUid
      }, {
        text: `Flow Runs [${parentFlowUid}]`,
        routeName: 'yarn-flow.runs',
        model: parentFlowUid
      });
    }
    crumbs.push({
      text: `Run Info [${flowRunId}]`,
      routeName: 'yarn-flowrun.info',
      model: flowRunId
    });
    return crumbs;
  })
});
