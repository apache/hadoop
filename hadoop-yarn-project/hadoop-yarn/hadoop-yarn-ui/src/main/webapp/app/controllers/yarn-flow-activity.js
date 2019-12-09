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
import TableDef from 'em-table/utils/table-definition';
import ColumnDef from 'em-table/utils/column-definition';
import lodash from 'lodash/lodash';

function _createColumns() {
  var columns = [];
  columns.push({
    id: 'flowName',
    headerTitle: 'Flow Name',
    contentPath: 'flowName',
    observePath: true,
  }, {
    id: 'user',
    headerTitle: 'User',
    contentPath: 'user',
    observePath: true
  }, {
    id: 'uid',
    headerTitle: 'Flow ID',
    contentPath: 'uid',
    observePath: true,
    cellComponentName: 'em-table-linked-cell',
    minWidth: "300px",
    getCellContent: function (row) {
      return {
        routeName: 'yarn-flow.info',
        id: row.get('uid'),
        displayText: row.get('uid')
      };
    }
  }, {
    id: 'lastExecDate',
    headerTitle: 'Last Execution Date',
    contentPath: 'lastExecDate',
    observePath: true
  });
  return ColumnDef.make(columns);
}

function _getAggregatedFlowsData(flows) {
  var aggregatedFlows = [];
  flows = flows? flows.get('content') : [];

  var aggregated = lodash.groupBy(flows, function(flow) {
    return flow.getRecord().get('uid');
  });

  lodash.forIn(aggregated, function(flows) {
    let flowsInAsc = lodash.sortBy(flows, function(flow) {
      return flow.getRecord().get('lastExecDate');
    });
    let flowsInDesc = flowsInAsc.reverse();
    aggregatedFlows.push(flowsInDesc[0].getRecord());
  });

  return aggregatedFlows;
}

function _createRows(flows) {
  var data = [],
      aggregatedFlows = null,
      row = null;

  aggregatedFlows = _getAggregatedFlowsData(flows);

  aggregatedFlows.forEach(function(flow) {
    row = Ember.Object.create({
      user: flow.get('user'),
      flowName: flow.get('flowName'),
      uid: flow.get('uid'),
      lastExecDate: flow.get('lastExecDate')
    });
    data.push(row);
  });

  return Ember.A(data);
}

export default Ember.Controller.extend({
  breadcrumbs: [{
    text: "Home",
    routeName: 'application'
  }, {
    text: "Flow Activities",
    routeName: 'yarn-flow-activity',
  }],

  columns: _createColumns(),

  rows: Ember.computed('model', function() {
    return _createRows(this.get('model'));
  }),

  tableDefinition: TableDef.create({
    sortColumnId: 'lastExecDate',
    sortOrder: 'desc'
  }),

  getLastFlowExecutionInfoByFlowUid: function(uid) {
    var aggregatedFlows = _getAggregatedFlowsData(this.get('model'));
    var recent = aggregatedFlows.find(function(flow) {
      return flow.get('uid') === uid;
    });
    return recent;
  }
});
