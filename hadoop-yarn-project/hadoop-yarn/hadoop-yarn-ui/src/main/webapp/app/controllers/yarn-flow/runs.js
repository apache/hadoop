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
import Converter from 'yarn-ui/utils/converter';

function createColumn() {
  var columns = [];

  // Generate columns
  columns.push({
    id: 'runid',
    headerTitle: 'Run ID',
    contentPath: 'runid',
    cellComponentName: 'em-table-linked-cell',
    minWidth: "300px",
    getCellContent: function (row) {
      return {
        routeName: 'yarn-flowrun.info',
        id: row.get('uid'),
        displayText: row.get('shownid')
      };
    }
  });

  columns.push({
    id: 'runDurationTs',
    headerTitle: 'Run Duration',
    contentPath: 'runDurationTs',
    getCellContent: function(row) {
      return Converter.msToElapsedTimeUnit(row.get('runDurationTs'));
    }
  });

  columns.push({
    id: 'cpuVCores',
    headerTitle: 'CPU VCores',
    contentPath: 'cpuVCores',
    getCellContent: function(row) {
      if (row.get('cpuVCores') > -1) {
        return row.get('cpuVCores');
      }
      return 'N/A';
    }
  });

  columns.push({
    id: 'memoryUsed',
    headerTitle: 'Memory Used',
    contentPath: 'memoryUsed',
    getCellContent: function(row) {
      if (row.get('memoryUsed') > -1) {
        return Converter.memoryBytesToMB(row.get('memoryUsed'));
      }
      return 'N/A';
    }
  });

  columns.push({
    id: 'createTime',
    headerTitle: 'Creation Time',
    contentPath: 'createTime'
  });

  columns.push({
    id: 'endTime',
    headerTitle: 'End Time',
    contentPath: 'endTime'
  });

  return ColumnDef.make(columns);
}

export default Ember.Controller.extend({
  vizWidgets: {
    runDuration: true,
    cpuVcores: false,
    memoryUsed: false
  },

  actions: {
    addVizWidget(widget) {
      Ember.set(this.vizWidgets, widget, true);
    },

    removeVizWidget(widget) {
      Ember.set(this.vizWidgets, widget, false);
    }
  },

  columns: createColumn(),

  tableDefinition: TableDef.create({
    sortColumnId: 'createTime',
    sortOrder: 'desc'
  }),

  elapsedTimeVizData: function() {
    var data = [];
    this.get('model.flowRuns').forEach(function(run) {
      var vizData = run.getElapsedTimeVizDataForBarChart();
      if (vizData.value > 0) {
        data.push(vizData);
      }
    });
    data = this.getSortedVizDataInDesc(data);
    return this.getRefactoredVizData(data);
  }.property('model.flowRuns'),

  elapsedTimeFormatter: function(tick) {
    return Converter.msToElapsedTimeUnit(tick, true);
  },

  cpuVCoresVizData: function() {
    var data = [];
    this.get('model.flowRuns').forEach(function(run) {
      var vizData = run.getCpuVCoresVizDataForBarChart();
      if (vizData.value > 0) {
        data.push(vizData);
      }
    });
    data = this.getSortedVizDataInDesc(data);
    return this.getRefactoredVizData(data);
  }.property('model.flowRuns'),

  memoryVizData: function() {
    var data = [];
    this.get('model.flowRuns').forEach(function(run) {
      var vizData = run.getMemoryVizDataForBarChart();
      if (vizData.value > 0) {
        data.push(vizData);
      }
    });
    data = this.getSortedVizDataInDesc(data);
    return this.getRefactoredVizData(data);
  }.property('model.flowRuns'),

  memoryFormatter: function(tick) {
    return Converter.memoryBytesToMB(tick);
  },

  onBarChartClick: function() {
    var self = this;
    return function(data) {
      self.transitionToRoute('yarn-flowrun.info', data.flowrunUid);
    };
  }.property(),

  getSortedVizDataInDesc: function(data) {
    return data.sort(function(d1, d2) {
      return d2.createdTs - d1.createdTs;
    });
  },

  getRefactoredVizData: function(data) {
    data.forEach(function(viz, idx) {
      viz.label = "Run " + (++idx);
    }, this);
    return data;
  }
});
