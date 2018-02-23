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
import ColumnDef from 'em-table/utils/column-definition';
import Converter from 'yarn-ui/utils/converter';
import TableDefinition from 'em-table/utils/table-definition';

function createColumn() {
  var columns = [];

  tableDefinition: TableDefinition.create({
    searchType: 'manual',
  }),

  // Generate columns
  columns.push({
    id: 'appId',
    headerTitle: 'Application ID',
    contentPath: 'appId',
    cellComponentName: 'em-table-linked-cell',
    minWidth: "300px",
    getCellContent: function (row) {
      return {
        routeName: 'yarn-app.attempts',
        id: row.get('appId'),
        displayText: row.get('appId')
      };
    }
  });

  columns.push({
    id: 'appType',
    headerTitle: 'Application Type',
    contentPath: 'type'
  });

  columns.push({
    id: 'state',
    headerTitle: 'State',
    contentPath: 'state',
    cellComponentName: 'em-table-status-cell',
  });

  columns.push({
    id: 'elapsedTs',
    headerTitle: 'Elapsed Time',
    contentPath: 'elapsedTs',
    getCellContent: function(row) {
      return Converter.msToElapsedTimeUnit(row.get('elapsedTs'));
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

  return ColumnDef.make(columns);
}

export default Ember.Controller.extend({
  vizWidgets: {
    cpuVcores: true,
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

  cpuVCoresVizData: function() {
    var data = [];
    this.get('model.apps').forEach(function(app) {
      var vizData = app.getCpuVCoresVizDataForBarChart();
      if (vizData.value > 0) {
        data.push(vizData);
      }
    });
    data = this.getSortedVizDataInDesc(data);
    return this.getRefactoredVizData(data);
  }.property('model.apps'),

  memoryVizData: function() {
    var data = [];
    this.get('model.apps').forEach(function(app) {
      var vizData = app.getMemoryVizDataForBarChart();
      if (vizData.value > 0) {
        data.push(vizData);
      }
    });
    data = this.getSortedVizDataInDesc(data);
    return this.getRefactoredVizData(data);
  }.property('model.apps'),

  memoryFormatter: function(tick) {
    return Converter.memoryBytesToMB(tick);
  },

  onBarChartClick: function() {
    var self = this;
    return function(data) {
      self.transitionToRoute('yarn-app', data.appId);
    };
  }.property(),

  getSortedVizDataInDesc: function(data) {
    return data.sort(function(d1, d2) {
      return d2.value - d1.value;
    });
  },

  getRefactoredVizData: function(data) {
    data.forEach(function(viz, idx) {
      viz.appId = viz.label;
      viz.label = "App " + (++idx);
    }, this);
    return data;
  }
});
