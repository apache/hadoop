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
import TableDef from 'em-table/utils/table-definition';

export default Ember.Controller.extend({
  tableDefinition: TableDef.create({
    sortColumnId: 'stTime',
    sortOrder: 'desc'
  }),

  columns: function() {
      var colums = [];
      colums.push({
          id: 'appId',
          headerTitle: 'Application ID',
          contentPath: 'id',
          cellComponentName: 'em-table-linked-cell',
          minWidth: "250px",
          getCellContent: function(row) {
            return {
              displayText: row.id,
              href: `#/yarn-app/${row.id}`
            };
          }
      }, {
          id: 'appType',
          headerTitle: 'Application Type',
          contentPath: 'applicationType',
      }, {
          id: 'appName',
          headerTitle: 'Application Name',
          contentPath: 'appName',
      }, {
          id: 'appUsr',
          headerTitle: 'User',
          contentPath: 'user',
          minWidth: "50px"
      }, {
          id: 'queue',
          headerTitle: 'Queue',
          contentPath: 'queue',
      }, {
          id: 'state',
          headerTitle: 'State',
          contentPath: 'state',
          cellComponentName: 'em-table-status-cell',
          minWidth: "50px"
      }, {
          id: 'progress',
          headerTitle: 'Progress',
          contentPath: 'progress',
          cellComponentName: 'em-table-progress-cell',
          cellDefinition: {
            valueMax: 100
          }
      }, {
          id: 'stTime',
          headerTitle: 'Start Time',
          contentPath: 'startTime',
      }, {
          id: 'elTime',
          headerTitle: 'Elapsed Time',
          contentPath: 'elapsedTime',
          cellDefinition: {
            type: "duration"
          }
      }, {
          id: 'finishTime',
          headerTitle: 'Finished Time',
          contentPath: 'validatedFinishedTs',
          observePath: true
      }, {
          id: 'priority',
          headerTitle: 'Priority',
          contentPath: 'priority',
      }, {
          id: 'cluster',
          headerTitle: '%Cluster',
          contentPath: 'clusterUsagePercentage',
          observePath: true
      });
      return ColumnDef.make(colums);
  }.property(),

  serviceColumns: function() {
    var colums = [];
    colums.push({
      id: 'appName',
      headerTitle: 'Service Name',
      contentPath: 'appName',
      minWidth: "200px",
      cellComponentName: 'em-table-linked-cell',
      getCellContent: function(row) {
        return {
          displayText: row.get('appName'),
          href: `#/yarn-app/${row.id}?service=${row.get('appName')}`
        };
      }
    }, {
      id: 'appId',
      headerTitle: 'Application ID',
      contentPath: 'id',
      minWidth: "250px"
    }, {
      id: 'state',
      headerTitle: 'State',
      contentPath: 'state',
      cellComponentName: 'em-table-status-cell',
      minWidth: "50px"
    }, {
      id: 'cluster',
      headerTitle: '%Cluster',
      contentPath: 'clusterUsagePercentage',
      observePath: true
    }, {
      id: 'elTime',
      headerTitle: 'Elapsed Time',
      contentPath: 'elapsedTime',
      cellDefinition: {
        type: "duration"
      },
      minWidth: "200px"
    }, {
        id: 'appUsr',
        headerTitle: 'User',
        contentPath: 'user',
        minWidth: "50px"
    }, {
        id: 'queue',
        headerTitle: 'Queue',
        contentPath: 'queue',
    }, {
      id: 'stTime',
      headerTitle: 'Started Time',
      contentPath: 'startTime',
    }, {
      id: 'finishTime',
      headerTitle: 'Finished Time',
      contentPath: 'validatedFinishedTs',
      observePath: true
    });
    return ColumnDef.make(colums);
  }.property(),
});
