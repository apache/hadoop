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
import Converter from 'yarn-ui/utils/converter';

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
          minWidth: "280px",
          facetType: null,
          getCellContent: function(row) {
            return {
              displayText: row.id,
              href: `#/yarn-app/${row.id}/attempts`
            };
          }
      }, {
          id: 'appType',
          headerTitle: 'Application Type',
          contentPath: 'applicationType',
          facetType: null,
      }, {
          id: 'appTag',
          headerTitle: 'Application Tag',
          contentPath: 'applicationTags',
          facetType: null,
      }, {
          id: 'appName',
          headerTitle: 'Application Name',
          cellComponentName: 'em-table-tooltip-text',
          contentPath: 'appName',
          facetType: null,
      }, {
          id: 'appUsr',
          headerTitle: 'User',
          contentPath: 'user',
          minWidth: "50px"
      }, {
        id: 'state',
        headerTitle: 'State',
        contentPath: 'state',
        cellComponentName: 'em-table-simple-status-cell',
        minWidth: "50px"
      }, {
          id: 'queue',
          headerTitle: 'Queue',
          cellComponentName: 'em-table-tooltip-text',
          contentPath: 'queue',
      }, {
          id: 'progress',
          headerTitle: 'Progress',
          contentPath: 'progress',
          cellComponentName: 'em-table-progress-cell',
          facetType: null,
          cellDefinition: {
            valueMax: 100
          }
      }, {
          id: 'stTime',
          headerTitle: 'Start Time',
          contentPath: 'startTime',
          facetType: null,
          getCellContent: function(row) {
            return row.get('formattedStartTime');
          }
      }, {
          id: 'elTime',
          headerTitle: 'Elapsed Time',
          contentPath: 'elapsedTime',
          facetType: null,
          cellDefinition: {
            type: "duration"
          }
      }, {
          id: 'finishTime',
          headerTitle: 'Finished Time',
          contentPath: 'validatedFinishedTs',
          facetType: null,
          observePath: true,
          getCellContent: function(row) {
            return row.get('formattedFinishedTime');
          }
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
      facetType: null,
      cellComponentName: 'em-table-linked-cell',
      getCellContent: function(row) {
        return {
          displayText: row.get('appName'),
          href: `#/yarn-app/${row.id}/components?service=${row.get('appName')}`
        };
      }
    }, {
      id: 'appId',
      headerTitle: 'Application Tag',
      contentPath: 'id',
      facetType: null,
      cellComponentName: 'em-table-tooltip-text',
      minWidth: "250px"
    }, {
      id: 'appTag',
      headerTitle: 'Application ID',
      contentPath: 'applicationTags',
      facetType: null,
    }, {
      id: 'state',
      headerTitle: 'State',
      contentPath: 'state',
      cellComponentName: 'em-table-simple-status-cell',
      minWidth: "50px"
    }, {
      id: 'cluster',
      headerTitle: '%Cluster',
      contentPath: 'clusterUsagePercentage',
      facetType: null,
      observePath: true
    }, {
      id: 'elTime',
      headerTitle: 'Elapsed Time',
      contentPath: 'elapsedTime',
      facetType: null,
      cellDefinition: {
        type: "duration"
      },
      minWidth: "200px"
    }, {
        id: 'appUsr',
        headerTitle: 'User',
        contentPath: 'user',
        facetType: null,
        minWidth: "50px"
    }, {
        id: 'queue',
        headerTitle: 'Queue',
        contentPath: 'queue',
        cellComponentName: 'em-table-tooltip-text',
    }, {
      id: 'stTime',
      headerTitle: 'Started Time',
      contentPath: 'startTime',
      facetType: null,
      getCellContent: function(row) {
        return row.get('formattedStartTime');
      }
    }, {
      id: 'finishTime',
      headerTitle: 'Finished Time',
      contentPath: 'validatedFinishedTs',
      facetType: null,
      observePath: true,
      getCellContent: function(row) {
        return row.get('formattedFinishedTime');
      }
    });
    return ColumnDef.make(colums);
  }.property(),
});
