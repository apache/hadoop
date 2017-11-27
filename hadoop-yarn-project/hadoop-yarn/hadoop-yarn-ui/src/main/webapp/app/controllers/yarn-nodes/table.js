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
import TableDefinition from 'em-table/utils/table-definition';

export default Ember.Controller.extend({
    queryParams: ['searchText', 'sortColumnId', 'sortOrder', 'pageNum', 'rowCount'],
    tableDefinition: TableDefinition.create(),
    searchText: Ember.computed.alias('tableDefinition.searchText'),
    sortColumnId: Ember.computed.alias('tableDefinition.sortColumnId'),
    sortOrder: Ember.computed.alias('tableDefinition.sortOrder'),
    pageNum: Ember.computed.alias('tableDefinition.pageNum'),
    rowCount: Ember.computed.alias('tableDefinition.rowCount'),
    tableDefinition: TableDefinition.create({
        enableFaceting: true,
        rowCount: 25
    }),
    columns: function() {
        var colums = [];
        colums.push({
            id: 'label',
            headerTitle: 'Node Label',
            contentPath: 'nodeLabelsAsString',
            minWidth: "100px"
        }, {
            id: 'rack',
            headerTitle: 'Rack',
            contentPath: 'rack',
            facetType: null,
            minWidth: "100px"
        }, {
            id: 'state',
            headerTitle: 'Node State',
            contentPath: 'state',
            cellComponentName: 'em-table-status-cell',
            minWidth: "100px"
        }, {
            id: 'address',
            headerTitle: 'Node Address',
            contentPath: 'id',
            minWidth: "300px",
            facetType: null,
        }, {
            id: 'nodeId',
            headerTitle: 'Node HTTP Address',
            contentPath: 'nodeHTTPAddress',
            cellComponentName: 'em-table-linked-cell',
            facetType: null,
            getCellContent: function(row) {
              var node_id = row.get("id"),
                  node_addr = row.get("nodeHTTPAddress"),
                  href = `#/yarn-node/${node_id}/${node_addr}/info`;
                switch(row.get("nodeState")) {
                case "SHUTDOWN":
                case "LOST":
                    href = "";
                }
              return {
                text: row.get("nodeHTTPAddress"),
                href: href
              };
            },
            minWidth: "250px"
        }, {
            id: 'containers',
            headerTitle: 'Containers',
            contentPath: 'numContainers',
            facetType: null,
        }, {
            id: 'memUsed',
            headerTitle: 'Mem Used',
            contentPath: 'usedMemoryBytes',
            facetType: null,
            cellDefinition: {
              type: "memory"
            }
        }, {
            id: 'memAvail',
            headerTitle: 'Mem Available',
            contentPath: 'availMemoryBytes',
            facetType: null,
            cellDefinition: {
              type: "memory"
            }
        }, {
            id: 'coresUsed',
            headerTitle: 'VCores Used',
            contentPath: 'usedVirtualCores',
            facetType: null,
        }, {
            id: 'coresAvail',
            headerTitle: 'VCores Available',
            contentPath: 'availableVirtualCores',
            facetType: null,
        }, {
            id: 'healthUpdate',
            headerTitle: 'Last Health Update',
            contentPath: 'lastHealthUpdate',
            facetType: null,
        }, {
            id: 'healthReport',
            headerTitle: 'Health-Report',
            contentPath: 'healthReport',
            facetType: null,
        }, {
            id: 'version',
            headerTitle: 'Version',
            contentPath: 'version',
            facetType: null,
            observePath: true
        });
        return ColumnDef.make(colums);
    }.property()
});
