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
(function () {
    "use strict";
    angular.module('scm', ['ozone', 'nvd3']);

    angular.module('scm').component('scmOverview', {
        templateUrl: 'scm-overview.html',
        require: {
            overview: "^overview"
        },
        controller: function ($http) {
            var ctrl = this;
            $http.get("jmx?qry=Hadoop:service=SCMNodeManager,name=SCMNodeManagerInfo")
                .then(function (result) {
                    ctrl.nodemanagermetrics = result.data.beans[0];
                });
            $http.get("jmx?qry=Hadoop:service=StorageContainerManager,name=StorageContainerManagerInfo,component=ServerRuntime")
                .then(function (result) {
                    ctrl.scmmetrics = result.data.beans[0];
                });

            var statusSortOrder = {
                "HEALTHY": "a",
                "STALE": "b",
                "DEAD": "c",
                "UNKNOWN": "z",
                "DECOMMISSIONING": "x",
                "DECOMMISSIONED": "y"
            };
            ctrl.nodeOrder = function (v1, v2) {
                //status with non defined sort order will be "undefined"
                return ("" + statusSortOrder[v1.value]).localeCompare("" + statusSortOrder[v2.value])
            }

        }
    });

})();
