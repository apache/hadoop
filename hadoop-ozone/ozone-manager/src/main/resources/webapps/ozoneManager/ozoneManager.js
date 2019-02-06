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

    var isIgnoredJmxKeys = function (key) {
        return key == 'name' || key == 'modelerType' || key.match(/tag.*/);
    };

    angular.module('ozoneManager', ['ozone', 'nvd3']);
    angular.module('ozoneManager').config(function ($routeProvider) {
        $routeProvider
            .when("/metrics/ozoneManager", {
                template: "<om-metrics></om-metrics>"
            });
    });
    angular.module('ozoneManager').component('omMetrics', {
        templateUrl: 'om-metrics.html',
        controller: function ($http) {
            var ctrl = this;

            ctrl.graphOptions = {
                chart: {
                    type: 'pieChart',
                    height: 500,
                    x: function (d) {
                        return d.key;
                    },
                    y: function (d) {
                        return d.value;
                    },
                    showLabels: true,
                    labelType: 'value',
                    duration: 500,
                    labelThreshold: 0.01,
                    valueFormat: function(d) {
                        return d3.format('d')(d);
                    },
                    legend: {
                        margin: {
                            top: 5,
                            right: 35,
                            bottom: 5,
                            left: 0
                        }
                    }
                }
            };


            $http.get("jmx?qry=Hadoop:service=OzoneManager,name=OMMetrics")
                .then(function (result) {

                    var groupedMetrics = {others: [], nums: {}};
                    var metrics = result.data.beans[0]
                    for (var key in metrics) {
                        var numericalStatistic = key.match(/Num([A-Z][a-z]+)(.+?)(Fails)?$/);
                        if (numericalStatistic) {
                            var type = numericalStatistic[1];
                            var name = numericalStatistic[2];
                            var failed = numericalStatistic[3];
                            groupedMetrics.nums[type] = groupedMetrics.nums[type] || {
                                    failures: [],
                                    all: []
                                };
                            if (failed) {
                                groupedMetrics.nums[type].failures.push({
                                    key: name,
                                    value: metrics[key]
                                })
                            } else {
                                if (name == "Ops") {
                                    groupedMetrics.nums[type].ops = metrics[key]
                                } else {
                                    groupedMetrics.nums[type].all.push({
                                        key: name,
                                        value: metrics[key]
                                    })
                                }
                            }
                        } else if (isIgnoredJmxKeys(key)) {
                            //ignore
                        } else {
                            groupedMetrics.others.push({
                                'key': key,
                                'value': metrics[key]
                            });
                        }
                    }
                    ctrl.metrics = groupedMetrics;
                })
        }
    });

})();
