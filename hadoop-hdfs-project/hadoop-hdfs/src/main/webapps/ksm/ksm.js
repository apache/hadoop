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
    angular.module('ksm', ['nvd3'])
    angular.module('ksm').component('overview', {
        templateUrl: 'overview.html',
        controller: function ($http) {
            var ctrl = this;
            $http.get("/jmx?qry=Hadoop:service=KeySpaceManager,name=KeySpaceManagerInfo")
                .then(function (result) {
                    ctrl.jmx = result.data.beans[0]
                })
        }
    });
    angular.module('ksm').component('jvmParameters', {
        templateUrl: 'jvm.html',
        controller: function ($http) {
            var ctrl = this
            $http.get("/jmx?qry=java.lang:type=Runtime")
                .then(function (result) {
                    ctrl.jmx = result.data.beans[0];

                    //convert array to a map
                    var systemProperties = {}
                    for (var idx in ctrl.jmx.SystemProperties) {
                        var item = ctrl.jmx.SystemProperties[idx];
                        systemProperties[item.key.replace(/\./g,"_")] = item.value;
                    }
                    ctrl.jmx.SystemProperties = systemProperties;
                })
        }
    });
    angular.module('ksm').component('ksmMetrics', {
        templateUrl: 'ksm-metrics.html',
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
                    labelSunbeamLayout: true,
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


            $http.get("/jmx?qry=Hadoop:service=KeySpaceManager,name=KSMMetrics")
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
                                groupedMetrics.nums[type].all.push({
                                    key: name,
                                    value: metrics[key]
                                })
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
    angular.module('ksm').component('rpcMetrics', {
        template: '<div ng-repeat="metric in $ctrl.metrics"><rpc-metric jmxdata="metric"></rpc-metric></div>',
        controller: function ($http) {
            var ctrl = this;
            $http.get("/jmx?qry=Hadoop:service=KeySpaceManager,name=RpcActivityForPort*")
                .then(function (result) {
                    ctrl.metrics = result.data.beans;
                })
        }
    });
    angular.module('ksm').component('rpcMetric', {
        bindings: {
            jmxdata: '<'
        },
        templateUrl: 'rpc-metrics.html',
        controller: function () {
            var ctrl = this;


            ctrl.percentileGraphOptions = {
                chart: {
                    type: 'discreteBarChart',
                    height: 450,
                    margin: {
                        top: 20,
                        right: 20,
                        bottom: 50,
                        left: 55
                    },
                    x: function (d) {
                        return d.label;
                    },
                    y: function (d) {
                        return d.value;
                    },
                    showValues: true,
                    valueFormat: function (d) {
                        return d3.format(',.1f')(d);
                    },
                    duration: 500,
                    xAxis: {
                        axisLabel: 'Percentage'
                    },
                    yAxis: {
                        axisLabel: 'Latency (ms)',
                        axisLabelDistance: -10
                    }
                }
            };

            ctrl.$onChanges = function (data) {
                var groupedMetrics = {}

                var createPercentageMetrics = function (metricName, window) {
                    groupedMetrics.percentiles = groupedMetrics['percentiles'] || {}
                    groupedMetrics.percentiles[metricName] = groupedMetrics.percentiles[metricName] || {};
                    groupedMetrics.percentiles[metricName][window] = groupedMetrics.percentiles[metricName][window] || {
                            graphdata: [{
                                key: window,
                                values: []
                            }], numOps: 0
                        };

                };
                var metrics = ctrl.jmxdata;
                for (var key in metrics) {
                    var percentile = key.match(/(.*Time)(\d+s)(\d+th)PercentileLatency/);
                    var percentileNumOps = key.match(/(.*Time)(\d+s)NumOps/);
                    var successFailures = key.match(/(.*)(Success|Failures)/);
                    var numAverages = key.match(/(.*Time)(NumOps|AvgTime)/);
                    if (percentile) {
                        var metricName = percentile[1];
                        var window = percentile[2];
                        var percentage = percentile[3]
                        createPercentageMetrics(metricName, window);


                        groupedMetrics.percentiles[metricName][window].graphdata[0]
                            .values.push({
                            label: percentage,
                            value: metrics[key]
                        })
                    } else if (successFailures) {
                        var metricName = successFailures[1];
                        groupedMetrics.successfailures = groupedMetrics['successfailures'] || {}
                        groupedMetrics.successfailures[metricName] = groupedMetrics.successfailures[metricName] || {
                                success: 0,
                                failures: 0
                            };
                        if (successFailures[2] == 'Success') {
                            groupedMetrics.successfailures[metricName].success = metrics[key];
                        } else {
                            groupedMetrics.successfailures[metricName].failures = metrics[key];
                        }

                    } else if (numAverages) {
                        var metricName = numAverages[1];
                        groupedMetrics.numavgs = groupedMetrics['numavgs'] || {}
                        groupedMetrics.numavgs[metricName] = groupedMetrics.numavgs[metricName] || {
                                numOps: 0,
                                avgTime: 0
                            };
                        if (numAverages[2] == 'NumOps') {
                            groupedMetrics.numavgs[metricName].numOps = metrics[key];
                        } else {
                            groupedMetrics.numavgs[metricName].avgTime = metrics[key];
                        }

                    } else if (percentileNumOps) {
                        var metricName = percentileNumOps[1];
                        var window = percentileNumOps[2];
                        createPercentageMetrics(metricName, window);
                        groupedMetrics.percentiles[metricName][window].numOps = metrics[key];
                    } else if (isIgnoredJmxKeys(key)) {
                        //ignore
                    } else {
                        groupedMetrics.others = groupedMetrics.others || [];
                        groupedMetrics.others.push({
                            'key': key,
                            'value': metrics[key]
                        });
                    }

                }
                ctrl.metrics = groupedMetrics;
            };

        }
    });

})();
