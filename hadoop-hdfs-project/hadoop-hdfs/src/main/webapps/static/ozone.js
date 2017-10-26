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
        return key == 'name' || key == 'modelerType' || key == "$$hashKey" ||
            key.match(/tag.*/);
    };
    angular.module('ozone', ['nvd3'])
    angular.module('ozone').component('overview', {
        templateUrl: 'static/templates/overview.html',
        transclude: true,
        controller: function ($http) {
            var ctrl = this;
            $http.get("/jmx?qry=Hadoop:service=*,name=*,component=ServerRuntime")
                .then(function (result) {
                    ctrl.jmx = result.data.beans[0]
                })
        }
    });
    angular.module('ozone').component('jvmParameters', {
        templateUrl: 'static/templates/jvm.html',
        controller: function ($http) {
            var ctrl = this
            $http.get("/jmx?qry=java.lang:type=Runtime")
                .then(function (result) {
                    ctrl.jmx = result.data.beans[0];

                    //convert array to a map
                    var systemProperties = {}
                    for (var idx in ctrl.jmx.SystemProperties) {
                        var item = ctrl.jmx.SystemProperties[idx];
                        systemProperties[item.key.replace(/\./g, "_")] = item.value;
                    }
                    ctrl.jmx.SystemProperties = systemProperties;
                })
        }
    });

    angular.module('ozone').component('rpcMetrics', {
        template: '<h1>Rpc metrics</h1><tabs>' +
        '<pane ng-repeat="metric in $ctrl.metrics" ' +
        'title="Port {{metric[\'tag.port\']}}">' +
        '<rpc-metric jmxdata="metric"></rpc-metric></pane>' +
        '</tabs>',
        controller: function ($http) {
            var ctrl = this;
            $http.get("/jmx?qry=Hadoop:service=*,name=RpcActivityForPort*")
                .then(function (result) {
                    ctrl.metrics = result.data.beans;
                })
        }
    });
    angular.module('ozone').component('rpcMetric', {
        bindings: {
            jmxdata: '<'
        },
        templateUrl: 'static/templates/rpc-metrics.html',
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
                    groupedMetrics.percentiles[window] = groupedMetrics.percentiles[window] || {};
                    groupedMetrics.percentiles[window][metricName] = groupedMetrics.percentiles[window][metricName] || {
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


                        groupedMetrics.percentiles[window][metricName].graphdata[0]
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
                        groupedMetrics.percentiles[window][metricName].numOps = metrics[key];
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
    angular.module('ozone')
        .component('tabs', {
            transclude: true,
            controller: function ($scope) {
                var ctrl = this;
                var panes = this.panes = [];
                this.select = function (pane) {
                    angular.forEach(panes, function (pane) {
                        pane.selected = false;
                    });
                    pane.selected = true;
                };
                this.addPane = function (pane) {
                    if (panes.length === 0) {
                        this.select(pane);
                    }
                    panes.push(pane);
                };
                this.click = function(pane) {
                    ctrl.select(pane);
                }
            },
            template: '<div class="nav navtabs"><div class="container"><ul class="nav nav-pills">' +
            '<li ng-repeat="pane in $ctrl.panes" ng-class="{active:pane.selected}">' +
            '<a href="" ng-click="$ctrl.click(pane)">{{pane.title}}</a> ' +
            '</li> </ul></div><br/><div class="tab-content" ng-transclude></div> </div>'
        })
        .component('pane', {
            transclude: true,
            require: {
                tabsCtrl: '^tabs'
            },
            bindings: {
                title: '@'
            },
            controller: function () {
                this.$onInit = function () {
                    this.tabsCtrl.addPane(this);
                };
            },
            template: '<div class="tab-pane" ng-if="$ctrl.selected" ng-transclude></div>'
        });

    angular.module('ozone').component('commonTools', {
        templateUrl: '/static/templates/tools.html',
        controller: function ($http) {
            var ctrl = this;
            ctrl.docs = false;
            $http.head("/docs/index.html")
                .then(function (result) {
                    ctrl.docs = true;
                },function(){
                    ctrl.docs = false;
                });
        }
    });
})();
