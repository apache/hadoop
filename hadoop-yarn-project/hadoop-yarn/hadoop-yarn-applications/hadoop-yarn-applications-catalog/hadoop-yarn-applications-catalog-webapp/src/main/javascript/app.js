/*
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
var app = angular.module('app', [
   'ngRoute',
   'filters',
   'controllers'
]);

app.directive('jsonText', function() {
  return {
      restrict: 'A',
      require: 'ngModel',
      link: function(scope, element, attr, ngModel) {
        function into(input) {
          console.log(JSON.parse(input));
          return JSON.parse(input);
        }
        function out(data) {
          return JSON.stringify(data);
        }
        ngModel.$parsers.push(into);
        ngModel.$formatters.push(out);
      }
  };
});

app.config(['$routeProvider',
  function ($routeProvider) {
    $routeProvider.when('/', {
      templateUrl: 'partials/home.html',
      controller: 'AppStoreController'
    }).when('/app/:id', {
      templateUrl: 'partials/details.html',
      controller: 'AppDetailsController'
    }).when('/new', {
      templateUrl: 'partials/new.html',
      controller: 'NewAppController'
    }).when('/deploy/:id', {
      templateUrl: 'partials/deploy.html',
      controller: 'DeployAppController'
    }).when('/upgrade/:id', {
      templateUrl: 'partials/upgrade.html',
      controller: 'UpgradeAppController'
    }).otherwise({
      redirectTo: '/'
    });
}]);
