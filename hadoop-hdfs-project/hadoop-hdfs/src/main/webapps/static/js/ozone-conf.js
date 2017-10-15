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
var app = angular.module('app', []);
app.controller('tagController', function($scope, $http,$filter) {
    $scope.configArray = [];
    $scope.selectedTags = [];

    $http.get("/conf?cmd=getOzoneTags&group=ozone")
        .then(function(response) {
            $scope.tags = response.data;

      var idx = $scope.tags.indexOf('CBLOCK');
        // Remove CBLOCK related properties
        if (idx > -1) {
            console.log('Removing cblock configs')
            $scope.tags.splice(idx, 1);
        }
      $scope.loadAll();
        });

    $scope.convertToArray = function(configAr) {
        $scope.configArray = configAr;
    }

    $scope.loadAll = function() {
      console.log("Displaying all configs");
        $http.get("/conf?cmd=getPropertyByTag&tags=" + $scope.tags + "&group=ozone").then(function(response) {
            $scope.configs = response.data;
            $scope.convertToArray($scope.configs);
            $scope.sortBy('name');
        });
    };

    $scope.selected = function(tag) {
        var idx = $scope.selectedTags.indexOf(tag);
        // Is currently selected
        if (idx > -1) {
            $scope.selectedTags.splice(idx, 1);
        } else {
            $scope.selectedTags.push(tag);
        }
        console.log("Tags selected:" + $scope.selectedTags);
        $scope.reloadConfig();
    };

    $scope.reloadConfig = function() {
        if ($scope.selectedTags.length > 0) {
            console.log("Displaying configs for:" + $scope.selectedTags);
            $http.get("/conf?cmd=getPropertyByTag&tags=" + $scope.selectedTags + "&group=ozone").then(function(response) {
                $scope.configs = response.data;
                $scope.convertToArray($scope.configs);
            });
        }
      else {
            $scope.loadAll();
      }

    };

    $scope.filterTags = function(tag) {
        return (!['KSM', 'SCM', 'OZONE','CBLOCK'].includes(tag));
    };

    $scope.filterConfig = function(filter) {
        $http.get("/conf?cmd=getPropertyByTag&tags=" + filter + "&group=ozone").then(function(response) {
          var tmpConfig = response.data;
          console.log('filtering config for tag:'+filter);
          array3 = [];

          for(var i1 in tmpConfig) {

             for(var i2 in  $scope.configs) {
              if(tmpConfig[i1].name == $scope.configs[i2].name){
                array3.push( tmpConfig[i1]);
              }
            }
          }
        $scope.convertToArray(array3);
        });
    };

  $scope.sortBy = function(propertyName) {
    $scope.reverse = ($scope.propertyName === propertyName) ? !$scope.reverse : false;
    $scope.propertyName = propertyName;
  };

});