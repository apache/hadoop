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
var controllers = angular.module("controllers", []);

controllers.controller("AppListController", [ '$scope', '$rootScope', '$http',
    function($scope, $rootScope, $http) {
      $scope.appList = [];

      function successCallback(response) {
        $scope.appList = response.data;
        $rootScope.$emit("hideLoadScreen", {});
      }

      function errorCallback(response) {
        $rootScope.$emit("hideLoadScreen", {});
        console.log("Error in downloading application list");
      }

      $rootScope.$on("RefreshAppList", function() {
        $scope.refreshList();
      });

      $scope.refreshList = function() {
        $http({
          method : 'GET',
          url : '/v1/app_list'
        }).then(successCallback, errorCallback);
      }

      $scope.deleteApp = function(id, name) {
        $rootScope.$emit("showLoadScreen", {});
        $http({
          method: 'DELETE',
          url: '/v1/app_list/' + id + '/' + name
        }).then(function(response) {
          $rootScope.$emit("RefreshAppList", {});
          window.location = '/#';
        }, function(response) {
          console.log(response);
        });
      }
      $http({
        method : 'GET',
        url : '/v1/app_list'
      }).then(successCallback, errorCallback);
    } ]);

controllers.controller("AppStoreController", [ '$scope', '$rootScope', '$http',
    function($scope, $rootScope, $http) {
      $scope.canDeployApp = function() {
        return false;
      };
      $scope.appStore = [];
      $scope.searchText = null;

      function successCallback(response) {
        $scope.appStore = response.data;
      }

      function errorCallback(response) {
        console.log("Error in downloading AppStore information.");
      }

      $scope.deployApp = function(id) {
        window.location = '/#!/deploy/' + id;
      }

      $http({
        method : 'GET',
        url : '/v1/app_store/recommended'
      }).then(successCallback, errorCallback);

      $scope.change = function(text) {
        var q = $scope.searchText;
        $http({
          method : 'GET',
          url : '/v1/app_store/search?q=' + q
        }).then(successCallback, errorCallback);
      }
    } ]);

controllers.controller("AppDetailsController", [ '$scope', '$interval', '$rootScope', '$http',
    '$routeParams', function($scope, $interval, $rootScope, $http, $routeParams) {
      $scope.details = {"yarnfile":{"state":"UNKNOWN"}};
      $scope.appName = $routeParams.id;
      var timer = $interval(function() {
        $scope.refreshAppDetails();
      }, 2000);

      $scope.refreshAppDetails = function() {
        $http({
          method : 'GET',
          url : '/v1/app_details/status/' + $scope.appName
        }).then(successCallback, errorCallback);
      }

      $scope.stopApp = function(id) {
        $http({
          method : 'POST',
          url : '/v1/app_details/stop/' + id
        }).then(function(data, status, header, config) {
          $scope.refreshAppDetails();
        }, errorCallback);
      }

      $scope.restartApp = function(id) {
        $http({
          method : 'POST',
          url : '/v1/app_details/restart/' + id
        }).then(function(data, status, header, config) {
          $scope.refreshAppDetails();
        }, errorCallback);
      }

      $scope.upgradeApp = function(id) {
        window.location = '/#!/upgrade/' + id;
      }

      $scope.canDeployApp = function() {
        return true;
      };

      $scope.checkServiceLink = function() {
        if ($scope.details.yarnfile.state != "STABLE") {
          return true;
        } else {
          return false;
        }
      }

      function successCallback(response) {
        if (response.data.yarnfile.components.length!=0) {
          $scope.details = response.data;
        } else {
          // When application is in accepted or failed state, it does not
          // have components detail, hence we update states only.
          $scope.details.yarnfile.state = response.data.yarnfile.state;
        }
      }

      function errorCallback(response) {
        console.log("Error in getting application detail");
      }

      $rootScope.$on("RefreshAppDetails", function() {
        $scope.refreshAppDetails();
      });

      $scope.$on("$locationChangeStart", function() {
        $interval.cancel(timer);
      });

      $scope.$on('$destroy', function() {
        $interval.cancel(timer);
      });

      $http({
        method : 'GET',
        url : '/v1/app_details/config/' + $scope.appName
      }).then(successCallback, errorCallback);

    } ]);

controllers.controller("NewAppController", [ '$scope', '$rootScope', '$http', function($scope, $rootScope, $http) {
    $scope.details = {
        "name" : "",
        "version" : "",
        "organization" : "",
        "description" : "",
        "quicklinks": {
          "UI": "http://${SERVICE_NAME}.${USER}.${DOMAIN}:8080/"
        },
        "icon" : "",
        "components" : [
          {
            "name" : "",
            "number_of_containers" : 1,
            "artifact" : {
              "id": "centos:latest"
            },
            "launch_command": "",
            "resource" : {
              "cpus" : 1,
              "memory" : 2048
            },
            "run_privileged_container" : false,
            "dependencies" : [],
            "placement_policy" : {
              "constraints" : []
            },
            "configuration" : {
              "env" : {
                "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE":"true"
              },
              "properties" : {
                "docker.network":"host"
              }
            }
          }
        ]
    };

    $scope.template = {
        "name" : "",
        "number_of_containers" : 1,
        "artifact" : {
          "id": "centos:latest"
        },
        "launch_command": "",
        "resource" : {
          "cpus" : 1,
          "memory" : 2048
        },
        "run_privileged_container" : false,
        "dependencies" : [],
        "placement_policy" : {
          "constraints" : []
        },
        "configuration" : {
          "env" : {
            "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE":"true"
          },
          "properties" : {
            "docker.network":"host"
          }
        }
    };

    $scope.message = null;
    $scope.error = null;

    $scope.save = function() {
      $http({
        method : 'POST',
        url : '/v1/app_store/register',
        data : JSON.stringify($scope.details)
      }).then(successCallback, errorCallback)
    }

    $scope.add = function() {
      $scope.details.components.push($scope.template);
    }

    $scope.remove = function(index) {
      $scope.details.components.splice(index, 1);
    }

    function successCallback(response) {
      $scope.message = "Application published successfully.";
      setTimeout(function() {
        $scope.$apply(function() {
          window.location = '/#';
        });
      }, 5000);
    }

    function errorCallback(response) {
      $scope.error = "Error in registering application configuration.";
    }

    } ]);

controllers.controller("DeployAppController", [ '$scope', '$rootScope', '$http',
    '$routeParams', function($scope, $rootScope, $http, $routeParams) {
    $scope.id = $routeParams.id;

    function successCallback(response) {
      $scope.details = response.data;
      $rootScope.$emit("hideLoadScreen", {});
    }

    function errorCallback(response) {
      $rootScope.$emit("hideLoadScreen", {});
      console.log("Error in downloading application template.");
    }

    $scope.launchApp = function(app) {
      $rootScope.$emit("showLoadScreen", {});
      $http({
        method : 'POST',
        url : '/v1/app_list/' + $scope.id,
        data : JSON.stringify($scope.details.app)
      }).then(function(data, status, headers, config) {
        $rootScope.$emit("RefreshAppList", {});
        window.location = '/#!/app/' + data.data.id;
      }, function(data, status, headers, config) {
        console.log('error', data, status);
      });
    }

    $http({
      method : 'GET',
      url : '/v1/app_store/get/' + $scope.id
    }).then(successCallback, errorCallback);

}]);

controllers.controller("UpgradeAppController", [ '$scope', '$rootScope', '$http',
    '$routeParams', function($scope, $rootScope, $http, $routeParams) {
    $scope.message = null;
    $scope.error = null;
    $scope.appName = $routeParams.id;
    $scope.refreshAppDetails = function() {
      $http({
        method : 'GET',
        url : '/v1/app_details/status/' + $scope.appName
      }).then(successCallback, errorCallback);
    }

    $scope.upgradeApp = function(app) {
      $rootScope.$emit("showLoadScreen", {});
      $http({
        method : 'PUT',
        url : '/v1/app_details/upgrade/' + $scope.appName,
        data : JSON.stringify($scope.details)
      }).then(function(data, status, headers, config) {
        $rootScope.$emit("RefreshAppList", {});
        window.location = '/#!/app/' + data.data.id;
      }, function(data, status, headers, config) {
        $rootScope.$emit("hideLoadScreen", {});
        $scope.error = data.data;
        $('#error-message').html(data.data);
        $('#myModal').modal('show');
        console.log('error', data, status);
      });
    }

    function successCallback(response) {
      if (response.data.yarnfile.components.length!=0) {
        $scope.details = response.data.yarnfile;
      } else {
        // When application is in accepted or failed state, it does not
        // have components detail, hence we update states only.
        $scope.details.state = response.data.yarnfile.state;
      }
    }

    function errorCallback(response) {
      $rootScope.$emit("hideLoadScreen", {});
      $scope.error = "Error in getting application detail.";
      $('#error-message').html($scope.error);
      $('#myModal').modal('show');
    }

    $scope.refreshAppDetails();
}]);

controllers.controller("LoadScreenController", [ '$scope', '$rootScope', '$http', function($scope, $rootScope, $http) {
  $scope.loadScreen = "hide";

  $rootScope.$on("showLoadScreen", function() {
    $scope.loadScreen = "show";
  });

  $rootScope.$on("hideLoadScreen", function() {
    $scope.loadScreen = "hide";
  });

}]);
