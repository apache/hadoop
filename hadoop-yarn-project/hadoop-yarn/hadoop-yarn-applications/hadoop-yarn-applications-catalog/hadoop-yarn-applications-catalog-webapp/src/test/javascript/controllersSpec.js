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

describe('Controller tests', function () {

  // Unit test for listing, and start/stop applications.
  describe('AppListController', function() {
    var scope, ctrl, http, httpBackend;

    beforeEach(module('app'));
    beforeEach(inject(function ($controller, $rootScope, $http, $httpBackend) {
      scope = $rootScope.$new();
      rootScope = $rootScope;
      http = $http;
      httpBackend = $httpBackend;
      ctrl = $controller('AppListController', {$scope: scope});
    }));

    afterEach(function() {
      httpBackend.verifyNoOutstandingExpectation();
      httpBackend.verifyNoOutstandingRequest();
    });

    it('should contain appList', function () {
      httpBackend.expectGET('/v1/app_list').respond(200, [{id:"jenkins",name:"jenkins",app:"",yarnfile:{}}]);
      httpBackend.expectGET('partials/home.html').respond(200, "");
      httpBackend.flush();
      expect(scope.appList.length).toBe(1);
    });

    it('should test to delete app', function () {
      httpBackend.expectGET('/v1/app_list').respond(200, [{id:"jenkins",name:"jenkins",app:"",yarnfile:{}}]);
      httpBackend.expectDELETE('/v1/app_list/jenkins/jenkins').respond(200, {data:"Application Deleted."});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      spyOn(rootScope, '$emit');
      scope.$apply(function() {
        scope.deleteApp("jenkins","jenkins");
      });
      httpBackend.flush();
      expect(rootScope.$emit).toHaveBeenCalledWith('RefreshAppList', {});
    });

    it('should test to refresh appList', function() {
      spyOn(rootScope, '$emit');
      httpBackend.expectGET('/v1/app_list').respond(200, [{id:"jenkins",name:"jenkins",app:"",yarnfile:{}}]);
      httpBackend.expectGET('/v1/app_list').respond(200, [{id:"jenkins",name:"jenkins",app:"",yarnfile:{}}]);
      httpBackend.expectGET('partials/home.html').respond(200, "");
      scope.$apply(function() {
        scope.refreshList();
      });
      httpBackend.flush();
      expect(rootScope.$emit).toHaveBeenCalledWith('hideLoadScreen', {});
    })
  });

  // Unit test for inspect YARN application details.
  describe('AppDetailsController', function() {
    var scope, ctrl, http, routeParams, httpBackend;

    beforeEach(module('app'));
    beforeEach(inject(function ($controller, $rootScope, $http, $routeParams, $httpBackend) {
      scope = $rootScope.$new();
      rootScope = $rootScope;
      http = $http;
      routeParams = $routeParams;
      httpBackend = $httpBackend;
      ctrl = $controller('AppDetailsController', {$scope: scope});
    }));

    afterEach(function() {
      httpBackend.verifyNoOutstandingExpectation();
      httpBackend.verifyNoOutstandingRequest();
    });

    it('should contain unknown state', function () {
      httpBackend.expectGET('/v1/app_details/config/undefined').respond(200, {"yarnfile":{"state":"UNKNOWN","components":[]}});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      httpBackend.flush();
      expect(scope.details.yarnfile.state).toBe("UNKNOWN");
    });

    it('should run test to refrshed details', function () {
      httpBackend.expectGET('/v1/app_details/config/undefined').respond(200, {"yarnfile":{"state":"UNKNOWN","components":[]}});
      httpBackend.expectGET('/v1/app_details/status/aabbccdd').respond(200, {yarnfile:{state: "ACCEPTED", components:[]}});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      scope.$apply(function() {
        routeParams.id = "aabbccdd";
        scope.appName = "aabbccdd";
        scope.refreshAppDetails();
      });
      httpBackend.flush();
      expect(scope.details.yarnfile.state).toBe("ACCEPTED");
    });

    it('should run test to restart app', function () {
      httpBackend.expectGET('/v1/app_details/config/undefined').respond(200, {"yarnfile":{"state":"UNKNOWN","components":[]}});
      httpBackend.expectPOST('/v1/app_details/restart/aabbccdd').respond(200, {yarnfile:{state: "ACCEPTED", components:[]}});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      httpBackend.expectGET('/v1/app_details/status/undefined').respond(200, {yarnfile:{state: "ACCEPTED", components:[]}});
      scope.$apply(function() {
        scope.restartApp("aabbccdd");
      });
      httpBackend.flush();
      expect(scope.details.yarnfile.components).toBe();
    });

    it('should run test to stop app', function () {
      httpBackend.expectGET('/v1/app_details/config/undefined').respond(200, {"yarnfile":{"state":"UNKNOWN","components":[]}});
      httpBackend.expectPOST('/v1/app_details/stop/aabbccdd').respond(200, {yarnfile:{state: "STOPPED", components:[]}});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      httpBackend.expectGET('/v1/app_details/status/undefined').respond(200, {yarnfile:{state: "ACCEPTED", components:[]}});
      scope.$apply(function() {
        scope.stopApp("aabbccdd");
      });
      httpBackend.flush();
      expect(scope.details.yarnfile.components).toBe();
    });

  });

  // Unit test for deploying app, and search for apps from Yarn Appstore.
  describe('AppStoreController', function() {
    var scope, ctrl, http, httpBackend;

    beforeEach(module('app'));
    beforeEach(inject(function ($controller, $rootScope, $http, $httpBackend) {
      scope = $rootScope.$new();
      http = $http;
      httpBackend = $httpBackend;
      ctrl = $controller('AppStoreController', {$scope: scope});
    }));

    afterEach(function() {
      httpBackend.verifyNoOutstandingExpectation();
      httpBackend.verifyNoOutstandingRequest();
    });

    it('should contain appStore', function () {
      httpBackend.expectGET('/v1/app_store/recommended').respond(200, "");
      httpBackend.expectGET('partials/home.html').respond(200, "");
      httpBackend.flush();
      expect(scope.appStore.length).toBe(0);
    });

    it('should run test to deploy app', function() {
      httpBackend.expectGET('/v1/app_store/recommended').respond(200, "");
      httpBackend.expectGET('partials/home.html').respond(200, "");
      httpBackend.flush();
      scope.$apply(function() {
        scope.deployApp("aabbccdd");
      });
      expect(scope.appStore.length).toBe(0);
    });

    it('should run test to search for apps', function() {
      httpBackend.expectGET('/v1/app_store/recommended').respond(200, "");
      httpBackend.expectGET('/v1/app_store/search?q=aabbccdd').respond(204, {data:'ACCEPTED'});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      scope.$apply(function() {
        scope.searchText = "aabbccdd";
        scope.change("aabbccdd");
      });
      httpBackend.flush();
      expect(scope.appStore.data).toBe('ACCEPTED');
    });

  });

  // Unit test cases for creating a new YARN application.
  describe('NewAppController', function() {
    var scope, ctrl, http, httpBackend;

    beforeEach(module('app'));
    beforeEach(inject(function ($controller, $rootScope, $http, $httpBackend) {
      scope = $rootScope.$new();
      http = $http;
      httpBackend = $httpBackend;
      ctrl = $controller('NewAppController', {$scope: scope});
    }));

    afterEach(function() {
      httpBackend.verifyNoOutstandingExpectation();
      httpBackend.verifyNoOutstandingRequest();
    });

    it('should contain details', function () {
      httpBackend.expectGET('partials/home.html').respond(200, "");
      httpBackend.flush();
      expect(scope.details.name).toBe("");
    });

    it('should run test to register data to backend', function() {
      httpBackend.expectPOST('/v1/app_store/register').respond(204, {data:'ACCEPTED'});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      scope.$apply(function() {
        scope.save();
      });
      httpBackend.flush();
      expect(scope.message).toEqual("Application published successfully.");
    });

    it('should run test to fail register data to backend', function() {
      httpBackend.expectPOST('/v1/app_store/register').respond(500, {data:'INTERNAL SERVER ERROR'});
      httpBackend.expectGET('partials/home.html').respond(200, "");
      scope.$apply(function() {
        scope.save();
      });
      httpBackend.flush();
      expect(scope.error).toEqual("Error in registering application configuration.");
    });

    it('should run test to add more component to details', function() {
      httpBackend.expectGET('partials/home.html').respond(200, "");
      expect(scope.details.components.length).toEqual(1);
      scope.$apply(function() {
        scope.add();
      });
      httpBackend.flush();
      expect(scope.details.components.length).toEqual(2);
    });

    it('should run test to remove second component', function() {
      httpBackend.expectGET('partials/home.html').respond(200, "");
      expect(scope.details.components.length).toEqual(1);
      scope.$apply(function() {
        scope.add();
        scope.remove(1);
      });
      httpBackend.flush();
      expect(scope.details.components.length).toEqual(1);
    });
  });

});
