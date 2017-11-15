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

export default Ember.Controller.extend({
  componentName: '',
  instanceName: '',
  serviceName: '',
  appId: '',

  breadcrumbs: [{
    text: "Home",
    routeName: 'application'
  }, {
    text: "Services",
    routeName: 'yarn-services',
  }],

  updateBreadcrumbs(appId, serviceName, componentName, instanceName) {
    var crumbs =  [{
      text: "Home",
      routeName: 'application'
    }, {
      text: "Services",
      routeName: 'yarn-services',
    }];
    if (appId && serviceName && componentName && instanceName) {
      crumbs.push({
        text: `${serviceName} [${appId}]`,
        href: `#/yarn-app/${appId}/info?service=${serviceName}`
      }, {
        text: 'Components',
        href: `#/yarn-app/${appId}/components?service=${serviceName}`
      }, {
        text: `${componentName}`,
        href: `#/yarn-component-instances/${componentName}/info?service=${serviceName}&&appid=${appId}`
      }, {
        text: `${instanceName}`
      });
    }
    this.set('breadcrumbs', crumbs);
  }
});
