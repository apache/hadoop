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

export default Ember.Route.extend({
  actions: {
    /**
     * Base error handler for the application.
     * If specific routes do not handle the error, it will bubble up to
     * this handler. Here we redirect to either 404 page or a generic
     * error handler page.
     */
    error: function (error) {
      Ember.Logger.log(error.stack);

      if (error && error.errors[0] &&
          error.errors[0].status == 404) {
        this.intermediateTransitionTo('/notfound');
      } else {
        this.intermediateTransitionTo('/error');
      }
    }
  }
});
