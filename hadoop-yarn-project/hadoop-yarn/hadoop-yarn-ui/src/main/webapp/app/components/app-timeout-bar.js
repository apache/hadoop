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
import Converter from 'yarn-ui/utils/converter';

export default Ember.Component.extend({
  app: null,

  appTimeoutValue: function() {
    var timeoutValueInSecs = this.get("app.remainingTimeoutInSeconds");
    if (timeoutValueInSecs > -1) {
      return Converter.msToElapsedTime(timeoutValueInSecs * 1000);
    } else {
      return timeoutValueInSecs;
    }
  }.property("app.remainingTimeoutInSeconds"),

  isAppTimedOut: function() {
    if (this.get("app.remainingTimeoutInSeconds") > 0) {
      return false;
    } else {
      return true;
    }
  }.property("app.remainingTimeoutInSeconds"),

  appTimeoutBarStyle: function() {
    var remainingInSecs = this.get("app.remainingTimeoutInSeconds"),
        expiryTimestamp = Converter.dateToTimeStamp(this.get("app.applicationExpiryTime")),
        expiryInSecs = expiryTimestamp / 1000,
        startTimestamp = Converter.dateToTimeStamp(this.get("app.startTime")),
        startInSecs = startTimestamp / 1000,
        totalRunInSecs = 0,
        appRunDurationInSecs = 0,
        width = 0;

    if (remainingInSecs > 0) {
      totalRunInSecs = expiryInSecs - startInSecs;
      appRunDurationInSecs = totalRunInSecs - remainingInSecs;
      width = appRunDurationInSecs / totalRunInSecs * 100;
    }

    return "width: " + width + "%";
  }.property("app.remainingTimeoutInSeconds", "app.applicationExpiryTime", "app.startTime")
});
