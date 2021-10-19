/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
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
import Constants from 'yarn-ui/constants';

export default Ember.Controller.extend({
  queryParams: ["service", "attempt", "containerid"],
  service: undefined,
  attempt: undefined,
  containerid: undefined,

  selectedAttemptId: "",
  attemptContainerList: null,
  selectedContainerId: "",
  selectedLogFileName: "",
  containerLogFiles: null,
  selectedLogFileContent: "",

  _isLoadingTopPanel: false,
  _isLoadingBottomPanel: false,

  initializeSelect: function(selector = ".js-fetch-attempt-containers") {
    Ember.run.schedule("afterRender", this, function() {
      $(selector).select2({ width: "350px", multiple: false });
    });
  },

  actions: {
    showContainersForAttemptId(attemptId, containerId = "") {
      this.set("selectedAttemptId", "");
      if (attemptId) {
        this.set("_isLoadingTopPanel", true);
        this.set("selectedAttemptId", attemptId);
        this.fetchContainersForAttemptId(attemptId)
          .then(hash => {
            let containers = null;
            let containerIdArr = {};

            // Getting running containers from the RM first
            if (
              hash.rmContainers.get("length") > 0 &&
              hash.rmContainers.get("content")
            ) {
              hash.rmContainers.get("content").forEach(function(o) {
                containerIdArr[o.id] = true;
              }.bind(this));
              containers = (containers || []).concat(
                hash.rmContainers.get("content")
              );
            }

            let historyProvider = this.fallbackToJHS ? hash.jhsContainers : hash.tsContainers;
            let fieldName =  this.fallbackToJHS ? "containerId" : "id";

            // Getting aggregated containers from the selected history provider
            if (
              historyProvider.get("length") > 0 &&
              historyProvider.get("content")
            ) {
              let historyContainers = [];
              historyProvider.get("content").forEach(function(o) {
                if(!containerIdArr[o[fieldName]]) {
                  historyContainers.push(o);
                }
              }.bind(this));
              containers = (containers || []).concat(
                historyContainers);
            }

            this.set("attemptContainerList", containers);
            this.initializeSelect(".js-fetch-logs-containers");
            if (containerId) {
              this.send("showLogFilesForContainerId", containerId);
            }
          })
          .finally(() => {
            this.set("_isLoadingTopPanel", false);
          });
      } else {
        this.set("attemptContainerList", null);
        this.set("selectedContainerId", "");
        this.set("containerLogFiles", null);
        this.set("selectedLogFileName", "");
        this.set("selectedLogFileContent", "");
      }
    },

    showLogFilesForContainerId(containerId) {
      this.set("selectedContainerId", "");
      this.set("containerLogFiles", null);
      this.set("selectedLogFileName", "");
      this.set("selectedLogFileContent", "");

      if (containerId) {
        this.set("_isLoadingBottomPanel", true);
        this.set("selectedContainerId", containerId);
        this.fetchLogFilesForContainerId(containerId)
          .then(hash => {
            if (hash.logs.get("length") > 0) {
              this.set("containerLogFiles", hash.logs);
            } else {
              this.set("containerLogFiles", null);
            }
            this.initializeSelect(".js-fetch-log-for-container");
          })
          .finally(() => {
            this.set("_isLoadingBottomPanel", false);
          });
      }
    },

    showContentForLogFile(logFile) {
      this.set("selectedLogFileName", "");
      Ember.$("#logContentTextArea1234554321").val("");
      this.set("showFullLog", false);
      if (logFile) {
        this.set("_isLoadingBottomPanel", true);
        this.set("selectedLogFileName", logFile);
        var id = this.get("selectedContainerId") + Constants.PARAM_SEPARATOR + logFile;
        this.fetchContentForLogFile(id)
          .then(
            hash => {
              this.set("selectedLogFileContent", hash.logs.get('logs').trim());
            },
            () => {
              this.set("selectedLogFileContent", "");
            }
          )
          .then(() => {
            this.set("_isLoadingBottomPanel", false);
          });
      } else {
        this.set("selectedLogFileContent", "");
      }
    },

    findNextTextInLogContent() {
      let searchInputElem = document.getElementById("logSeachInput98765");
      this.send("searchTextInLogContent", searchInputElem.value);
    },

    searchTextInLogContent(searchText) {
      Ember.$("body").scrollTop(278);
      let textAreaElem = document.getElementById(
        "logContentTextArea1234554321"
      );
      let logContent = textAreaElem.innerText;
      let startIndex = this.searchTextStartIndex || 0;
      if (startIndex === -1) {
        startIndex = this.searchTextStartIndex = 0;
      }
      if (this.prevSearchText !== searchText) {
        startIndex = this.searchTextStartIndex = 0;
      }
      if (searchText && searchText.trim()) {
        searchText = searchText.trim();
        this.prevSearchText = searchText;
        if (startIndex === 0) {
          startIndex = logContent.indexOf(searchText, 0);
        }
        let endIndex = startIndex + searchText.length;
        if (document.createRange && window.getSelection) {
          let range = document.createRange();
          range.selectNodeContents(textAreaElem);
          range.setStart(textAreaElem.childNodes.item(0), startIndex);
          range.setEnd(textAreaElem.childNodes.item(0), endIndex);
          let selection = window.getSelection();
          selection.removeAllRanges();
          selection.addRange(range);
        }
        this.searchTextStartIndex = logContent.indexOf(
          searchText,
          endIndex + 1
        );
      } else {
        this.searchTextStartIndex = 0;
      }
    },

    showFullLogFileContent() {
      this.set("showFullLog", true);
      this.notifyPropertyChange("selectedLogFileContent");
    }
  },

  attemptList: Ember.computed("model.attempts", function() {
    let attempts = this.get("model.attempts");
    let list = null;
    if (attempts && attempts.get("length") && attempts.get("content")) {
      list = [].concat(attempts.get("content"));
    }
    return list;
  }),

  fetchContainersForAttemptId(attemptId) {
    let request = {};

    request["rmContainers"] = this.store
      .query("yarn-container", {
        app_attempt_id: attemptId
      })
      .catch(function(error) {
        return Ember.A();
      });

    let historyProvider = this.fallbackToJHS ? "jhsContainers" : "tsContainers";
    let historyQuery = this.fallbackToJHS ? "yarn-jhs-container" : "yarn-timeline-container";

    request[historyProvider] = this.store
      .query(historyQuery, {
        app_attempt_id: attemptId
      })
      .catch(function(error) {
        return Ember.A();
      });

    return Ember.RSVP.hash(request);
  },

  fetchLogFilesForContainerId(containerId) {
    let queryName = this.fallbackToJHS ? "yarn-jhs-log" : "yarn-log";
    let redirectQuery = queryName === "yarn-jhs-log" ? "yarn-jhs-redirect-log" : "yarn-redirect-log";

    return Ember.RSVP.hash({
      logs: this.resolveRedirectableQuery(
        this.store.query(queryName, { containerId }),
        m => {
          return m.map(model => model.get('redirectedUrl'))[0];
        },
        url => {
          return this.store.query(redirectQuery, url);
        })
    });
  },

  fetchContentForLogFile(id) {
    let queryName = this.fallbackToJHS ? 'yarn-app-jhs-log' : 'yarn-app-log';
    let redirectQuery = queryName === "yarn-app-jhs-log" ? "yarn-app-jhs-redirect-log" : "yarn-app-redirect-log";

    return Ember.RSVP.hash({
      logs: this.resolveRedirectableQuery(
        this.store.findRecord(queryName, id),
        m => {
          return m.get('redirectedUrl');
        },
        url => {
          return this.store.findRecord(redirectQuery, url + Constants.PARAM_SEPARATOR + id);
        })
    });
  },

  resolveRedirectableQuery(initial, urlResolver, redirectResolver) {
    return initial.then(m => {
      let redirectedUrl = urlResolver(m);
      if (redirectedUrl !== null && redirectedUrl !== undefined && redirectedUrl !== '') {
        let logFromRedirect = redirectResolver(redirectedUrl);
        return Promise.all([m, logFromRedirect]);
      } else {
        return Promise.all([m, null]);
      }
    })
      .then(([originalLog, logFromRedirect]) => {
        return logFromRedirect !== null ? logFromRedirect : originalLog;
      })
      .catch(function () {
        return Ember.A();
      });
  },

  resetAfterRefresh() {
    this.set("selectedAttemptId", "");
    this.set("attemptContainerList", null);
    this.set("selectedContainerId", "");
    this.set("selectedLogFileName", "");
    this.set("containerLogFiles", null);
    this.set("selectedLogFileContent", "");
  },

  showFullLog: false,

  showLastFewLinesOfLogContent: Ember.computed(
    "selectedLogFileContent",
    function() {
      let content = this.get("selectedLogFileContent");
      let lines = content.split("\n");
      if (this.get("showFullLog") || lines.length < 10) {
        return content;
      }
      return lines.slice(lines.length - 10).join("\n");
    }
  ),

  isLogAggregationNotSucceeded: Ember.computed("model.app", function() {
    const logAggregationStatus = this.get("model.app.logAggregationStatus");
    return logAggregationStatus !== "SUCCEEDED";
  }),

  fallbackToJHS: function() {
    // Let's fall back to JHS if ATS is not available, but JHS is.
    return this.model &&
      (!this.model.timelineHealth || this.model.timelineHealth.get('isTimelineUnHealthy')) &&
      this.model.jhsHealth && this.model.jhsHealth.get('isJHSHealthy');
  }.property('model.timelineHealth', 'model.isJHSHealthy'),

  areJHSandATSUnhealthy: function() {
    if (this.model && this.model.timelineHealth) {
      if (!this.model.timelineHealth.get('isTimelineUnHealthy')) {
        return false;
      }
    }
    if (this.model && this.model.jhsHealth) {
      if (this.model.jhsHealth.get('isJHSHealthy')) {
        return false;
      }
    }
    return true;
  }.property('model.timelineHealth', 'model.isJHSHealthy')
});
