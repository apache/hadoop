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
  selectedContainerThreaddumpContent: "",
  containerNodeMappings: [],
  threaddumpErrorMessage: "",
  stdoutLogsFetchFailedError: "",
  appAttemptToStateMappings: [],

  _isLoadingTopPanel: false,
  _isLoadingBottomPanel: false,
  _isThreaddumpAttemptFailed: false,
  _isStdoutLogsFetchFailed: false,

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
        this.fetchRunningContainersForAttemptId(attemptId)
          .then(hash => {
            let containers = null;
            let containerIdArr = {};
            var containerNodeMapping = null;
            var nodeHttpAddress = null;

            // Getting RUNNING containers from the RM first.
            if (
              hash.rmContainers.get("length") > 0 &&
              hash.rmContainers.get("content")
            ) {
                // Create a container-to-NMnode mapping for later use.
                hash.rmContainers.get("content").forEach(
                  function(containerInfo) {
                    nodeHttpAddress = containerInfo._data.nodeHttpAddress;
                    nodeHttpAddress = nodeHttpAddress
                                        .replace(/(^\w+:|^)\/\//, '');
                      containerNodeMapping = Ember.Object.create({
                        name: containerInfo.id,
                        value: nodeHttpAddress
                      });
                      this.containerNodeMappings.push(containerNodeMapping);
                      containerIdArr[containerInfo.id] = true;
                }.bind(this));

                containers = (containers || []).concat(
                  hash.rmContainers.get("content")
                );
            }

            this.set("attemptContainerList", containers);
            this.initializeSelect(".js-fetch-threaddump-containers");

            if (containerId) {
              this.send("showThreaddumpForContainer", containerId);
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
        this.set("selectedContainerThreaddumpContent", "");
        this.set("containerNodeMappings", []);
      }
    },

    showThreaddumpForContainer(containerIdForThreaddump) {
      Ember.$("#logContentTextArea1234554321").val("");
      this.set("showFullThreaddump", false);
      this.set("selectedContainerId", containerIdForThreaddump);
      this.set("_isLoadingBottomPanel", true);
      this.set("_isStdoutLogsFetchFailed", false);
      this.set("stdoutLogsFetchFailedError", "");
      this.set("_isThreaddumpAttemptFailed", false);
      this.set("threaddumpErrorMessage", "");

      if (containerIdForThreaddump) {
        this.set("selectedContainerThreaddumpContent", "");

        this.fetchThreaddumpForContainer(containerIdForThreaddump)
          .then(function() {
            Ember.Logger.log("Threaddump attempt has been successful!");

            var currentContainerId = null;
            var currentContainerNode = null;
            var nodeForContainerThreaddump = null;

            // Fetch the NM node for the selected container from the
            // mappings stored above when
            this.containerNodeMappings.forEach(function(item) {
              currentContainerId = item.name;
              currentContainerNode = item.value;

              if ((currentContainerId === containerIdForThreaddump)
                    && nodeForContainerThreaddump == null) {
                nodeForContainerThreaddump = currentContainerNode;
              }
            });

            if (nodeForContainerThreaddump) {
              // Fetch stdout logs after 1.2 seconds of a successful POST
              // request to RM. This is to allow for sufficient time for the NM
              // to heartbeat into RM (default of 1 second) whereupon it will
              // receive RM's signal to post a threaddump that will be captured
              // in the stdout logs of the container. The extra 200ms is to
              // allow for any network/IO delays.
              Ember.run.later((function() {
                this.fetchLogsForContainer(nodeForContainerThreaddump,
                                                containerIdForThreaddump,
                                                "stdout")
                .then(
                  hash => {
                   this.set("selectedContainerThreaddumpContent",
                              hash.logs.get('logs').trim());
                }.bind(this))
                .catch(function(error) {
                  this.set("_isStdoutLogsFetchFailed", true);
                  this.set("stdoutLogsFetchFailedError", error);
                }.bind(this))
                .finally(function() {
                  this.set("_isLoadingBottomPanel", false);
                }.bind(this));
              }.bind(this)), 1200);
            }
          }.bind(this), function(error) {
            this.set("_isThreaddumpAttemptFailed", true);
            this.set("threaddumpErrorMessage", error);
            this.set("_isLoadingBottomPanel", false);
          }.bind(this)).finally(function() {
            this.set("selectedContainerThreaddumpContent", "");
          }.bind(this));
      } else {
        this.set("selectedContainerId", "");
        this.set("selectedContainerThreaddumpContent", "");
      }
    }
  },

  // Set up the running attempt for the first time threaddump button is clicked.
  runningAttempt: Ember.computed("model.attempts", function() {
    let attempts = this.get("model.attempts");
    let runningAttemptId = null;

    if (attempts && attempts.get("length") && attempts.get("content")) {
      attempts.get("content").forEach(function(appAttempt) {
        if (appAttempt._data.state === "RUNNING") {
          runningAttemptId = appAttempt._data.appAttemptId;
        }
      });
    }

    return runningAttemptId;
  }),

  /**
   * Create and send fetch running containers request.
   */
  fetchRunningContainersForAttemptId(attemptId) {
    let request = {};

    request["rmContainers"] = this.store
      .query("yarn-container", {
        app_attempt_id: attemptId
      })
      .catch(function(error) {
        return Ember.A();
      });

    return Ember.RSVP.hash(request);
  },

  /**
   * Create and send fetch logs request for a selected container, from a
   * specific node.
   */
  fetchLogsForContainer(nodeForContainer, containerId, logFile) {
    let request = {};

    request["logs"] = this.store
      .queryRecord("yarn-node-container-log", {
        nodeHttpAddr: nodeForContainer,
        containerId: containerId,
        fileName: logFile
      })

    return Ember.RSVP.hash(request);
  },

  /**
   * Send out a POST request to RM to signal a threaddump for the selected
   * container for the currently logged-in user.
   */
  fetchThreaddumpForContainer(containerId) {
    var adapter = this.store.adapterFor("yarn-container-threaddump");

    let requestedUser = "";
    if (this.model
        && this.model.userInfo
        && this.model.userInfo.get('firstObject')) {
      requestedUser = this.model.userInfo
                        .get('firstObject')
                        .get('requestedUser');
    }

    return adapter.signalContainerForThreaddump({}, containerId, requestedUser);
  },

  /**
   * Reset attributes after a refresh.
   */
  resetAfterRefresh() {
    this.set("selectedAttemptId", "");
    this.set("attemptContainerList", null);
    this.set("selectedContainerId", "");
    this.set("selectedLogFileName", "");
    this.set("containerLogFiles", null);
    this.set("selectedContainerThreaddumpContent", "");
    this.set("containerNodeMappings", []);
    this.set("_isThreaddumpAttemptFailed", false);
    this.set("threaddumpErrorMessage", "");
    this.set("_isStdoutLogsFetchFailed", false);
    this.set("stdoutLogsFetchFailedError", "");
    this.set("appAttemptToStateMappings", []);
  },

  // Set the threaddump content in the content area.
  showThreaddumpContent: Ember.computed(
    "selectedContainerThreaddumpContent",
    function() {
      return this.get("selectedContainerThreaddumpContent");
    }
  ),

  /**
   * Check if the application for the container whose threaddump is being attempted
   * is even running.
   */
  isRunningApp: Ember.computed('model.app.state', function() {
    return this.get('model.app.state') === "RUNNING";
  })
});
