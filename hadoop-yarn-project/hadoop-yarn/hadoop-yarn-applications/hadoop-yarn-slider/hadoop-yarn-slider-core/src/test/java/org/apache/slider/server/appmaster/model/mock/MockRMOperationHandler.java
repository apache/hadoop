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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.operations.RMOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock RM operation handler.
 */
public class MockRMOperationHandler extends RMOperationHandler {
  protected static final Logger LOG =
      LoggerFactory.getLogger(MockRMOperationHandler.class);

  private List<AbstractRMOperation> operations = new ArrayList<>();
  private int requests;
  private int releases;
  // number available to cancel
  private int availableToCancel = 0;
  // count of cancelled values. This must be explicitly set
  private int cancelled;
  // number blacklisted
  private int blacklisted = 0;

  @Override
  public void releaseAssignedContainer(ContainerId containerId) {
    operations.add(new ContainerReleaseOperation(containerId));
    LOG.info("Releasing container ID " + containerId.getContainerId());
    releases++;
  }

  @Override
  public void addContainerRequest(AMRMClient.ContainerRequest req) {
    operations.add(new ContainerRequestOperation(req));
    LOG.info("Requesting container role #" + req.getPriority());
    requests++;
  }

  @Override
  public int cancelContainerRequests(
      Priority priority1,
      Priority priority2,
      int count) {
    int releaseable = Math.min(count, availableToCancel);
    availableToCancel -= releaseable;
    cancelled += releaseable;
    return releaseable;
  }

  @Override
  public void cancelSingleRequest(AMRMClient.ContainerRequest request) {
    // here assume that there is a copy of this request in the list
    if (availableToCancel > 0) {
      availableToCancel--;
      cancelled++;
    }
  }

  @Override
  public void updateBlacklist(List<String> blacklistAdditions, List<String>
      blacklistRemovals) {
    blacklisted += blacklistAdditions.size();
    blacklisted -= blacklistRemovals.size();
  }

  /**
   * Clear the history.
   */
  public void clear() {
    operations.clear();
    releases = 0;
    requests = 0;
  }

  public AbstractRMOperation getFirstOp() {
    return operations.get(0);
  }

  public int getNumReleases() {
    return releases;
  }

  public void setAvailableToCancel(int num) {
    this.availableToCancel = num;
  }

  public int getAvailableToCancel() {
    return availableToCancel;
  }

  public int getBlacklisted() {
    return blacklisted;
  }
}
