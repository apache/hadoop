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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * This is an evolving engine to mock YARN operations.
 */
public class MockYarnEngine {
  private static final Logger LOG =
      LoggerFactory.getLogger(MockYarnEngine.class);

  private MockYarnCluster cluster;
  private Allocator allocator;
  private List<ContainerRequestOperation> pending = new ArrayList<>();

  private ApplicationId appId = new MockApplicationId(0, 0);

  private ApplicationAttemptId attemptId = new MockApplicationAttemptId(appId,
      1);

  @Override
  public String toString() {
    return "MockYarnEngine " + cluster + " + pending=" + pending.size();
  }

  public int containerCount() {
    return cluster.containersInUse();
  }

  public MockYarnEngine(int clusterSize, int containersPerNode) {
    cluster = new MockYarnCluster(clusterSize, containersPerNode);
    allocator = new Allocator(cluster);
  }

  public MockYarnCluster getCluster() {
    return cluster;
  }

  public Allocator getAllocator() {
    return allocator;
  }

  /**
   * Allocate a container from a request. The containerID will be
   * unique, nodeId and other fields chosen internally with
   * no such guarantees; resource and priority copied over
   * @param request request
   * @return container
   */
  public Container allocateContainer(AMRMClient.ContainerRequest request) {
    MockContainer allocated = allocator.allocate(request);
    if (allocated != null) {
      MockContainerId id = (MockContainerId)allocated.getId();
      id.setApplicationAttemptId(attemptId);
    }
    return allocated;
  }

  MockYarnCluster.MockYarnClusterContainer releaseContainer(ContainerId
      containerId) {
    return cluster.release(containerId);
  }

  /**
   * Process a list of operations -release containers to be released,
   * allocate those for which there is space (but don't rescan the list after
   * the scan).
   * @param ops
   * @return
   */
  public List<Container> execute(List<AbstractRMOperation> ops) {
    return execute(ops, new ArrayList<>());
  }

  /**
   * Process a list of operations -release containers to be released,
   * allocate those for which there is space (but don't rescan the list after
   * the scan). Unsatisifed entries are appended to the "pending" list
   * @param ops operations
   * @return the list of all satisfied operations
   */
  public List<Container> execute(List<AbstractRMOperation> ops,
                               List<ContainerId> released) {
    validateRequests(ops);
    List<Container> allocation = new ArrayList<>();
    for (AbstractRMOperation op : ops) {
      if (op instanceof ContainerReleaseOperation) {
        ContainerReleaseOperation cro = (ContainerReleaseOperation) op;
        ContainerId cid = cro.getContainerId();
        assertNotNull(releaseContainer(cid));
        released.add(cid);
      } else if (op instanceof CancelSingleRequest) {
        // no-op
        LOG.debug("cancel request {}", op);
      } else if (op instanceof ContainerRequestOperation) {
        ContainerRequestOperation req = (ContainerRequestOperation) op;
        Container container = allocateContainer(req.getRequest());
        if (container != null) {
          LOG.info("allocated container {} for {}", container, req);
          allocation.add(container);
        } else {
          LOG.debug("Unsatisfied allocation {}", req);
          pending.add(req);
        }
      } else {
        LOG.warn("Unsupported operation {}", op);
      }
    }
    return allocation;
  }

  /**
   * Try and mimic some of the logic of <code>AMRMClientImpl
   * .checkLocalityRelaxationConflict</code>.
   * @param ops operations list
   */
  void validateRequests(List<AbstractRMOperation> ops) {
    // run through the requests and verify that they are all consistent.
    List<ContainerRequestOperation> outstandingRequests = new ArrayList<>();
    for (AbstractRMOperation operation : ops) {
      if (operation instanceof ContainerRequestOperation) {
        ContainerRequestOperation containerRequest =
            (ContainerRequestOperation) operation;
        ContainerRequest amRequest = containerRequest.getRequest();
        Priority priority = amRequest.getPriority();
        boolean relax = amRequest.getRelaxLocality();

        for (ContainerRequestOperation req : outstandingRequests) {
          if (req.getPriority() == priority && req.getRelaxLocality() !=
              relax) {
            // mismatch in values
            Assert.fail("operation " + operation + " has incompatible request" +
                    " priority from outsanding request");
          }
          outstandingRequests.add(containerRequest);

        }

      }
    }
  }

  /**
   * Get the list of node reports. These are not cloned; updates will persist
   * in the nodemap.
   * @return current node report list
   */
  List<NodeReport> getNodeReports() {
    return cluster.getNodeReports();
  }
}
