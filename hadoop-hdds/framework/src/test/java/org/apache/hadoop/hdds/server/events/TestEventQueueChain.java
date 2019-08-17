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
package org.apache.hadoop.hdds.server.events;

import org.junit.Test;

/**
 * More realistic event test with sending event from one listener.
 */
public class TestEventQueueChain {

  private static final Event<FailedNode> DECOMMISSION =
      new TypedEvent<>(FailedNode.class);

  private static final Event<FailedNode> DECOMMISSION_START =
      new TypedEvent<>(FailedNode.class);

  @Test
  public void simpleEvent() {
    EventQueue queue = new EventQueue();

    queue.addHandler(DECOMMISSION, new PipelineManager());
    queue.addHandler(DECOMMISSION_START, new NodeWatcher());

    queue.fireEvent(DECOMMISSION, new FailedNode("node1"));

    queue.processAll(5000);
  }


  static class FailedNode {
    private final String nodeId;

    FailedNode(String nodeId) {
      this.nodeId = nodeId;
    }

    String getNodeId() {
      return nodeId;
    }
  }

  private static class PipelineManager implements EventHandler<FailedNode> {

    @Override
    public void onMessage(FailedNode message, EventPublisher publisher) {

      System.out.println(
          "Closing pipelines for all pipelines including node: " + message
              .getNodeId());

      publisher.fireEvent(DECOMMISSION_START, message);
    }

  }

  private static class NodeWatcher implements EventHandler<FailedNode> {

    @Override
    public void onMessage(FailedNode message, EventPublisher publisher) {
      System.out.println("Clear timer");
    }
  }
}