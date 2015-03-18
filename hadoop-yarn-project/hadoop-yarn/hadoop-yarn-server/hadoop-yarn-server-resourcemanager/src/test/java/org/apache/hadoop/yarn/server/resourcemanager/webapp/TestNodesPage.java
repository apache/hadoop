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
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodesPage.NodesBlock;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * This tests the NodesPage block table that it should contain the table body
 * data for all the columns in the table as specified in the header.
 */
public class TestNodesPage {
  
  final int numberOfRacks = 2;
  final int numberOfNodesPerRack = 6;
  // The following is because of the way TestRMWebApp.mockRMContext creates
  // nodes.
  final int numberOfLostNodesPerRack = numberOfNodesPerRack
      / NodeState.values().length;

  // Number of Actual Table Headers for NodesPage.NodesBlock might change in
  // future. In that case this value should be adjusted to the new value.
  final int numberOfThInMetricsTable = 20;
  final int numberOfActualTableHeaders = 13;

  private Injector injector;
  
  @Before
  public void setUp() throws Exception {
    final RMContext mockRMContext =
        TestRMWebApp.mockRMContext(3, numberOfRacks, numberOfNodesPerRack,
          8 * TestRMWebApp.GiB);
    injector =
        WebAppTests.createMockInjector(RMContext.class, mockRMContext,
          new Module() {
            @Override
            public void configure(Binder binder) {
              try {
                binder.bind(ResourceManager.class).toInstance(
                  TestRMWebApp.mockRm(mockRMContext));
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
            }
          });
  }

  @Test
  public void testNodesBlockRender() throws Exception {
    injector.getInstance(NodesBlock.class).render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    Mockito.verify(writer,
        Mockito.times(numberOfActualTableHeaders + numberOfThInMetricsTable))
        .print("<th");
    Mockito.verify(
        writer,
        Mockito.times(numberOfRacks * numberOfNodesPerRack
            * numberOfActualTableHeaders + numberOfThInMetricsTable)).print(
        "<td");
  }
  
  @Test
  public void testNodesBlockRenderForLostNodes() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.state", "lost");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    Mockito.verify(writer,
        Mockito.times(numberOfActualTableHeaders + numberOfThInMetricsTable))
        .print("<th");
    Mockito.verify(
        writer,
        Mockito.times(numberOfRacks * numberOfLostNodesPerRack
            * numberOfActualTableHeaders + numberOfThInMetricsTable)).print(
        "<td");
  }
  
  @Test
  public void testNodesBlockRenderForNodeLabelFilterWithNonEmptyLabel() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.label", "x");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    Mockito.verify(
        writer,
        Mockito.times(numberOfRacks
            * numberOfActualTableHeaders + numberOfThInMetricsTable)).print(
        "<td");
  }
  
  @Test
  public void testNodesBlockRenderForNodeLabelFilterWithEmptyLabel() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.label", "");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    Mockito.verify(
        writer,
        Mockito.times(numberOfRacks * (numberOfNodesPerRack - 1)
            * numberOfActualTableHeaders + numberOfThInMetricsTable)).print(
        "<td");
  }
  
  @Test
  public void testNodesBlockRenderForNodeLabelFilterWithAnyLabel() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.label", "*");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    Mockito.verify(
        writer,
        Mockito.times(numberOfRacks * numberOfNodesPerRack
            * numberOfActualTableHeaders + numberOfThInMetricsTable)).print(
        "<td");
  }
}
