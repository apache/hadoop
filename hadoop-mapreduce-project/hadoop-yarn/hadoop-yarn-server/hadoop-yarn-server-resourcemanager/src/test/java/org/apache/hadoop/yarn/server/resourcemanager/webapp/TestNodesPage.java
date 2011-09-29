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

import java.io.PrintWriter;

import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodesPage.NodesBlock;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * This tests the NodesPage block table that it should contain the table body
 * data for all the columns in the table as specified in the header.
 */
public class TestNodesPage {

  @Test
  public void testNodesBlockRender() throws Exception {
    int numberOfRacks = 2;
    int numberOfNodesPerRack = 2;
    // Number of Actual Table Headers for NodesPage.NodesBlock might change in
    // future. In that case this value should be adjusted to the new value.
    int numberOfActualTableHeaders = 7;

    PrintWriter writer = WebAppTests.testBlock(
        NodesBlock.class,
        RMContext.class,
        TestRMWebApp.mockRMContext(3, numberOfRacks, numberOfNodesPerRack,
            8 * TestRMWebApp.GiB)).getInstance(PrintWriter.class);

    Mockito.verify(writer, Mockito.times(numberOfActualTableHeaders)).print(
        "<th");
    Mockito.verify(
        writer,
        Mockito.times(numberOfRacks * numberOfNodesPerRack
            * numberOfActualTableHeaders)).print("<td");
  }
}
