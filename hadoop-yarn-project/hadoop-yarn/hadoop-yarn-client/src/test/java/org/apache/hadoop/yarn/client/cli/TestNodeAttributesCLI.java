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

package org.apache.hadoop.yarn.client.cli;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

/**
 * Test class for TestNodeAttributesCLI.
 */
public class TestNodeAttributesCLI {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNodeAttributesCLI.class);
  private ResourceManagerAdministrationProtocol admin;
  private NodesToAttributesMappingRequest request;
  private NodeAttributesCLI nodeAttributesCLI;
  private ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();
  private String errOutput;

  @Before
  public void configure() throws IOException, YarnException {
    admin = mock(ResourceManagerAdministrationProtocol.class);

    when(admin.mapAttributesToNodes(any(NodesToAttributesMappingRequest.class)))
        .thenAnswer(new Answer<NodesToAttributesMappingResponse>() {
          @Override
          public NodesToAttributesMappingResponse answer(
              InvocationOnMock invocation) throws Throwable {
            request =
                (NodesToAttributesMappingRequest) invocation.getArguments()[0];
            return NodesToAttributesMappingResponse.newInstance();
          }
        });

    nodeAttributesCLI = new NodeAttributesCLI(new Configuration()) {
      @Override
      protected ResourceManagerAdministrationProtocol createAdminProtocol()
          throws IOException {
        return admin;
      }
    };

    nodeAttributesCLI.setErrOut(new PrintStream(errOutBytes));
  }

  @Test
  public void testHelp() throws Exception {
    String[] args = new String[] { "-help", "-replace" };
    assertTrue("It should have succeeded help for replace", 0 == runTool(args));
    assertOutputContains(
        "-replace <\"node1:attribute[(type)][=value],attribute1"
            + "[=value],attribute2  node2:attribute2[=value],attribute3\"> :");
    assertOutputContains("Replace the node to attributes mapping information at"
        + " the ResourceManager with the new mapping. Currently supported"
        + " attribute type. And string is the default type too. Attribute value"
        + " if not specified for string type value will be considered as empty"
        + " string. Replaced node-attributes should not violate the existing"
        + " attribute to attribute type mapping.");

    args = new String[] { "-help", "-remove" };
    assertTrue("It should have succeeded help for replace", 0 == runTool(args));
    assertOutputContains(
        "-remove <\"node1:attribute,attribute1" + " node2:attribute2\"> :");
    assertOutputContains("Removes the specified node to attributes mapping"
        + " information at the ResourceManager");

    args = new String[] { "-help", "-add" };
    assertTrue("It should have succeeded help for replace", 0 == runTool(args));
    assertOutputContains("-add <\"node1:attribute[(type)][=value],"
        + "attribute1[=value],attribute2  node2:attribute2[=value],attribute3\">"
        + " :");
    assertOutputContains("Adds or updates the node to attributes mapping"
        + " information at the ResourceManager. Currently supported attribute"
        + " type is string. And string is the default type too. Attribute value"
        + " if not specified for string type value will be considered as empty"
        + " string. Added or updated node-attributes should not violate the"
        + " existing attribute to attribute type mapping.");

    args = new String[] { "-help", "-failOnUnknownNodes" };
    assertTrue("It should have succeeded help for replace", 0 == runTool(args));
    assertOutputContains("-failOnUnknownNodes :");
    assertOutputContains("Can be used optionally along with other options. When"
        + " its set, it will fail if specified nodes are unknown.");
  }

  @Test
  public void testReplace() throws Exception {
    // --------------------------------
    // failure scenarios
    // --------------------------------
    // parenthesis not match
    String[] args = new String[] { "-replace", "x(" };
    assertTrue("It should have failed as no node is specified",
        0 != runTool(args));
    assertFailureMessageContains(NodeAttributesCLI.INVALID_MAPPING_ERR_MSG);

    // parenthesis not match
    args = new String[] { "-replace", "x:(=abc" };
    assertTrue(
        "It should have failed as no closing parenthesis is not specified",
        0 != runTool(args));
    assertFailureMessageContains(
        "Attribute for node x is not properly configured : (=abc");

    args = new String[] { "-replace", "x:()=abc" };
    assertTrue("It should have failed as no type specified inside parenthesis",
        0 != runTool(args));
    assertFailureMessageContains(
        "Attribute for node x is not properly configured : ()=abc");

    args = new String[] { "-replace", ":x(string)" };
    assertTrue("It should have failed as no node is specified",
        0 != runTool(args));
    assertFailureMessageContains("Node name cannot be empty");

    // Not expected key=value specifying inner parenthesis
    args = new String[] { "-replace", "x:(key=value)" };
    assertTrue(0 != runTool(args));
    assertFailureMessageContains(
        "Attribute for node x is not properly configured : (key=value)");

    // Should fail as no attributes specified
    args = new String[] { "-replace" };
    assertTrue("Should fail as no attribute mappings specified",
        0 != runTool(args));
    assertFailureMessageContains(NodeAttributesCLI.NO_MAPPING_ERR_MSG);

    // no labels, should fail
    args = new String[] { "-replace", "-failOnUnknownNodes",
        "x:key(string)=value,key2=val2" };
    assertTrue("Should fail as no attribute mappings specified for replace",
        0 != runTool(args));
    assertFailureMessageContains(NodeAttributesCLI.NO_MAPPING_ERR_MSG);

    // no labels, should fail
    args = new String[] { "-replace", " " };
    assertTrue(0 != runTool(args));
    assertFailureMessageContains(NodeAttributesCLI.NO_MAPPING_ERR_MSG);

    args = new String[] { "-replace", ", " };
    assertTrue(0 != runTool(args));
    assertFailureMessageContains(NodeAttributesCLI.INVALID_MAPPING_ERR_MSG);
    // --------------------------------
    // success scenarios
    // --------------------------------
    args = new String[] { "-replace",
        "x:key(string)=value,key2=val2 y:key2=val23,key3 z:key4" };
    assertTrue("Should not fail as attribute has been properly mapped",
        0 == runTool(args));
    List<NodeToAttributes> nodeAttributesList = new ArrayList<>();
    List<NodeAttribute> attributes = new ArrayList<>();
    attributes.add(
        NodeAttribute.newInstance("key", NodeAttributeType.STRING, "value"));
    attributes.add(
        NodeAttribute.newInstance("key2", NodeAttributeType.STRING, "val2"));
    nodeAttributesList.add(NodeToAttributes.newInstance("x", attributes));

    // for node y
    attributes = new ArrayList<>();
    attributes.add(
        NodeAttribute.newInstance("key2", NodeAttributeType.STRING, "val23"));
    attributes
        .add(NodeAttribute.newInstance("key3", NodeAttributeType.STRING, ""));
    nodeAttributesList.add(NodeToAttributes.newInstance("y", attributes));

    // for node y
    attributes = new ArrayList<>();
    attributes.add(
        NodeAttribute.newInstance("key2", NodeAttributeType.STRING, "val23"));
    attributes
        .add(NodeAttribute.newInstance("key3", NodeAttributeType.STRING, ""));
    nodeAttributesList.add(NodeToAttributes.newInstance("y", attributes));

    // for node z
    attributes = new ArrayList<>();
    attributes
        .add(NodeAttribute.newInstance("key4", NodeAttributeType.STRING, ""));
    nodeAttributesList.add(NodeToAttributes.newInstance("z", attributes));

    NodesToAttributesMappingRequest expected =
        NodesToAttributesMappingRequest.newInstance(
            AttributeMappingOperationType.REPLACE, nodeAttributesList, false);
    assertTrue(request.equals(expected));
  }

  @Test
  public void testRemove() throws Exception {
    // --------------------------------
    // failure scenarios
    // --------------------------------
    // parenthesis not match
    String[] args = new String[] { "-remove", "x:" };
    assertTrue("It should have failed as no node is specified",
        0 != runTool(args));
    assertFailureMessageContains(
        "Attributes cannot be null or empty for Operation REMOVE on the node x");
    // --------------------------------
    // success scenarios
    // --------------------------------
    args =
        new String[] { "-remove", "x:key2,key3 z:key4", "-failOnUnknownNodes" };
    assertTrue("Should not fail as attribute has been properly mapped",
        0 == runTool(args));
    List<NodeToAttributes> nodeAttributesList = new ArrayList<>();
    List<NodeAttribute> attributes = new ArrayList<>();
    attributes
        .add(NodeAttribute.newInstance("key2", NodeAttributeType.STRING, ""));
    attributes
        .add(NodeAttribute.newInstance("key3", NodeAttributeType.STRING, ""));
    nodeAttributesList.add(NodeToAttributes.newInstance("x", attributes));

    // for node z
    attributes = new ArrayList<>();
    attributes
        .add(NodeAttribute.newInstance("key4", NodeAttributeType.STRING, ""));
    nodeAttributesList.add(NodeToAttributes.newInstance("z", attributes));

    NodesToAttributesMappingRequest expected =
        NodesToAttributesMappingRequest.newInstance(
            AttributeMappingOperationType.REMOVE, nodeAttributesList, true);
    assertTrue(request.equals(expected));
  }

  @Test
  public void testAdd() throws Exception {
    // --------------------------------
    // failure scenarios
    // --------------------------------
    // parenthesis not match
    String[] args = new String[] { "-add", "x:" };
    assertTrue("It should have failed as no node is specified",
        0 != runTool(args));
    assertFailureMessageContains(
        "Attributes cannot be null or empty for Operation ADD on the node x");
    // --------------------------------
    // success scenarios
    // --------------------------------
    args = new String[] { "-add", "x:key2=123,key3=abc z:key4(string)",
        "-failOnUnknownNodes" };
    assertTrue("Should not fail as attribute has been properly mapped",
        0 == runTool(args));
    List<NodeToAttributes> nodeAttributesList = new ArrayList<>();
    List<NodeAttribute> attributes = new ArrayList<>();
    attributes.add(
        NodeAttribute.newInstance("key2", NodeAttributeType.STRING, "123"));
    attributes.add(
        NodeAttribute.newInstance("key3", NodeAttributeType.STRING, "abc"));
    nodeAttributesList.add(NodeToAttributes.newInstance("x", attributes));

    // for node z
    attributes = new ArrayList<>();
    attributes
        .add(NodeAttribute.newInstance("key4", NodeAttributeType.STRING, ""));
    nodeAttributesList.add(NodeToAttributes.newInstance("z", attributes));

    NodesToAttributesMappingRequest expected =
        NodesToAttributesMappingRequest.newInstance(
            AttributeMappingOperationType.ADD, nodeAttributesList, true);
    assertTrue(request.equals(expected));
  }

  private void assertFailureMessageContains(String... messages) {
    assertOutputContains(messages);
    assertOutputContains(NodeAttributesCLI.USAGE_YARN_NODE_ATTRIBUTES);
  }

  private void assertOutputContains(String... messages) {
    for (String message : messages) {
      if (!errOutput.contains(message)) {
        fail("Expected output to contain '" + message
            + "' but err_output was:\n" + errOutput);
      }
    }
  }

  private int runTool(String... args) throws Exception {
    errOutBytes.reset();
    LOG.info("Running: NodeAttributesCLI " + Joiner.on(" ").join(args));
    int ret = nodeAttributesCLI.run(args);
    errOutput = new String(errOutBytes.toByteArray(), Charsets.UTF_8);
    LOG.info("Err_output:\n" + errOutput);
    return ret;
  }
}
