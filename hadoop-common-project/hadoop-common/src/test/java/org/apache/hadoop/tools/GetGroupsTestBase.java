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
package org.apache.hadoop.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class GetGroupsTestBase {
  
  protected Configuration conf;
  private UserGroupInformation testUser1;
  private UserGroupInformation testUser2;
  
  protected abstract Tool getTool(PrintStream o);
  
  @BeforeEach
  public void setUpUsers() throws IOException {
    // Make sure the current user's info is in the list of test users.
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation.createUserForTesting(currentUser.getUserName(), currentUser.getGroupNames());
    
    testUser1 = UserGroupInformation.createUserForTesting("foo", new String[]{"bar", "baz"});
    testUser2 = UserGroupInformation.createUserForTesting("fiz", new String[]{"buz", "boz"});
  }
  
  @Test
  public void testNoUserGiven() throws Exception {
    String actualOutput = runTool(conf, new String[0], true);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    assertEquals(getExpectedOutput(currentUser), actualOutput,
        "No user provided should default to current user");
  }
  
  @Test
  public void testExistingUser() throws Exception {
    String actualOutput = runTool(conf, new String[]{testUser1.getUserName()}, true);
    assertEquals(getExpectedOutput(testUser1), actualOutput,
        "Show only the output of the user given");
  }
  
  @Test
  public void testMultipleExistingUsers() throws Exception {
    String actualOutput = runTool(conf,
        new String[]{testUser1.getUserName(), testUser2.getUserName()}, true);
    assertEquals(getExpectedOutput(testUser1) + getExpectedOutput(testUser2), actualOutput,
        "Show the output for both users given");
  }
  
  @Test
  public void testNonExistentUser() throws Exception {
    String actualOutput = runTool(conf,
        new String[]{"does-not-exist"}, true);
    assertEquals(getExpectedOutput(UserGroupInformation.createRemoteUser("does-not-exist")),
        actualOutput, "Show the output for only the user given, with no groups");
  }
  
  @Test
  public void testMultipleNonExistingUsers() throws Exception {
    String actualOutput = runTool(conf,
        new String[]{"does-not-exist1", "does-not-exist2"}, true);
    assertEquals(getExpectedOutput(UserGroupInformation.createRemoteUser("does-not-exist1")) +
        getExpectedOutput(UserGroupInformation.createRemoteUser("does-not-exist2")),
        actualOutput, "Show the output for only the user given, with no groups");
  }
  
  @Test
  public void testExistingInterleavedWithNonExistentUsers() throws Exception {
    String actualOutput = runTool(conf,
        new String[]{"does-not-exist1", testUser1.getUserName(),
                     "does-not-exist2", testUser2.getUserName()}, true);
    assertEquals(getExpectedOutput(UserGroupInformation.createRemoteUser("does-not-exist1")) +
        getExpectedOutput(testUser1) +
        getExpectedOutput(UserGroupInformation.createRemoteUser("does-not-exist2")) +
        getExpectedOutput(testUser2),
        actualOutput,
        "Show the output for only the user given, with no groups");
  }
  
  private static String getExpectedOutput(UserGroupInformation user) {
    String expectedOutput = user.getUserName() + " :";
    for (String group : user.getGroupNames()) {
      expectedOutput += " " + group;
    }
    return expectedOutput + System.getProperty("line.separator");
  }
  
  private String runTool(Configuration conf, String[] args, boolean success)
      throws Exception {
    ByteArrayOutputStream o = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(o, true);
    try {
      int ret = ToolRunner.run(getTool(out), args);
      assertEquals(success, ret == 0);
      return o.toString();
    } finally {
      o.close();
      out.close();
    }
  }
}
