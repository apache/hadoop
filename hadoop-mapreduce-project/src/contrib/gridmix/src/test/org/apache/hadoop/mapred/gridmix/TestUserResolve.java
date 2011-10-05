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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.net.URI;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class TestUserResolve {

  private static Path rootDir = null;
  private static Configuration conf = null;
  private static FileSystem fs = null;

  @BeforeClass
  public static void createRootDir() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    rootDir = new Path(new Path(System.getProperty("test.build.data", "/tmp"))
                 .makeQualified(fs), "gridmixUserResolve");
  }

  /**
   * Creates users file with the content as the String usersFileContent.
   * @param usersFilePath    the path to the file that is to be created
   * @param usersFileContent Content of users file
   * @throws IOException
   */
  private static void writeUserList(Path usersFilePath, String usersFileContent)
      throws IOException {

    FSDataOutputStream out = null;
    try {
      out = fs.create(usersFilePath, true);
      out.writeBytes(usersFileContent);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Validate RoundRobinUserResolver's behavior for bad user resource file.
   * RoundRobinUserResolver.setTargetUsers() should throw proper Exception for
   * the cases like
   * <li> non existent user resource file and
   * <li> empty user resource file
   * 
   * @param rslv              The RoundRobinUserResolver object
   * @param userRsrc          users file
   * @param expectedErrorMsg  expected error message
   */
  private void validateBadUsersFile(UserResolver rslv, URI userRsrc,
      String expectedErrorMsg) {
    boolean fail = false;
    try {
      rslv.setTargetUsers(userRsrc, conf);
    } catch (IOException e) {
      assertTrue("Exception message from RoundRobinUserResolver is wrong",
          e.getMessage().equals(expectedErrorMsg));
      fail = true;
    }
    assertTrue("User list required for RoundRobinUserResolver", fail);
  }

  /**
   * Validate the behavior of {@link RoundRobinUserResolver} for different
   * user resource files like
   * <li> Empty user resource file
   * <li> Non existent user resource file
   * <li> User resource file with valid content
   * @throws Exception
   */
  @Test
  public void testRoundRobinResolver() throws Exception {

    final UserResolver rslv = new RoundRobinUserResolver();
    Path usersFilePath = new Path(rootDir, "users");
    URI userRsrc = new URI(usersFilePath.toString());

    // Check if the error message is as expected for non existent
    // user resource file.
    fs.delete(usersFilePath, false);
    String expectedErrorMsg = "File " + userRsrc + " does not exist";
    validateBadUsersFile(rslv, userRsrc, expectedErrorMsg);

    // Check if the error message is as expected for empty user resource file
    writeUserList(usersFilePath, "");// creates empty users file
    expectedErrorMsg =
        RoundRobinUserResolver.buildEmptyUsersErrorMsg(userRsrc);
    validateBadUsersFile(rslv, userRsrc, expectedErrorMsg);

    // Create user resource file with valid content like older users list file
    // with usernames and groups
    writeUserList(usersFilePath,
        "user0,groupA,groupB,groupC\nuser1,groupA,groupC\n");
    validateValidUsersFile(rslv, userRsrc);

    // Create user resource file with valid content with
    // usernames with groups and without groups
    writeUserList(usersFilePath, "user0,groupA,groupB\nuser1,");
    validateValidUsersFile(rslv, userRsrc);

    // Create user resource file with valid content with
    // usernames without groups
    writeUserList(usersFilePath, "user0\nuser1");
    validateValidUsersFile(rslv, userRsrc);
  }

  // Validate RoundRobinUserResolver for the case of
  // user resource file with valid content.
  private void validateValidUsersFile(UserResolver rslv, URI userRsrc)
      throws IOException {
    assertTrue(rslv.setTargetUsers(userRsrc, conf));
    UserGroupInformation ugi1 = UserGroupInformation.createRemoteUser("hfre0");
    assertEquals("user0", rslv.getTargetUgi(ugi1).getUserName());
    assertEquals("user1", 
        rslv.getTargetUgi(UserGroupInformation.createRemoteUser("hfre1"))
            .getUserName());
    assertEquals("user0",
        rslv.getTargetUgi(UserGroupInformation.createRemoteUser("hfre2"))
            .getUserName());
    assertEquals("user0", rslv.getTargetUgi(ugi1).getUserName());
    assertEquals("user1",
        rslv.getTargetUgi(UserGroupInformation.createRemoteUser("hfre3"))
            .getUserName());

    // Verify if same user comes again, its mapped user name should be
    // correct even though UGI is constructed again.
    assertEquals("user0", rslv.getTargetUgi(
        UserGroupInformation.createRemoteUser("hfre0")).getUserName());
    assertEquals("user0",
        rslv.getTargetUgi(UserGroupInformation.createRemoteUser("hfre5"))
            .getUserName());
    assertEquals("user0",
        rslv.getTargetUgi(UserGroupInformation.createRemoteUser("hfre0"))
            .getUserName());
  }

  @Test
  public void testSubmitterResolver() throws Exception {
    final UserResolver rslv = new SubmitterUserResolver();
    assertFalse(rslv.needsTargetUsersList());
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertEquals(ugi, rslv.getTargetUgi((UserGroupInformation)null));
  }
}
