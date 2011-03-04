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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;

public class TestUserResolve {

  static Path userlist;

  @BeforeClass
  public static void writeUserList() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.getLocal(conf);
    final Path wd = new Path(new Path(
          System.getProperty("test.build.data", "/tmp")).makeQualified(fs),
        "gridmixUserResolve");
    userlist = new Path(wd, "users");
    FSDataOutputStream out = null;
    try {
      out = fs.create(userlist, true);
      out.writeBytes("user0,groupA,groupB,groupC\n");
      out.writeBytes("user1,groupA,groupC\n");
      out.writeBytes("user2,groupB\n");
      out.writeBytes("user3,groupA,groupB,groupC\n");
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  @Test
  public void testRoundRobinResolver() throws Exception {
    final Configuration conf = new Configuration();
    final UserResolver rslv = new RoundRobinUserResolver();

    boolean fail = false;
    try {
      rslv.setTargetUsers(null, conf);
    } catch (IOException e) {
      fail = true;
    }
    assertTrue("User list required for RoundRobinUserResolver", fail);

    rslv.setTargetUsers(new URI(userlist.toString()), conf);
    assertEquals("user0", rslv.getTargetUgi("hfre0").getUserName());
    assertEquals("user1", rslv.getTargetUgi("hfre1").getUserName());
    assertEquals("user2", rslv.getTargetUgi("hfre2").getUserName());
    assertEquals("user0", rslv.getTargetUgi("hfre0").getUserName());
    assertEquals("user3", rslv.getTargetUgi("hfre3").getUserName());
    assertEquals("user0", rslv.getTargetUgi("hfre0").getUserName());
    assertEquals("user0", rslv.getTargetUgi("hfre4").getUserName());
    assertArrayEquals(new String[] { "groupA", "groupB", "groupC" },
        rslv.getTargetUgi("hfre0").getGroupNames());
    assertArrayEquals(new String[] { "groupB" },
        rslv.getTargetUgi("hfre2").getGroupNames());
  }

  @Test
  public void testSubmitterResolver() throws Exception {
    final Configuration conf = new Configuration();
    final UserResolver rslv = new SubmitterUserResolver();
    rslv.setTargetUsers(null, conf);
    assertEquals(UnixUserGroupInformation.login(),
        rslv.getTargetUgi((UserGroupInformation)null));
  }

}
