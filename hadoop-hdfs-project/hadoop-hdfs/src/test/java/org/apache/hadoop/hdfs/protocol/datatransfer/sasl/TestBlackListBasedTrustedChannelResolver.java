/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlackListBasedTrustedChannelResolver;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for  {@link BlackListBasedTrustedChannelResolver}.
 */
public class TestBlackListBasedTrustedChannelResolver {

  private final static String FILE_NAME = "blacklistfile.txt";
  private File blacklistFile;
  private final static String BLACK_LISTED = "127.0.0.1\n216.58.216.174\n";
  private BlackListBasedTrustedChannelResolver resolver;

  @Before
  public void setup() {
    blacklistFile = new File(GenericTestUtils.getTestDir(), FILE_NAME);
    resolver
        = new BlackListBasedTrustedChannelResolver();
    try {
      FileUtils.write(blacklistFile, BLACK_LISTED);
    } catch (IOException e) {
      fail("Setup for TestBlackListBasedTrustedChannelResolver failed.");
    }
  }

  @After
  public void cleanUp() {
    FileUtils.deleteQuietly(blacklistFile);
  }

  @Test
  public void testBlackListIpClient() throws IOException {
    Configuration conf = new Configuration();
    FileUtils.write(blacklistFile,
        InetAddress.getLocalHost().getHostAddress(), true);
    conf.set(BlackListBasedTrustedChannelResolver
            .DFS_DATATRANSFER_CLIENT_FIXED_BLACK_LIST_FILE,
        blacklistFile.getAbsolutePath());

    resolver.setConf(conf);
    assertFalse(resolver.isTrusted());

  }

  @Test
  public void testBlackListIpServer() throws UnknownHostException {
    Configuration conf = new Configuration();
    conf.set(BlackListBasedTrustedChannelResolver
            .DFS_DATATRANSFER_SERVER_FIXED_BLACK_LIST_FILE,
        blacklistFile.getAbsolutePath());

    resolver.setConf(conf);
    assertTrue(resolver.isTrusted());
    assertFalse(resolver.isTrusted(InetAddress
        .getByName("216.58.216.174")));
  }
}
