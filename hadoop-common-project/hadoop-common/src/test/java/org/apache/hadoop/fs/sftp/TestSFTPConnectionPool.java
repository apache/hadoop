/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.password.UserAuthPasswordFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.*;

public class TestSFTPConnectionPool {

  @Rule public TestName name = new TestName();


  @Test
  public void testConnectionPool() throws Exception {

    MockSFTPConnectionPool pool = new MockSFTPConnectionPool(2);
    SFTPConnectionPool.ConnectionInfo info = new SFTPConnectionPool.ConnectionInfo("localhost", 1234, "user");
    pool.manualAddToPool(new MockChannelSFTP(), info);
    pool.manualAddToPool(new MockChannelSFTP(), info);

    assertEquals(2, pool.getIdleCount());

    ChannelSftp channel1FromPool = pool.getFromPool(info);
    assertNotNull(channel1FromPool);

    assertEquals(1, pool.getIdleCount());

    ChannelSftp channel2FromPool = pool.getFromPool(info);
    assertNotNull(channel2FromPool);

    assertEquals(0, pool.getIdleCount());

    ChannelSftp channel3FromPool = pool.getFromPool(info);
    assertNull(channel3FromPool);

  }

  public static class MockSFTPConnectionPool extends SFTPConnectionPool {

    MockSFTPConnectionPool(int maxConnection) {
      super(maxConnection);
    }

    public void manualAddToPool(MockChannelSFTP channel, SFTPConnectionPool.ConnectionInfo info) {
      this.con2infoMap.put(channel, info);
      this.returnToPool(channel);
    }
  }
  public static class MockChannelSFTP extends ChannelSftp {

    @Override
    public boolean isConnected() {
      return true;
    }
  }
}
