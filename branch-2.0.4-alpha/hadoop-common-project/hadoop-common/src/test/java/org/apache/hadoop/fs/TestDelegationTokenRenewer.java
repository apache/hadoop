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
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;

import org.junit.Before;
import org.junit.Test;

public class TestDelegationTokenRenewer {
  private static final int RENEW_CYCLE = 1000;
  private static final int MAX_RENEWALS = 100;

  @SuppressWarnings("rawtypes")
  static class TestToken extends Token {
    public volatile int renewCount = 0;
    public volatile boolean cancelled = false;

    @Override
    public long renew(Configuration conf) {
      if (renewCount == MAX_RENEWALS) {
        Thread.currentThread().interrupt();
      } else {
        renewCount++;
      }
      return renewCount;
    }

    @Override
    public void cancel(Configuration conf) {
      cancelled = true;
    }
  }
  
  static class TestFileSystem extends FileSystem implements
      DelegationTokenRenewer.Renewable {
    private Configuration mockConf = mock(Configuration.class);;
    private TestToken testToken = new TestToken();

    @Override
    public Configuration getConf() {
      return mockConf;
    }

    @Override
    public Token<?> getRenewToken() {
      return testToken;
    }

    @Override
    public URI getUri() {
      return null;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
        IOException {
      return null;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
    }

    @Override
    public Path getWorkingDirectory() {
      return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return null;
    }

    @Override
    public <T extends TokenIdentifier> void setDelegationToken(Token<T> token) {
      return;
    }
  }

  private DelegationTokenRenewer renewer;

  @Before
  public void setup() {
    DelegationTokenRenewer.renewCycle = RENEW_CYCLE;
    renewer = DelegationTokenRenewer.getInstance();
  }

  @Test
  public void testAddRemoveRenewAction() throws IOException,
      InterruptedException {
    TestFileSystem tfs = new TestFileSystem();
    renewer.addRenewAction(tfs);
    assertEquals("FileSystem not added to DelegationTokenRenewer", 1,
        renewer.getRenewQueueLength());

    for (int i = 0; i < 60; i++) {
      Thread.sleep(RENEW_CYCLE);
      if (tfs.testToken.renewCount > 0) {
        renewer.removeRenewAction(tfs);
        break;
      }
    }

    assertTrue("Token not renewed even after 1 minute",
        (tfs.testToken.renewCount > 0));
    assertEquals("FileSystem not removed from DelegationTokenRenewer", 0,
        renewer.getRenewQueueLength());
    assertTrue("Token not cancelled", tfs.testToken.cancelled);
  }
}
