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

package org.apache.hadoop.fs.contract.sftp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.sftp.SFTPFileSystem;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.password.UserAuthPasswordFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;

public class SFTPContract extends AbstractFSContract {

  private static final String CONTRACT_XML = "contract/sftp.xml";
  private static final URI TEST_URI =
      URI.create("sftp://user:password@localhost");
  private final String testDataDir =
      new FileSystemTestHelper().getTestRootDir();
  private final Configuration conf;
  private SshServer sshd;

  public SFTPContract(Configuration conf) {
    super(conf);
    addConfResource(CONTRACT_XML);
    this.conf = conf;
  }

  @Override
  public void init() throws IOException {
    sshd = SshServer.setUpDefaultServer();
    // ask OS to assign a port
    sshd.setPort(0);
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());

    List<NamedFactory<UserAuth>> userAuthFactories = new ArrayList<>();
    userAuthFactories.add(new UserAuthPasswordFactory());

    sshd.setUserAuthFactories(userAuthFactories);
    sshd.setPasswordAuthenticator((username, password, session) ->
        username.equals("user") && password.equals("password")
    );

    sshd.setSubsystemFactories(
        Collections.singletonList(new SftpSubsystemFactory()));

    sshd.start();
    int port = sshd.getPort();

    conf.setClass("fs.sftp.impl", SFTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.sftp.host.port", port);
    conf.setBoolean("fs.sftp.impl.disable.cache", true);
  }

  @Override
  public void teardown() throws IOException {
    if (sshd != null) {
      sshd.stop();
    }
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return FileSystem.get(TEST_URI, conf);
  }

  @Override
  public String getScheme() {
    return "sftp";
  }

  @Override
  public Path getTestPath() {
    try {
      FileSystem fs = FileSystem.get(
          URI.create("sftp://user:password@localhost"), conf
      );
      return fs.makeQualified(new Path(testDataDir));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
