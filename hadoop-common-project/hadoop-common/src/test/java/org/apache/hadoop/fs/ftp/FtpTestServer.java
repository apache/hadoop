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

package org.apache.hadoop.fs.ftp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.impl.DefaultFtpServer;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;

/**
 * Helper class facilitating to manage a local ftp
 * server for unit tests purposes only.
 */
public class FtpTestServer {

  private int port;
  private Path ftpRoot;
  private UserManager userManager;
  private FtpServer server;

  public FtpTestServer(Path ftpRoot) {
    this.ftpRoot = ftpRoot;
    this.userManager = new PropertiesUserManagerFactory().createUserManager();
    FtpServerFactory serverFactory = createServerFactory();
    serverFactory.setUserManager(userManager);
    this.server = serverFactory.createServer();
  }

  public FtpTestServer start() throws Exception {
    server.start();
    Listener listener = ((DefaultFtpServer) server)
        .getListeners()
        .get("default");
    port = listener.getPort();
    return this;
  }

  public Path getFtpRoot() {
    return ftpRoot;
  }

  public int getPort() {
    return port;
  }

  public void stop() {
    if (!server.isStopped()) {
      server.stop();
    }
  }

  public BaseUser addUser(String name, String password,
      Authority... authorities) throws IOException, FtpException {

    BaseUser user = new BaseUser();
    user.setName(name);
    user.setPassword(password);
    Path userHome = Files.createDirectory(ftpRoot.resolve(name));
    user.setHomeDirectory(userHome.toString());
    user.setAuthorities(Arrays.asList(authorities));
    userManager.save(user);
    return user;
  }

  private FtpServerFactory createServerFactory() {
    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory defaultListener = new ListenerFactory();
    defaultListener.setPort(0);
    serverFactory.addListener("default", defaultListener.createListener());
    return serverFactory;
  }
}
