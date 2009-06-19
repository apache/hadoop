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
package org.apache.hadoop.fs.ftp;

import java.io.File;
import java.net.URI;
import junit.framework.TestCase;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.UserManagerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * Generates a bunch of random files and directories using class 'DFSTestUtil',
 * stores them on the FTP file system, copies them and check if all the files
 * were retrieved successfully without any data corruption
 */
public class TestFTPFileSystem extends TestCase {

  private Configuration defaultConf = new Configuration();
  private FtpServer server = null;
  private FileSystem localFs = null;
  private FileSystem ftpFs = null;
  private Listener listener = null;

  private Path workDir = new Path(System.getProperty("test.build.data", "."));
  private File userFile = new File(System.getProperty("test.src.dir"), "ftp.user.properties");

  Path ftpServerRoot = new Path(workDir, "ftp-server");

  private void startServer() {
    try {
      FtpServerFactory serverFactory = new FtpServerFactory();
      ListenerFactory factory = new ListenerFactory();
      factory.setPort(0);
      listener = factory.createListener();
      serverFactory.addListener("default", listener);
      // Create a test user
      PropertiesUserManagerFactory userFactory = new PropertiesUserManagerFactory();
      userFactory.setFile(userFile);
      userFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor());
      UserManager userManager = userFactory.createUserManager();
      serverFactory.setUserManager(userManager);

      // Initialize the server and start.
      
      server = serverFactory.createServer();
      server.start();

    } catch (Exception e) {
      throw new RuntimeException("FTP server start-up failed", e);
    }
  }

  private void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  @Override
  public void setUp() throws Exception {
    startServer();
    defaultConf = new Configuration();
    localFs = FileSystem.getLocal(defaultConf);
    localFs.mkdirs(ftpServerRoot);
    int serverPort = listener.getPort();
    ftpFs = FileSystem.get(URI.create("ftp://admin:admin@localhost:"
        + serverPort), defaultConf);
  }

  @Override
  public void tearDown() throws Exception {
    localFs.delete(ftpServerRoot, true);
    localFs.close();
    ftpFs.close();
    stopServer();
  }

  /**
   * Tests FTPFileSystem, create(), open(), delete(), mkdirs(), rename(),
   * listStatus(), getStatus() APIs. *
   * 
   * @throws Exception
   */
  public void testReadWrite() throws Exception {

    DFSTestUtil util = new DFSTestUtil("TestFTPFileSystem", 20, 3, 1024 * 1024);
    localFs.setWorkingDirectory(workDir);
    Path localData = new Path(workDir, "srcData");
    Path remoteData = new Path("srcData");

    util.createFiles(localFs, localData.toUri().getPath());

    boolean dataConsistency = util.checkFiles(localFs, localData.getName());
    assertTrue("Test data corrupted", dataConsistency);

    // Copy files and directories recursively to FTP file system.
    boolean filesCopied = FileUtil.copy(localFs, localData, ftpFs, remoteData,
        false, defaultConf);
    assertTrue("Copying to FTPFileSystem failed", filesCopied);

    // Rename the remote copy
    Path renamedData = new Path("Renamed");
    boolean renamed = ftpFs.rename(remoteData, renamedData);
    assertTrue("Rename failed", renamed);

    // Copy files and directories from FTP file system and delete remote copy.
    filesCopied = FileUtil.copy(ftpFs, renamedData, localFs, workDir, true,
        defaultConf);
    assertTrue("Copying from FTPFileSystem fails", filesCopied);

    // Check if the data was received completely without any corruption.
    dataConsistency = util.checkFiles(localFs, renamedData.getName());
    assertTrue("Invalid or corrupted data recieved from FTP Server!",
        dataConsistency);

    // Delete local copies
    boolean deleteSuccess = localFs.delete(renamedData, true)
        & localFs.delete(localData, true);
    assertTrue("Local test data deletion failed", deleteSuccess);
  }
}
