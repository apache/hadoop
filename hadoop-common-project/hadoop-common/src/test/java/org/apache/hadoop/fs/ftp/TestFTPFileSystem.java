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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Preconditions;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test basic @{link FTPFileSystem} class methods. Contract tests are in
 * TestFTPContractXXXX.
 */
@Timeout(180)
public class TestFTPFileSystem {
  public static final Logger LOG = LoggerFactory.getLogger(TestFTPFileSystem.class);

  private FtpTestServer server;
  private java.nio.file.Path testDir;

  @BeforeEach
  public void setUp() throws Exception {
    testDir = Files.createTempDirectory(
        GenericTestUtils.getTestDir().toPath(), getClass().getName()
    );
    server = new FtpTestServer(testDir).start();
  }

  @AfterEach
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void tearDown() throws Exception {
    if (server != null) {
      server.stop();
      Files.walk(testDir)
          .sorted(Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  public void testCreateWithWritePermissions() throws Exception {
    BaseUser user = server.addUser("test", "password", new WritePermission());
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "ftp:///");
    configuration.set("fs.ftp.host", "localhost");
    configuration.setInt("fs.ftp.host.port", server.getPort());
    configuration.set("fs.ftp.user.localhost", user.getName());
    configuration.set("fs.ftp.password.localhost", user.getPassword());
    configuration.setBoolean("fs.ftp.impl.disable.cache", true);

    FileSystem fs = FileSystem.get(configuration);
    byte[] bytesExpected = "hello world".getBytes(StandardCharsets.UTF_8);
    try (FSDataOutputStream outputStream = fs.create(new Path("test1.txt"))) {
      outputStream.write(bytesExpected);
    }
    try (FSDataInputStream input = fs.open(new Path("test1.txt"))) {
      assertThat(bytesExpected).isEqualTo(IOUtils.readFullyToByteArray(input));
    }
  }

  @Test
  public void testCreateWithoutWritePermissions() throws Exception {
    BaseUser user = server.addUser("test", "password");
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "ftp:///");
    configuration.set("fs.ftp.host", "localhost");
    configuration.setInt("fs.ftp.host.port", server.getPort());
    configuration.set("fs.ftp.user.localhost", user.getName());
    configuration.set("fs.ftp.password.localhost", user.getPassword());
    configuration.setBoolean("fs.ftp.impl.disable.cache", true);

    FileSystem fs = FileSystem.get(configuration);
    byte[] bytesExpected = "hello world".getBytes(StandardCharsets.UTF_8);
    LambdaTestUtils.intercept(
        IOException.class, "Unable to create file: test1.txt, Aborting",
        () -> {
          try (FSDataOutputStream out = fs.create(new Path("test1.txt"))) {
            out.write(bytesExpected);
          }
        }
    );
  }

  @Test
  public void testFTPDefaultPort() throws Exception {
    FTPFileSystem ftp = new FTPFileSystem();
    assertEquals(FTP.DEFAULT_PORT, ftp.getDefaultPort());
  }

  @Test
  public void testFTPTransferMode() throws Exception {
    Configuration conf = new Configuration();
    FTPFileSystem ftp = new FTPFileSystem();
    assertEquals(FTP.BLOCK_TRANSFER_MODE, ftp.getTransferMode(conf));

    conf.set(FTPFileSystem.FS_FTP_TRANSFER_MODE, "STREAM_TRANSFER_MODE");
    assertEquals(FTP.STREAM_TRANSFER_MODE, ftp.getTransferMode(conf));

    conf.set(FTPFileSystem.FS_FTP_TRANSFER_MODE, "COMPRESSED_TRANSFER_MODE");
    assertEquals(FTP.COMPRESSED_TRANSFER_MODE, ftp.getTransferMode(conf));

    conf.set(FTPFileSystem.FS_FTP_TRANSFER_MODE, "invalid");
    assertEquals(FTPClient.BLOCK_TRANSFER_MODE, ftp.getTransferMode(conf));
  }

  @Test
  public void testFTPDataConnectionMode() throws Exception {
    Configuration conf = new Configuration();
    FTPClient client = new FTPClient();
    FTPFileSystem ftp = new FTPFileSystem();
    assertEquals(FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE,
        client.getDataConnectionMode());

    ftp.setDataConnectionMode(client, conf);
    assertEquals(FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE,
        client.getDataConnectionMode());

    conf.set(FTPFileSystem.FS_FTP_DATA_CONNECTION_MODE, "invalid");
    ftp.setDataConnectionMode(client, conf);
    assertEquals(FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE,
        client.getDataConnectionMode());

    conf.set(FTPFileSystem.FS_FTP_DATA_CONNECTION_MODE,
        "PASSIVE_LOCAL_DATA_CONNECTION_MODE");
    ftp.setDataConnectionMode(client, conf);
    assertEquals(FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE,
        client.getDataConnectionMode());

  }

  @Test
  public void testGetFsAction(){
    FTPFileSystem ftp = new FTPFileSystem();
    int[] accesses = new int[] {FTPFile.USER_ACCESS, FTPFile.GROUP_ACCESS,
        FTPFile.WORLD_ACCESS};
    FsAction[] actions = FsAction.values();
    for(int i = 0; i < accesses.length; i++){
      for(int j = 0; j < actions.length; j++){
        enhancedAssertEquals(actions[j], ftp.getFsAction(accesses[i],
            getFTPFileOf(accesses[i], actions[j])));
      }
    }
  }

  private void enhancedAssertEquals(FsAction actionA, FsAction actionB){
    String notNullErrorMessage = "FsAction cannot be null here.";
    Preconditions.checkNotNull(actionA, notNullErrorMessage);
    Preconditions.checkNotNull(actionB, notNullErrorMessage);
    String errorMessageFormat = "expect FsAction is %s, whereas it is %s now.";
    String notEqualErrorMessage = String.format(errorMessageFormat,
        actionA.name(), actionB.name());
    assertEquals(actionA, actionB, notEqualErrorMessage);
  }

  private FTPFile getFTPFileOf(int access, FsAction action) {
    boolean check = access == FTPFile.USER_ACCESS ||
                      access == FTPFile.GROUP_ACCESS ||
                      access == FTPFile.WORLD_ACCESS;
    String errorFormat = "access must be in [%d,%d,%d], but it is %d now.";
    String errorMessage = String.format(errorFormat, FTPFile.USER_ACCESS,
         FTPFile.GROUP_ACCESS, FTPFile.WORLD_ACCESS, access);
    Preconditions.checkArgument(check, errorMessage);
    Preconditions.checkNotNull(action);
    FTPFile ftpFile = new FTPFile();

    if(action.implies(FsAction.READ)){
      ftpFile.setPermission(access, FTPFile.READ_PERMISSION, true);
    }

    if(action.implies(FsAction.WRITE)){
      ftpFile.setPermission(access, FTPFile.WRITE_PERMISSION, true);
    }

    if(action.implies(FsAction.EXECUTE)){
      ftpFile.setPermission(access, FTPFile.EXECUTE_PERMISSION, true);
    }

    return ftpFile;
  }

  @Test
  public void testFTPSetTimeout() {
    Configuration conf = new Configuration();
    FTPClient client = new FTPClient();
    FTPFileSystem ftp = new FTPFileSystem();

    ftp.setTimeout(client, conf);
    assertEquals(client.getControlKeepAliveTimeout(),
        FTPFileSystem.DEFAULT_TIMEOUT);

    long timeout = 600;
    conf.setLong(FTPFileSystem.FS_FTP_TIMEOUT, timeout);
    ftp.setTimeout(client, conf);
    assertEquals(client.getControlKeepAliveTimeout(), timeout);
  }

  private static void touch(FileSystem fs, Path filePath) throws IOException {
    touch(fs, filePath, new byte[] {1});
  }

  private static void touch(FileSystem fs, Path path, byte[] data)
          throws IOException {
    try (FSDataOutputStream out = fs.create(path)) {
      if (data != null) {
        out.write(data);
      }
    }
  }

  /**
   * Test renaming a file.
   *
   * @throws Exception
   */
  @Test
  public void testRenameFileWithFullQualifiedPath() throws Exception {
    BaseUser user = server.addUser("test", "password", new WritePermission());
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "ftp:///");
    configuration.set("fs.ftp.host", "localhost");
    configuration.setInt("fs.ftp.host.port", server.getPort());
    configuration.set("fs.ftp.user.localhost", user.getName());
    configuration.set("fs.ftp.password.localhost", user.getPassword());
    configuration.setBoolean("fs.ftp.impl.disable.cache", true);

    FileSystem fs = FileSystem.get(configuration);

    Path ftpDir = fs.makeQualified(new Path(testDir.toAbsolutePath().toString()));
    Path file1 = fs.makeQualified(new Path(ftpDir, "renamefile" + "1"));
    Path file2 = fs.makeQualified(new Path(ftpDir, "renamefile" + "2"));

    touch(fs, file1);
    FTPClient ftpClient = connect(configuration);
    FTPFile[] files = ftpClient.listFiles("/D/projects/github/apache/hadoop/hadoop-common-project/hadoop-common/target/test/data");
    FTPFile[] files1 = ftpClient.listFiles("D:/projects/github/apache/hadoop/hadoop-common-project/hadoop-common/target/test/data");
    FTPFile[] files2 = ftpClient.listFiles("D:\\projects\\github\\apache\\hadoop\\hadoop-common-project\\hadoop-common\\target\\test\\data");
    assertTrue(fs.rename(file1, file2));
  }

  private FTPClient connect(Configuration conf) throws IOException {
    FTPClient client = null;
    String host = conf.get("fs.ftp.host");
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    String user = conf.get("fs.ftp.user." + host);
    String password = conf.get("fs.ftp.password." + host);
    client = new FTPClient();
    FTPClientConfig config = new FTPClientConfig(FTPClientConfig.SYST_NT);
    client.configure(config);
    client.connect(host, port);
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw NetUtils.wrapException(host, port,
          NetUtils.UNKNOWN_HOST, 0,
          new ConnectException("Server response " + reply));
    } else if (client.login(user, password)) {
      client.setFileTransferMode(getTransferMode(conf));
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.setBufferSize(FTPFileSystem.DEFAULT_BUFFER_SIZE);
      setTimeout(client, conf);
      setDataConnectionMode(client, conf);
    } else {
      throw new IOException("Login failed on server - " + host + ", port - "
          + port + " as user '" + user + "'");
    }

    client.enterLocalActiveMode();
    client.initiateListParsing();

    client.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true));
    return client;
  }

  private
  int getTransferMode(Configuration conf) {
    final String mode = conf.get(FTPFileSystem.FS_FTP_TRANSFER_MODE);
    // FTP default is STREAM_TRANSFER_MODE, but Hadoop FTPFS's default is
    // FTP.BLOCK_TRANSFER_MODE historically.
    int ret = FTP.BLOCK_TRANSFER_MODE;
    if (mode == null) {
      return ret;
    }
    final String upper = mode.toUpperCase();
    if (upper.equals("STREAM_TRANSFER_MODE")) {
      ret = FTP.STREAM_TRANSFER_MODE;
    } else if (upper.equals("COMPRESSED_TRANSFER_MODE")) {
      ret = FTP.COMPRESSED_TRANSFER_MODE;
    } else {
      if (!upper.equals("BLOCK_TRANSFER_MODE")) {
        LOG.warn("Cannot parse the value for " + FTPFileSystem.FS_FTP_TRANSFER_MODE
            + ": {}. Using default.", mode);
      }
    }
    return ret;
  }

  private void setTimeout(FTPClient client, Configuration conf) {
    long timeout = conf.getLong(FTPFileSystem.FS_FTP_TIMEOUT, FTPFileSystem.DEFAULT_TIMEOUT);
    client.setControlKeepAliveTimeout(timeout);
  }

  private void setDataConnectionMode(FTPClient client, Configuration conf) throws IOException {
    final String mode = conf.get(FTPFileSystem.FS_FTP_DATA_CONNECTION_MODE);
    if (mode == null) {
      return;
    }
    final String upper = mode.toUpperCase();
    if (upper.equals("PASSIVE_LOCAL_DATA_CONNECTION_MODE")) {
      client.enterLocalPassiveMode();
    } else if (upper.equals("PASSIVE_REMOTE_DATA_CONNECTION_MODE")) {
      client.enterRemotePassiveMode();
    } else {
      if (!upper.equals("ACTIVE_LOCAL_DATA_CONNECTION_MODE")) {
        LOG.warn(
            "Cannot parse the value for " + FTPFileSystem.FS_FTP_DATA_CONNECTION_MODE + ": " + mode
                + ". Using default.");
      }
    }
  }
}
