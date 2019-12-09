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

package org.apache.hadoop.yarn.logaggregation;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.TestContainerId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.util.Times;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestAggregatedLogFormat {

  private static final File testWorkDir = new File("target",
      "TestAggregatedLogFormat");
  private static final Configuration conf = new Configuration();
  private static final FileSystem fs;
  private static final char filler = 'x';
  private static final Log LOG = LogFactory
      .getLog(TestAggregatedLogFormat.class);

  static {
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  @After
  public void cleanupTestDir() throws Exception {
    Path workDirPath = new Path(testWorkDir.getAbsolutePath());
    LOG.info("Cleaning test directory [" + workDirPath + "]");
    fs.delete(workDirPath, true);
  }

  //Test for Corrupted AggregatedLogs. The Logs should not write more data
  //if Logvalue.write() is called and the application is still
  //appending to logs

  @Test
  public void testForCorruptedAggregatedLogs() throws Exception {
    Configuration conf = new Configuration();
    File workDir = new File(testWorkDir, "testReadAcontainerLogs1");
    Path remoteAppLogFile =
        new Path(workDir.getAbsolutePath(), "aggregatedLogFile");
    Path srcFileRoot = new Path(workDir.getAbsolutePath(), "srcFiles");
    ContainerId testContainerId = TestContainerId.newContainerId(1, 1, 1, 1);
    Path t =
        new Path(srcFileRoot, testContainerId.getApplicationAttemptId()
            .getApplicationId().toString());
    Path srcFilePath = new Path(t, testContainerId.toString());

    long numChars = 950000;

    writeSrcFileAndALog(srcFilePath, "stdout", numChars, remoteAppLogFile,
       srcFileRoot, testContainerId);

    LogReader logReader = new LogReader(conf, remoteAppLogFile);
    LogKey rLogKey = new LogKey();
    DataInputStream dis = logReader.next(rLogKey);
    Writer writer = new StringWriter();
    try {
      LogReader.readAcontainerLogs(dis, writer);
    } catch (Exception e) {
      if(e.toString().contains("NumberFormatException")) {
        Assert.fail("Aggregated logs are corrupted.");
      }
    }
  }

  private void writeSrcFileAndALog(Path srcFilePath, String fileName, final long length,
      Path remoteAppLogFile, Path srcFileRoot, ContainerId testContainerId)
      throws Exception {
    File dir = new File(srcFilePath.toString());
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IOException("Unable to create directory : " + dir);
      }
    }

    File outputFile = new File(new File(srcFilePath.toString()), fileName);
    FileOutputStream os = new FileOutputStream(outputFile);
    final OutputStreamWriter osw = new OutputStreamWriter(os, "UTF8");
    final int ch = filler;

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    try (LogWriter logWriter = new LogWriter()) {
      logWriter.initialize(new Configuration(), remoteAppLogFile, ugi);

      LogKey logKey = new LogKey(testContainerId);
      LogValue logValue =
          spy(new LogValue(Collections.singletonList(srcFileRoot.toString()),
              testContainerId, ugi.getShortUserName()));

      final CountDownLatch latch = new CountDownLatch(1);

      Thread t = new Thread() {
        public void run() {
          try {
            for (int i = 0; i < length / 3; i++) {
              osw.write(ch);
            }

            latch.countDown();

            for (int i = 0; i < (2 * length) / 3; i++) {
              osw.write(ch);
            }
            osw.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      };
      t.start();

      //Wait till the osw is partially written
      //aggregation starts once the ows has completed 1/3rd of its work
      latch.await();

      //Aggregate The Logs
      logWriter.append(logKey, logValue);
    }
  }

  @Test
  public void testReadAcontainerLogs1() throws Exception {
    //Verify the output generated by readAContainerLogs(DataInputStream, Writer, logUploadedTime)
    testReadAcontainerLog(true);

    //Verify the output generated by readAContainerLogs(DataInputStream, Writer)
    testReadAcontainerLog(false);
  }

  private void testReadAcontainerLog(boolean logUploadedTime) throws Exception {
    Configuration conf = new Configuration();
    File workDir = new File(testWorkDir, "testReadAcontainerLogs1");
    Path remoteAppLogFile =
        new Path(workDir.getAbsolutePath(), "aggregatedLogFile");
    Path srcFileRoot = new Path(workDir.getAbsolutePath(), "srcFiles");
    ContainerId testContainerId = TestContainerId.newContainerId(1, 1, 1, 1);
    Path t =
        new Path(srcFileRoot, testContainerId.getApplicationAttemptId()
            .getApplicationId().toString());
    Path srcFilePath = new Path(t, testContainerId.toString());

    int numChars = 80000;

    // create a sub-folder under srcFilePath
    // and create file logs in this sub-folder.
    // We only aggregate top level files.
    // So, this log file should be ignored.
    Path subDir = new Path(srcFilePath, "subDir");
    fs.mkdirs(subDir);
    writeSrcFile(subDir, "logs", numChars);

    // create file stderr and stdout in containerLogDir
    writeSrcFile(srcFilePath, "stderr", numChars);
    writeSrcFile(srcFilePath, "stdout", numChars);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    try (LogWriter logWriter = new LogWriter()) {
      logWriter.initialize(conf, remoteAppLogFile, ugi);

      LogKey logKey = new LogKey(testContainerId);
      LogValue logValue =
          new LogValue(Collections.singletonList(srcFileRoot.toString()),
              testContainerId, ugi.getShortUserName());

      // When we try to open FileInputStream for stderr, it will throw out an
      // IOException. Skip the log aggregation for stderr.
      LogValue spyLogValue = spy(logValue);
      File errorFile = new File((new Path(srcFilePath, "stderr")).toString());
      doThrow(new IOException("Mock can not open FileInputStream")).when(
          spyLogValue).secureOpenFile(errorFile);

      logWriter.append(logKey, spyLogValue);
    }

    // make sure permission are correct on the file
    FileStatus fsStatus =  fs.getFileStatus(remoteAppLogFile);
    Assert.assertEquals("permissions on log aggregation file are wrong",  
      FsPermission.createImmutable((short) 0640), fsStatus.getPermission()); 

    LogReader logReader = new LogReader(conf, remoteAppLogFile);
    LogKey rLogKey = new LogKey();
    DataInputStream dis = logReader.next(rLogKey);
    Writer writer = new StringWriter();

    if (logUploadedTime) {
      LogReader.readAcontainerLogs(dis, writer, System.currentTimeMillis());
    } else {
      LogReader.readAcontainerLogs(dis, writer);
    }

    // We should only do the log aggregation for stdout.
    // Since we could not open the fileInputStream for stderr, this file is not
    // aggregated.
    String s = writer.toString();

    int expectedLength = "LogType:stdout".length()
        + (logUploadedTime
            ? (System.lineSeparator() + "Log Upload Time:"
                + Times.format(System.currentTimeMillis())).length()
            : 0)
        + (System.lineSeparator() + "LogLength:" + numChars).length()
        + (System.lineSeparator() + "Log Contents:" + System.lineSeparator())
            .length()
        + numChars + ("\n").length() + ("End of LogType:stdout"
            + System.lineSeparator() + System.lineSeparator()).length();

    Assert.assertTrue("LogType not matched", s.contains("LogType:stdout"));
    Assert.assertTrue("log file:stderr should not be aggregated.", !s.contains("LogType:stderr"));
    Assert.assertTrue("log file:logs should not be aggregated.", !s.contains("LogType:logs"));
    Assert.assertTrue("LogLength not matched", s.contains("LogLength:" + numChars));
    Assert.assertTrue("Log Contents not matched", s.contains("Log Contents"));
    
    StringBuilder sb = new StringBuilder();
    for (int i = 0 ; i < numChars ; i++) {
      sb.append(filler);
    }
    String expectedContent = sb.toString();
    Assert.assertTrue("Log content incorrect", s.contains(expectedContent));
    
    Assert.assertEquals(expectedLength, s.length());
  }

  @Test(timeout=10000)
  public void testContainerLogsFileAccess() throws IOException {
    // This test will run only if NativeIO is enabled as SecureIOUtils 
    // require it to be enabled.
    Assume.assumeTrue(NativeIO.isAvailable());
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    File workDir = new File(testWorkDir, "testContainerLogsFileAccess1");
    Path remoteAppLogFile =
        new Path(workDir.getAbsolutePath(), "aggregatedLogFile");
    Path srcFileRoot = new Path(workDir.getAbsolutePath(), "srcFiles");

    String data = "Log File content for container : ";
    // Creating files for container1. Log aggregator will try to read log files
    // with illegal user.
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId testContainerId1 =
        ContainerId.newContainerId(applicationAttemptId, 1);
    Path appDir =
        new Path(srcFileRoot, testContainerId1.getApplicationAttemptId()
            .getApplicationId().toString());
    Path srcFilePath1 = new Path(appDir, testContainerId1.toString());
    String stdout = "stdout";
    String stderr = "stderr";
    writeSrcFile(srcFilePath1, stdout, data + testContainerId1.toString()
        + stdout);
    writeSrcFile(srcFilePath1, stderr, data + testContainerId1.toString()
        + stderr);

    UserGroupInformation ugi =
        UserGroupInformation.getCurrentUser();
    try (LogWriter logWriter = new LogWriter()) {
      logWriter.initialize(conf, remoteAppLogFile, ugi);

      LogKey logKey = new LogKey(testContainerId1);
      String randomUser = "randomUser";
      LogValue logValue =
          spy(new LogValue(Collections.singletonList(srcFileRoot.toString()),
              testContainerId1, randomUser));

      // It is trying simulate a situation where first log file is owned by
      // different user (probably symlink) and second one by the user itself.
      // The first file should not be aggregated. Because this log file has
      // the invalid user name.
      when(logValue.getUser()).thenReturn(randomUser).thenReturn(
          ugi.getShortUserName());
      logWriter.append(logKey, logValue);
    }

    BufferedReader in =
        new BufferedReader(new FileReader(new File(remoteAppLogFile
            .toUri().getRawPath())));
    String line;
    StringBuffer sb = new StringBuffer("");
    while ((line = in.readLine()) != null) {
      LOG.info(line);
      sb.append(line);
    }
    line = sb.toString();

    String expectedOwner = ugi.getShortUserName();
    if (Path.WINDOWS) {
      final String adminsGroupString = "Administrators";
      if (Arrays.asList(ugi.getGroupNames()).contains(adminsGroupString)) {
        expectedOwner = adminsGroupString;
      }
    }

    // This file: stderr should not be aggregated.
    // And we will not aggregate the log message.
    String stdoutFile1 =
        StringUtils.join(
            File.separator,
            Arrays.asList(new String[] {
                workDir.getAbsolutePath(), "srcFiles",
                testContainerId1.getApplicationAttemptId().getApplicationId()
                    .toString(), testContainerId1.toString(), stderr }));

    // The file: stdout is expected to be aggregated.
    String stdoutFile2 =
        StringUtils.join(
            File.separator,
            Arrays.asList(new String[] {
                workDir.getAbsolutePath(), "srcFiles",
                testContainerId1.getApplicationAttemptId().getApplicationId()
                    .toString(), testContainerId1.toString(), stdout }));
    String message2 =
        "Owner '" + expectedOwner + "' for path "
            + stdoutFile2 + " did not match expected owner '"
            + ugi.getShortUserName() + "'";
    
    Assert.assertFalse(line.contains(message2));
    Assert.assertFalse(line.contains(data + testContainerId1.toString()
        + stderr));
    Assert.assertTrue(line.contains(data + testContainerId1.toString()
        + stdout));
  }
  
  private void writeSrcFile(Path srcFilePath, String fileName, long length)
      throws IOException {
    OutputStreamWriter osw = getOutputStreamWriter(srcFilePath, fileName);
    int ch = filler;
    for (int i = 0; i < length; i++) {
      osw.write(ch);
    }
    osw.close();
  }
  
  private void writeSrcFile(Path srcFilePath, String fileName, String data)
      throws IOException {
    OutputStreamWriter osw = getOutputStreamWriter(srcFilePath, fileName);
    osw.write(data);
    osw.close();
  }

  private OutputStreamWriter getOutputStreamWriter(Path srcFilePath,
      String fileName) throws IOException, FileNotFoundException,
      UnsupportedEncodingException {
    File dir = new File(srcFilePath.toString());
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IOException("Unable to create directory : " + dir);
      }
    }
    File outputFile = new File(new File(srcFilePath.toString()), fileName);
    FileOutputStream os = new FileOutputStream(outputFile);
    OutputStreamWriter osw = new OutputStreamWriter(os, "UTF8");
    return osw;
  }
}
