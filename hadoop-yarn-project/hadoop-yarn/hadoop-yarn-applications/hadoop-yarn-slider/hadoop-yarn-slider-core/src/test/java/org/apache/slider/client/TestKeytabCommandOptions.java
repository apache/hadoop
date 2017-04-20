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

package org.apache.slider.client;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.params.ClientArgs;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.utils.SliderTestBase;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Test a keytab installation.
 */
public class TestKeytabCommandOptions extends SliderTestBase {

  private static SliderFileSystem testFileSystem;

  @Before
  public void setupFilesystem() throws IOException {
    org.apache.hadoop.fs.FileSystem fileSystem = new RawLocalFileSystem();
    YarnConfiguration configuration = SliderUtils.createConfiguration();
    fileSystem.setConf(configuration);
    testFileSystem = new SliderFileSystem(fileSystem, configuration);
    File testFolderDir = new File(testFileSystem
        .buildKeytabInstallationDirPath("").toUri().getPath());
    FileUtils.deleteDirectory(testFolderDir);
  }

  @Test
  public void testInstallKeytab() throws Throwable {
    // create a mock keytab file
    File localKeytab =
        FileUtil.createLocalTempFile(getTempLocation(), "test", true);
    String contents = UUID.randomUUID().toString();
    FileUtils.write(localKeytab, contents);
    YarnConfiguration conf = SliderUtils.createConfiguration();
    ServiceLauncher launcher = launch(TestSliderClient.class,
                                      conf,
                                      Arrays.asList(
                                          ClientArgs.ACTION_KEYTAB,
                                          ClientArgs.ARG_KEYTABINSTALL,
                                          ClientArgs.ARG_KEYTAB,
                                          localKeytab.getAbsolutePath(),
                                          Arguments.ARG_FOLDER,
                                          "testFolder"));
    Path installedPath = new Path(testFileSystem
        .buildKeytabInstallationDirPath("testFolder"), localKeytab.getName());
    File installedKeytab = new File(installedPath.toUri().getPath());
    assertTrue(installedKeytab.exists());
    assertEquals(FileUtils.readFileToString(installedKeytab),
        FileUtils.readFileToString(localKeytab));
  }

  @Test
  public void testInstallThenDeleteKeytab() throws Throwable {
    // create a mock keytab file
    File localKeytab =
        FileUtil.createLocalTempFile(getTempLocation(), "test", true);
    String contents = UUID.randomUUID().toString();
    FileUtils.write(localKeytab, contents);
    YarnConfiguration conf = SliderUtils.createConfiguration();
    ServiceLauncher launcher = launch(TestSliderClient.class,
                                      conf,
                                      Arrays.asList(
                                          ClientArgs.ACTION_KEYTAB,
                                          ClientArgs.ARG_KEYTABINSTALL,
                                          ClientArgs.ARG_KEYTAB,
                                          localKeytab.getAbsolutePath(),
                                          Arguments.ARG_FOLDER,
                                          "testFolder"));
    Path installedPath = new Path(testFileSystem
        .buildKeytabInstallationDirPath("testFolder"), localKeytab.getName());
    File installedKeytab = new File(installedPath.toUri().getPath());
    assertTrue(installedKeytab.exists());
    assertEquals(FileUtils.readFileToString(installedKeytab),
        FileUtils.readFileToString(localKeytab));

    launcher = launch(TestSliderClient.class,
                      conf,
                      Arrays.asList(
                          ClientArgs.ACTION_KEYTAB,
                          ClientArgs.ARG_KEYTABDELETE,
                          ClientArgs.ARG_KEYTAB,
                          localKeytab.getName(),
                          Arguments.ARG_FOLDER,
                          "testFolder"));

    assertFalse(installedKeytab.exists());

  }

  @Test
  public void testInstallThenListKeytab() throws Throwable {
    // create a mock keytab file
    File localKeytab =
        FileUtil.createLocalTempFile(getTempLocation(), "test", true);
    String contents = UUID.randomUUID().toString();
    FileUtils.write(localKeytab, contents);
    YarnConfiguration conf = SliderUtils.createConfiguration();
    ServiceLauncher launcher = launch(TestSliderClient.class,
                                      conf,
                                      Arrays.asList(
                                          ClientArgs.ACTION_KEYTAB,
                                          ClientArgs.ARG_KEYTABINSTALL,
                                          ClientArgs.ARG_KEYTAB,
                                          localKeytab.getAbsolutePath(),
                                          Arguments.ARG_FOLDER,
                                          "testFolder"));
    Path installedPath = new Path(testFileSystem
        .buildKeytabInstallationDirPath("testFolder"), localKeytab.getName());
    File installedKeytab = new File(installedPath.toUri().getPath());
    assertTrue(installedKeytab.exists());
    assertEquals(FileUtils.readFileToString(installedKeytab),
        FileUtils.readFileToString(localKeytab));

    // install an additional copy into another folder to test listing
    launcher = launch(TestSliderClient.class,
                      conf,
                      Arrays.asList(
                          ClientArgs.ACTION_KEYTAB,
                          ClientArgs.ARG_KEYTABINSTALL,
                          ClientArgs.ARG_KEYTAB,
                          localKeytab.getAbsolutePath(),
                          Arguments.ARG_FOLDER,
                          "testFolder2"));

    TestAppender testAppender = new TestAppender();

    Logger.getLogger(SliderClient.class).addAppender(testAppender);

    try {
      launcher = launch(TestSliderClient.class,
                        conf,
                        Arrays.asList(
                            ClientArgs.ACTION_KEYTAB,
                            ClientArgs.ARG_KEYTABLIST)
      );
      assertEquals(3, testAppender.events.size());
      String msg = (String) testAppender.events.get(1).getMessage();
      assertTrue(msg.contains("/.slider/keytabs/testFolder"));
      assertTrue(msg.endsWith(installedKeytab.getName()));
      msg = (String) testAppender.events.get(2).getMessage();
      assertTrue(msg.contains("/.slider/keytabs/testFolder"));
      assertTrue(msg.endsWith(installedKeytab.getName()));
    } finally {
      Logger.getLogger(SliderClient.class).removeAppender(testAppender);
    }

    // now listing while specifying the folder name
    testAppender = new TestAppender();

    Logger.getLogger(SliderClient.class).addAppender(testAppender);

    try {
      launcher = launch(TestSliderClient.class,
                        conf,
                        Arrays.asList(
                            ClientArgs.ACTION_KEYTAB,
                            ClientArgs.ARG_KEYTABLIST,
                            Arguments.ARG_FOLDER,
                            "testFolder"));
      assertEquals(2, testAppender.events.size());
      String msg = (String) testAppender.events.get(1).getMessage();
      assertTrue(msg.contains("/.slider/keytabs/testFolder/" +
          installedKeytab.getName()));
    } finally {
      Logger.getLogger(SliderClient.class).removeAppender(testAppender);
    }
  }

  @Test
  public void testDeleteNonExistentKeytab() throws Throwable {
    // create a mock keytab file
    YarnConfiguration conf = SliderUtils.createConfiguration();
    try {
      ServiceLauncher launcher = launch(TestSliderClient.class,
                                        conf,
                                        Arrays.asList(
                                            ClientArgs.ACTION_KEYTAB,
                                            ClientArgs.ARG_KEYTABDELETE,
                                            ClientArgs.ARG_KEYTAB,
                                            "HeyIDontExist.keytab",
                                            Arguments.ARG_FOLDER,
                                            "testFolder"));
      fail("expected BadCommandArgumentsException from launch");
    } catch (BadCommandArgumentsException e) {
      // expected
    }
  }

  @Test
  public void testInstallKeytabWithNoFolder() throws Throwable {
    // create a mock keytab file
    File localKeytab =
        FileUtil.createLocalTempFile(getTempLocation(), "test", true);
    String contents = UUID.randomUUID().toString();
    FileUtils.write(localKeytab, contents);
    YarnConfiguration conf = SliderUtils.createConfiguration();
    try {
      ServiceLauncher launcher = launch(TestSliderClient.class,
                                        conf,
                                        Arrays.asList(
                                            ClientArgs.ACTION_KEYTAB,
                                            ClientArgs.ARG_KEYTABINSTALL,
                                            ClientArgs.ARG_KEYTAB,
                                            localKeytab.getAbsolutePath()));
      fail("expected BadCommandArgumentsException from launch");
    } catch (BadCommandArgumentsException e) {
      // expected
    }
  }

  @Test
  public void testInstallKeytabWithNoKeytab() throws Throwable {
    // create a mock keytab file
    File localKeytab =
        FileUtil.createLocalTempFile(getTempLocation(), "test", true);
    String contents = UUID.randomUUID().toString();
    FileUtils.write(localKeytab, contents);
    YarnConfiguration conf = SliderUtils.createConfiguration();
    try {
      ServiceLauncher launcher = launch(TestSliderClient.class,
                                        conf,
                                        Arrays.asList(
                                            ClientArgs.ACTION_KEYTAB,
                                            ClientArgs.ARG_KEYTABINSTALL,
                                            ClientArgs.ARG_FOLDER,
                                            "testFolder"));
      fail("expected BadCommandArgumentsException from launch");
    } catch (BadCommandArgumentsException e) {
      // expected
    }
  }

  @Test
  public void testInstallKeytabAllowingOverwrite() throws Throwable {
    // create a mock keytab file
    File localKeytab =
        FileUtil.createLocalTempFile(getTempLocation(), "test", true);
    String contents = UUID.randomUUID().toString();
    FileUtils.write(localKeytab, contents);
    YarnConfiguration conf = SliderUtils.createConfiguration();
    ServiceLauncher launcher = launch(TestSliderClient.class,
                                      conf,
                                      Arrays.asList(
                                          ClientArgs.ACTION_KEYTAB,
                                          ClientArgs.ARG_KEYTABINSTALL,
                                          ClientArgs.ARG_KEYTAB,
                                          localKeytab.getAbsolutePath(),
                                          Arguments.ARG_FOLDER,
                                          "testFolder"));
    Path installedPath = new Path(testFileSystem
        .buildKeytabInstallationDirPath("testFolder"), localKeytab.getName());
    File installedKeytab = new File(installedPath.toUri().getPath());
    assertTrue(installedKeytab.exists());
    assertEquals(FileUtils.readFileToString(installedKeytab), FileUtils
        .readFileToString(localKeytab));
    launcher = launch(TestSliderClient.class,
                      conf,
                      Arrays.asList(
                          ClientArgs.ACTION_KEYTAB,
                          ClientArgs.ARG_KEYTABINSTALL,
                          ClientArgs.ARG_KEYTAB,
                          localKeytab.getAbsolutePath(),
                          Arguments.ARG_FOLDER,
                          "testFolder",
                          Arguments.ARG_OVERWRITE)
    );
    assertTrue(installedKeytab.exists());
    assertEquals(FileUtils.readFileToString(installedKeytab),
        FileUtils.readFileToString(localKeytab));
  }

  @Test
  public void testInstallKeytabNotAllowingOverwrite() throws Throwable {
    // create a mock keytab file
    File localKeytab =
        FileUtil.createLocalTempFile(getTempLocation(), "test", true);
    String contents = UUID.randomUUID().toString();
    FileUtils.write(localKeytab, contents);
    YarnConfiguration conf = SliderUtils.createConfiguration();
    ServiceLauncher launcher = launch(TestSliderClient.class,
                                      conf,
                                      Arrays.asList(
                                          ClientArgs.ACTION_KEYTAB,
                                          ClientArgs.ARG_KEYTABINSTALL,
                                          ClientArgs.ARG_KEYTAB,
                                          localKeytab.getAbsolutePath(),
                                          Arguments.ARG_FOLDER,
                                          "testFolder"));
    Path installedPath = new Path(testFileSystem
        .buildKeytabInstallationDirPath("testFolder"), localKeytab.getName());
    File installedKeytab = new File(installedPath.toUri().getPath());
    assertTrue(installedKeytab.exists());
    assertEquals(FileUtils.readFileToString(installedKeytab),
        FileUtils.readFileToString(localKeytab));
    try {
      launcher = launch(TestSliderClient.class,
                        conf,
                        Arrays.asList(
                            ClientArgs.ACTION_KEYTAB,
                            ClientArgs.ARG_KEYTABINSTALL,
                            ClientArgs.ARG_KEYTAB,
                            localKeytab.getAbsolutePath(),
                            Arguments.ARG_FOLDER,
                            "testFolder"));
      fail("expected BadCommandArgumentsException from launch");
    } catch (BadCommandArgumentsException e) {
      // expected
    }
  }

  @Test
  public void testInstallKeytabWithMissingKeytab() throws Throwable {
    // create a mock keytab file
    YarnConfiguration conf = SliderUtils.createConfiguration();
    try {
      ServiceLauncher launcher = launch(TestSliderClient.class,
                                        conf,
                                        Arrays.asList(
                                            ClientArgs.ACTION_KEYTAB,
                                            ClientArgs.ARG_KEYTABINSTALL,
                                            ClientArgs.ARG_KEYTAB,
                                            "HeyIDontExist.keytab",
                                            Arguments.ARG_FOLDER,
                                            "testFolder"));
      fail("expected BadCommandArgumentsException from launch");
    } catch (BadCommandArgumentsException e) {
      // expected
    }
  }

  private File getTempLocation() {
    return new File(System.getProperty("user.dir") + "/target");
  }

  /**
   * Test SliderClient with overridden filesystem.
   */
  public static class TestSliderClient extends SliderClient {
    public TestSliderClient() {
      super();
    }

    @Override
    protected void initHadoopBinding() throws IOException, SliderException {
      sliderFileSystem = testFileSystem;
    }

  }

  /**
   * Appender that captures logging events.
   */
  public static class TestAppender extends AppenderSkeleton {
    private List<LoggingEvent> events = new ArrayList<>();

    public void close() {}

    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }
  }
}
