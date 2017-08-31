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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.junit.After;
import org.junit.Assume;

public class TestContainerManagerWithLCE extends TestContainerManager {

  private static final Logger LOG =
       LoggerFactory.getLogger(TestContainerManagerWithLCE.class);

  public TestContainerManagerWithLCE() throws UnsupportedFileSystemException {
    super();
  }

  static {
    localDir =
        new File("target",
            TestContainerManagerWithLCE.class.getName() + "-localDir")
            .getAbsoluteFile();
    tmpDir = new File("target",
        TestContainerManagerWithLCE.class.getName() + "-tmpDir");
  }

  @Override
  public void setup() throws IOException {
    Assume.assumeTrue("LCE binary path is not passed. Not running the test",
        shouldRunTest());
    super.setup();
    localFS.setPermission(new Path(localDir.getCanonicalPath()),
        new FsPermission(
            (short) 0777));
    localFS.setPermission(new Path(tmpDir.getCanonicalPath()),
        new FsPermission(
            (short) 0777));
  }

  @After
  @Override
  public void tearDown() throws IOException, InterruptedException {
    if (shouldRunTest()) {
      super.tearDown();
    }
  }

  private boolean shouldRunTest() {
    return System
        .getProperty(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH) != null;
  }

  @Override
  protected ContainerExecutor createContainerExecutor() {
    super.conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, System
        .getProperty(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH));
    LinuxContainerExecutor linuxContainerExecutor =
        new LinuxContainerExecutor();
    linuxContainerExecutor.setConf(super.conf);
    return linuxContainerExecutor;
  }
}
