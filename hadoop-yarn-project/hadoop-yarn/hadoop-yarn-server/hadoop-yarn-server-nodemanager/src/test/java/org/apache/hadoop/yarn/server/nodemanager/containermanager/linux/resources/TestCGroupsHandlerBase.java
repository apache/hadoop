/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.Permission;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the CGroups handler implementation.
 */
public abstract class TestCGroupsHandlerBase {
  protected PrivilegedOperationExecutor privilegedOperationExecutorMock;
  protected String tmpPath;
  protected String hierarchy;
  protected CGroupsHandler.CGroupController controller;
  protected String controllerPath;

  @BeforeEach
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);

    // Prepare test directory
    tmpPath = System.getProperty("test.build.data") + "/cgroup";
    File tmpDir = new File(tmpPath);
    FileUtils.deleteQuietly(tmpDir);
    assertTrue(tmpDir.mkdirs());

    //no leading or trailing slashes here
    hierarchy = "test-hadoop-yarn";

    // Sample subsystem. Not used by all the tests
    controller = CGroupsHandler.CGroupController.CPU;
    controllerPath = getControllerFilePath(controller.getName());
  }

  @AfterEach
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }

  protected abstract String getControllerFilePath(String controllerName);

  /**
   * Security manager simulating access denied.
   */
  protected static class MockSecurityManagerDenyWrite extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      if(perm.getActions().equals("write")) {
        throw new SecurityException("Mock not allowed");
      }
    }
  }

  /**
   * Create configuration to mount cgroups that do not exist.
   * @return configuration object
   */
  protected YarnConfiguration createMountConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, hierarchy);
    conf.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT, true);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    return conf;
  }

  /**
   * Create configuration where the cgroups are premounted.
   * @param myHierarchy YARN cgroup
   * @return configuration object
   */
  protected Configuration createNoMountConfiguration(String myHierarchy) {
    Configuration confNoMount = new Configuration();
    confNoMount.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        myHierarchy);
    confNoMount.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT,
        false);
    return confNoMount;
  }

  /**
   * Create an empty mtab file. No cgroups are premounted
   * @return mtab file
   * @throws IOException could not create file
   */
  protected File createEmptyMtabFile() throws IOException {
    File emptyMtab = new File(tmpPath, "mtab");
    assertTrue(emptyMtab.createNewFile(), "New file should have been created");
    return emptyMtab;
  }

  /**
   * Create a new file with supplied content.
   * @param parentDir parent directory
   * @param fileName name of the file
   * @param content content to write in the file
   * @return file created
   * @throws IOException if file could not be created
   */
  public File createFileWithContent(File parentDir, String fileName, String content)
      throws IOException {
    File fileToCreate = new File(parentDir, fileName);
    if (!fileToCreate.exists()) {
      if (!fileToCreate.createNewFile()) {
        String message = "Could not create file " + fileToCreate.getAbsolutePath();
        throw new IOException(message);
      }
    }
    FileWriter fWriter = new FileWriter(fileToCreate.getAbsoluteFile());
    fWriter.write(content);
    fWriter.close();
    fileToCreate.deleteOnExit();

    return fileToCreate;
  }
}
