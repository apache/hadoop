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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLocalDirsHandlerService {
  private static final File testDir = new File("target",
      TestDirectoryCollection.class.getName()).getAbsoluteFile();
  private static final File testFile = new File(testDir, "testfile");

  @BeforeClass
  public static void setup() throws IOException {
    testDir.mkdirs();
    testFile.createNewFile();
  }

  @AfterClass
  public static void teardown() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testDirStructure() throws Exception {
    Configuration conf = new YarnConfiguration();
    String localDir1 = new File("file:///" + testDir, "localDir1").getPath();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir1);
    String logDir1 = new File("file:///" + testDir, "logDir1").getPath();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1);
    LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
    dirSvc.init(conf);
    Assert.assertEquals(1, dirSvc.getLocalDirs().size());
  }

  @Test
  public void testValidPathsDirHandlerService() {
    Configuration conf = new YarnConfiguration();
    String localDir1 = new File("file:///" + testDir, "localDir1").getPath();
    String localDir2 = new File("hdfs:///" + testDir, "localDir2").getPath();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir1 + "," + localDir2);
    String logDir1 = new File("file:///" + testDir, "logDir1").getPath();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1);
    LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
    try {
      dirSvc.init(conf);
      Assert.fail("Service should have thrown an exception due to wrong URI");
    } catch (YarnRuntimeException e) {
    }
    Assert.assertEquals("Service should not be inited",
                        STATE.STOPPED,
                        dirSvc.getServiceState());
  }
}
