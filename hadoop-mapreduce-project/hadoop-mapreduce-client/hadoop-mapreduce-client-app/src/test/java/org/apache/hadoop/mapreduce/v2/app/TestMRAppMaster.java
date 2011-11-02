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
package org.apache.hadoop.mapreduce.v2.app;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;

public class TestMRAppMaster {
  @Test
  public void testMRAppMasterForDifferentUser() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000001";
    String containerIdStr = "container_1317529182569_0004_000001_1";
    String stagingDir = "/tmp/staging";
    String userName = "TestAppMasterUser";
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    MRAppMasterTest appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis());
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    Assert.assertEquals(stagingDir + Path.SEPARATOR + userName + Path.SEPARATOR
        + ".staging", appMaster.stagingDirPath.toString());
  }
}

class MRAppMasterTest extends MRAppMaster {

  Path stagingDirPath;
  private Configuration conf;

  public MRAppMasterTest(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String host, int port, int httpPort,
      long submitTime) {
    super(applicationAttemptId, containerId, host, port, httpPort, submitTime);
  }

  @Override
  public void init(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void start() {
    try {
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      stagingDirPath = MRApps.getStagingAreaDir(conf, user);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

}
