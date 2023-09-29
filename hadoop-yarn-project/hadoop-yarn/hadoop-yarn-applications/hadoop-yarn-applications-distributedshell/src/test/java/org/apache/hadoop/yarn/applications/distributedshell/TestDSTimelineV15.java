/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.applications.distributedshell;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.PluginStoreTestUtils;

/**
 * Unit tests implementations for distributed shell on TimeLineV1.5.
 */
public class TestDSTimelineV15 extends DistributedShellBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDSTimelineV15.class);

  @Override
  protected float getTimelineVersion() {
    return 1.5f;
  }

  @Override
  protected void customizeConfiguration(
      YarnConfiguration config) throws Exception {
    setUpHDFSCluster();
    PluginStoreTestUtils.prepareFileSystemForPluginStore(
        getHDFSCluster().getFileSystem());
    PluginStoreTestUtils.prepareConfiguration(config, getHDFSCluster());
    config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES,
        DistributedShellTimelinePlugin.class.getName());
  }

  @Override
  protected void checkTimeline(ApplicationId appId,
      boolean defaultFlow, boolean haveDomain,
      ApplicationReport appReport) throws Exception {
    long scanInterval = getConfiguration().getLong(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS_DEFAULT
    );
    Path doneDir = new Path(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR_DEFAULT
    );
    // Wait till the data is moved to done dir, or timeout and fail
    AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);
    GenericTestUtils.waitFor(() -> {
      try {
        RemoteIterator<FileStatus> iterApps =
            getHDFSCluster().getFileSystem().listStatusIterator(doneDir);
        return (iterApps.hasNext());
      } catch (Exception e) {
        exceptionRef.set(e);
        LOG.error("Exception listing Done Dir", e);
        return true;
      }
    }, scanInterval * 2, TEST_TIME_WINDOW_EXPIRE);
    Assert.assertNull("Exception in getting listing status",
        exceptionRef.get());
    super.checkTimeline(appId, defaultFlow, haveDomain, appReport);
  }

  @Test
  public void testDSShellWithDomain() throws Exception {
    baseTestDSShell(true);
  }

  @Test
  public void testDSShellWithoutDomain() throws Exception {
    baseTestDSShell(false);
  }
}
