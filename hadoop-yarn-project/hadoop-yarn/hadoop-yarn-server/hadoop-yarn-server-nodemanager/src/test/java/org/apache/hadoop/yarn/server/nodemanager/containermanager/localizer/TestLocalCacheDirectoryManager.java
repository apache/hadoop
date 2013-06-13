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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Test;

public class TestLocalCacheDirectoryManager {

  @Test(timeout = 10000)
  public void testHierarchicalSubDirectoryCreation() {
    // setting per directory file limit to 1.
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY, "37");

    LocalCacheDirectoryManager hDir = new LocalCacheDirectoryManager(conf);
    // Test root directory path = ""
    Assert.assertTrue(hDir.getRelativePathForLocalization().isEmpty());

    // Testing path generation from "0" to "0/0/z/z"
    for (int i = 1; i <= 37 * 36 * 36; i++) {
      StringBuffer sb = new StringBuffer();
      String num = Integer.toString(i - 1, 36);
      if (num.length() == 1) {
        sb.append(num.charAt(0));
      } else {
        sb.append(Integer.toString(
          Integer.parseInt(num.substring(0, 1), 36) - 1, 36));
      }
      for (int j = 1; j < num.length(); j++) {
        sb.append(Path.SEPARATOR).append(num.charAt(j));
      }
      Assert.assertEquals(sb.toString(), hDir.getRelativePathForLocalization());
    }

    String testPath1 = "4";
    String testPath2 = "2";
    /*
     * Making sure directory "4" and "2" becomes non-full so that they are
     * reused for future getRelativePathForLocalization() calls in the order
     * they are freed.
     */
    hDir.decrementFileCountForPath(testPath1);
    hDir.decrementFileCountForPath(testPath2);
    // After below call directory "4" should become full.
    Assert.assertEquals(testPath1, hDir.getRelativePathForLocalization());
    Assert.assertEquals(testPath2, hDir.getRelativePathForLocalization());
  }

  @Test(timeout = 10000)
  public void testMinimumPerDirectoryFileLimit() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY, "1");
    Exception e = null;
    ResourceLocalizationService service =
        new ResourceLocalizationService(null, null, null, null);
    try {
      service.init(conf);
    } catch (Exception e1) {
      e = e1;
    }
    Assert.assertNotNull(e);
    Assert.assertEquals(YarnRuntimeException.class, e.getClass());
    Assert.assertEquals(e.getMessage(),
      YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
          + " parameter is configured with a value less than 37.");

  }

  @Test(timeout = 1000)
  public void testDirectoryStateChangeFromFullToNonFull() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY, "40");
    LocalCacheDirectoryManager dir = new LocalCacheDirectoryManager(conf);

    // checking for first four paths
    String rootPath = "";
    String firstSubDir = "0";
    for (int i = 0; i < 4; i++) {
      Assert.assertEquals(rootPath, dir.getRelativePathForLocalization());
    }
    // Releasing two files from the root directory.
    dir.decrementFileCountForPath(rootPath);
    dir.decrementFileCountForPath(rootPath);
    // Space for two files should be available in root directory.
    Assert.assertEquals(rootPath, dir.getRelativePathForLocalization());
    Assert.assertEquals(rootPath, dir.getRelativePathForLocalization());
    // As no space is now available in root directory so it should be from
    // first sub directory
    Assert.assertEquals(firstSubDir, dir.getRelativePathForLocalization());
  }
}