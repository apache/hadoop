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

import org.junit.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalCacheDirectoryManager.Directory;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
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
    NMContext nmContext =
        new NMContext(new NMContainerTokenSecretManager(conf),
          new NMTokenSecretManagerInNM(), null,
          new ApplicationACLsManager(conf), new NMNullStateStoreService(),
            false);
    ResourceLocalizationService service =
        new ResourceLocalizationService(null, null, null, null, nmContext);
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

  @Test
  public void testDirectoryConversion() {
    for (int i = 0; i < 10000; ++i) {
      String path = Directory.getRelativePath(i);
      Assert.assertEquals("Incorrect conversion for " + i, i,
          Directory.getDirectoryNumber(path));
    }
  }

  @Test
  public void testIncrementFileCountForPath() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY,
        LocalCacheDirectoryManager.DIRECTORIES_PER_LEVEL + 2);
    LocalCacheDirectoryManager mgr = new LocalCacheDirectoryManager(conf);
    final String rootPath = "";
    mgr.incrementFileCountForPath(rootPath);
    Assert.assertEquals(rootPath, mgr.getRelativePathForLocalization());
    Assert.assertFalse("root dir should be full",
        rootPath.equals(mgr.getRelativePathForLocalization()));
    // finish filling the other directory
    mgr.getRelativePathForLocalization();
    // free up space in the root dir
    mgr.decrementFileCountForPath(rootPath);
    mgr.decrementFileCountForPath(rootPath);
    Assert.assertEquals(rootPath, mgr.getRelativePathForLocalization());
    Assert.assertEquals(rootPath, mgr.getRelativePathForLocalization());
    String otherDir = mgr.getRelativePathForLocalization();
    Assert.assertFalse("root dir should be full", otherDir.equals(rootPath));

    final String deepDir0 = "d/e/e/p/0";
    final String deepDir1 = "d/e/e/p/1";
    final String deepDir2 = "d/e/e/p/2";
    final String deepDir3 = "d/e/e/p/3";
    mgr.incrementFileCountForPath(deepDir0);
    Assert.assertEquals(otherDir, mgr.getRelativePathForLocalization());
    Assert.assertEquals(deepDir0, mgr.getRelativePathForLocalization());
    Assert.assertEquals("total dir count incorrect after increment",
        deepDir1, mgr.getRelativePathForLocalization());
    mgr.incrementFileCountForPath(deepDir2);
    mgr.incrementFileCountForPath(deepDir1);
    mgr.incrementFileCountForPath(deepDir2);
    Assert.assertEquals(deepDir3, mgr.getRelativePathForLocalization());
  }
}
