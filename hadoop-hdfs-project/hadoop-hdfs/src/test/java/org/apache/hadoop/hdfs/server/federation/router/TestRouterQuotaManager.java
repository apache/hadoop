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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for class {@link RouterQuotaManager}.
 */
public class TestRouterQuotaManager {
  private static RouterQuotaManager manager;

  @Before
  public void setup() {
    manager = new RouterQuotaManager();
  }

  @After
  public void cleanup() {
    manager.clear();
  }

  @Test
  public void testGetChildrenPaths() {
    RouterQuotaUsage quotaUsage = new RouterQuotaUsage.Builder().build();
    manager.put("/path1", quotaUsage);
    manager.put("/path2", quotaUsage);
    manager.put("/path1/subdir", quotaUsage);
    manager.put("/path1/subdir/subdir", quotaUsage);

    Set<String> childrenPaths = manager.getPaths("/path1");
    assertEquals(3, childrenPaths.size());
    assertTrue(childrenPaths.contains("/path1/subdir")
        && childrenPaths.contains("/path1/subdir/subdir")
        && childrenPaths.contains("/path1"));

    // test for corner case
    manager.put("/path3", quotaUsage);
    manager.put("/path3/subdir", quotaUsage);
    manager.put("/path3-subdir", quotaUsage);

    childrenPaths = manager.getPaths("/path3");
    assertEquals(2, childrenPaths.size());
    // path /path3-subdir should not be returned
    assertTrue(childrenPaths.contains("/path3")
        && childrenPaths.contains("/path3/subdir")
        && !childrenPaths.contains("/path3-subdir"));
  }

  @Test
  public void testGetQuotaUsage() {
    RouterQuotaUsage quotaGet;

    // test case1: get quota with an non-exist path
    quotaGet = manager.getQuotaUsage("/non-exist-path");
    assertNull(quotaGet);

    // test case2: get quota from an no-quota set path
    RouterQuotaUsage.Builder quota = new RouterQuotaUsage.Builder()
        .quota(HdfsConstants.QUOTA_DONT_SET)
        .spaceQuota(HdfsConstants.QUOTA_DONT_SET);
    manager.put("/noQuotaSet", quota.build());
    quotaGet = manager.getQuotaUsage("/noQuotaSet");
    // it should return null
    assertNull(quotaGet);

    // test case3: get quota from an quota-set path
    quota.quota(1);
    quota.spaceQuota(HdfsConstants.QUOTA_DONT_SET);
    manager.put("/hasQuotaSet", quota.build());
    quotaGet = manager.getQuotaUsage("/hasQuotaSet");
    assertEquals(1, quotaGet.getQuota());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaGet.getSpaceQuota());

    // test case4: get quota with an non-exist child path
    quotaGet = manager.getQuotaUsage("/hasQuotaSet/file");
    // it will return the nearest ancestor which quota was set
    assertEquals(1, quotaGet.getQuota());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaGet.getSpaceQuota());

    // test case5: get quota with an child path which its parent
    // wasn't quota set
    quota.quota(HdfsConstants.QUOTA_DONT_SET);
    quota.spaceQuota(HdfsConstants.QUOTA_DONT_SET);
    manager.put("/hasQuotaSet/noQuotaSet", quota.build());
    // here should returns the quota of path /hasQuotaSet
    // (the nearest ancestor which quota was set)
    quotaGet = manager.getQuotaUsage("/hasQuotaSet/noQuotaSet/file");
    assertEquals(1, quotaGet.getQuota());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaGet.getSpaceQuota());

    // test case6: get quota with an child path which its parent was quota set
    quota.quota(2);
    quota.spaceQuota(HdfsConstants.QUOTA_DONT_SET);
    manager.put("/hasQuotaSet/hasQuotaSet", quota.build());
    // here should return the quota of path /hasQuotaSet/hasQuotaSet
    quotaGet = manager.getQuotaUsage("/hasQuotaSet/hasQuotaSet/file");
    assertEquals(2, quotaGet.getQuota());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quotaGet.getSpaceQuota());
  }
}
