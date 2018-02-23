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
package org.apache.hadoop.util;

import java.io.IOException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCacheableIPList {

  /**
   * Add a bunch of subnets and IPSs to the file
   * setup a low cache refresh
   * test for inclusion
   * Check for exclusion
   * Add a bunch of subnets and Ips
   * wait for cache timeout.
   * test for inclusion
   * Check for exclusion
   */
  @Test
  public void testAddWithSleepForCacheTimeout() throws IOException, InterruptedException {

    String[] ips = {"10.119.103.112", "10.221.102.0/23", "10.113.221.221"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips);

    CacheableIPList cipl = new CacheableIPList(
        new FileBasedIPList("ips.txt"),100);

    assertFalse("10.113.221.222 is in the list",
        cipl.isIn("10.113.221.222"));
    assertFalse ("10.222.103.121 is  in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
    String[]ips2 = {"10.119.103.112", "10.221.102.0/23",
        "10.222.0.0/16", "10.113.221.221", "10.113.221.222"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips2);
    Thread.sleep(101);

    assertTrue("10.113.221.222 is not in the list",
        cipl.isIn("10.113.221.222"));
    assertTrue ("10.222.103.121 is not in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
  }

  /**
   * Add a bunch of subnets and IPSs to the file
   * setup a low cache refresh
   * test for inclusion
   * Check for exclusion
   * Remove a bunch of subnets and Ips
   * wait for cache timeout.
   * test for inclusion
   * Check for exclusion
   */
  @Test
  public void testRemovalWithSleepForCacheTimeout() throws IOException, InterruptedException {

    String[] ips = {"10.119.103.112", "10.221.102.0/23",
        "10.222.0.0/16", "10.113.221.221", "10.113.221.222"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips);

    CacheableIPList cipl = new CacheableIPList(
        new FileBasedIPList("ips.txt"),100);

    assertTrue("10.113.221.222 is not in the list",
        cipl.isIn("10.113.221.222"));
    assertTrue ("10.222.103.121 is not in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
    String[]ips2 = {"10.119.103.112", "10.221.102.0/23", "10.113.221.221"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips2);
    Thread.sleep(1005);

    assertFalse("10.113.221.222 is in the list",
        cipl.isIn("10.113.221.222"));
    assertFalse ("10.222.103.121 is  in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
  }

  /**
   * Add a bunch of subnets and IPSs to the file
   * setup a low cache refresh
   * test for inclusion
   * Check for exclusion
   * Add a bunch of subnets and Ips
   * do a refresh
   * test for inclusion
   * Check for exclusion
   */
  @Test
  public void testAddWithRefresh() throws IOException, InterruptedException {

    String[] ips = {"10.119.103.112", "10.221.102.0/23", "10.113.221.221"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips);

    CacheableIPList cipl = new CacheableIPList(
        new FileBasedIPList("ips.txt"),100);

    assertFalse("10.113.221.222 is in the list",
        cipl.isIn("10.113.221.222"));
    assertFalse ("10.222.103.121 is  in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
    String[]ips2 = {"10.119.103.112", "10.221.102.0/23",
        "10.222.0.0/16", "10.113.221.221", "10.113.221.222"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips2);
    cipl.refresh();

    assertTrue("10.113.221.222 is not in the list",
        cipl.isIn("10.113.221.222"));
    assertTrue ("10.222.103.121 is not in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
  }

  /**
   * Add a bunch of subnets and IPSs to the file
   * setup a low cache refresh
   * test for inclusion
   * Check for exclusion
   * Remove a bunch of subnets and Ips
   * wait for cache timeout.
   * test for inclusion
   * Check for exclusion
   */
  @Test
  public void testRemovalWithRefresh() throws IOException, InterruptedException {

    String[] ips = {"10.119.103.112", "10.221.102.0/23",
        "10.222.0.0/16", "10.113.221.221", "10.113.221.222"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips);

    CacheableIPList cipl = new CacheableIPList(
        new FileBasedIPList("ips.txt"),100);

    assertTrue("10.113.221.222 is not in the list",
        cipl.isIn("10.113.221.222"));
    assertTrue ("10.222.103.121 is not in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
    String[]ips2 = {"10.119.103.112", "10.221.102.0/23", "10.113.221.221"};

    TestFileBasedIPList.createFileWithEntries ("ips.txt", ips2);
    cipl.refresh();

    assertFalse("10.113.221.222 is in the list",
        cipl.isIn("10.113.221.222"));
    assertFalse ("10.222.103.121 is  in the list",
        cipl.isIn("10.222.103.121"));

    TestFileBasedIPList.removeFile("ips.txt");
  }



}
