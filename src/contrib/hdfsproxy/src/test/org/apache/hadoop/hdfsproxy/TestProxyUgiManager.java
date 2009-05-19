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

package org.apache.hadoop.hdfsproxy;

import org.apache.hadoop.security.UnixUserGroupInformation;

import junit.framework.TestCase;

/** Unit tests for ProxyUgiManager */
public class TestProxyUgiManager extends TestCase {

  private static final UnixUserGroupInformation root1Ugi = new UnixUserGroupInformation(
      "root", new String[] { "group1" });
  private static final UnixUserGroupInformation root2Ugi = new UnixUserGroupInformation(
      "root", new String[] { "group2" });
  private static final long ugiLifetime = 1000L; // milliseconds

  /** Test caching functionality */
  public void testCache() throws Exception {
    ProxyUgiManager.saveToCache(root1Ugi);
    UnixUserGroupInformation ugi = ProxyUgiManager.getUgiForUser(root1Ugi
        .getUserName());
    assertEquals(root1Ugi, ugi);
    ProxyUgiManager.saveToCache(root2Ugi);
    ugi = ProxyUgiManager.getUgiForUser(root2Ugi.getUserName());
    assertEquals(root2Ugi, ugi);
  }

  /** Test clearCache method */
  public void testClearCache() throws Exception {
    UnixUserGroupInformation ugi = ProxyUgiManager.getUgiForUser(root1Ugi
        .getUserName());
    if (root1Ugi.equals(ugi)) {
      ProxyUgiManager.saveToCache(root2Ugi);
      ugi = ProxyUgiManager.getUgiForUser(root2Ugi.getUserName());
      assertEquals(root2Ugi, ugi);
      ProxyUgiManager.clearCache();
      ugi = ProxyUgiManager.getUgiForUser(root2Ugi.getUserName());
      assertFalse(root2Ugi.equals(ugi));
    } else {
      ProxyUgiManager.saveToCache(root1Ugi);
      ugi = ProxyUgiManager.getUgiForUser(root1Ugi.getUserName());
      assertEquals(root1Ugi, ugi);
      ProxyUgiManager.clearCache();
      ugi = ProxyUgiManager.getUgiForUser(root1Ugi.getUserName());
      assertFalse(root1Ugi.equals(ugi));
    }
  }

  /** Test cache timeout */
  public void testTimeOut() throws Exception {
    String[] users = new String[] { "root", "nobody", "SYSTEM",
        "Administrator", "Administrators", "Guest" };
    String realUser = null;
    UnixUserGroupInformation ugi = null;
    ProxyUgiManager.clearCache();
    for (String user : users) {
      ugi = ProxyUgiManager.getUgiForUser(user);
      if (ugi != null) {
        realUser = user;
        break;
      }
    }
    if (realUser != null) {
      ProxyUgiManager.setUgiLifetime(ugiLifetime);
      ProxyUgiManager.clearCache();
      UnixUserGroupInformation[] fakedUgis = generateUgi(ProxyUgiManager.CLEANUP_THRESHOLD);
      for (int i = 0; i < ProxyUgiManager.CLEANUP_THRESHOLD; i++) {
        ProxyUgiManager.saveToCache(fakedUgis[i]);
      }
      assertTrue(ProxyUgiManager.getCacheSize() == ProxyUgiManager.CLEANUP_THRESHOLD);
      Thread.sleep(ugiLifetime + 1000L);
      UnixUserGroupInformation newugi = ProxyUgiManager.getUgiForUser(realUser);
      assertTrue(ProxyUgiManager.getCacheSize() == ProxyUgiManager.CLEANUP_THRESHOLD + 1);
      assertEquals(newugi, ugi);
      Thread.sleep(ugiLifetime + 1000L);
      newugi = ProxyUgiManager.getUgiForUser(realUser);
      assertTrue(ProxyUgiManager.getCacheSize() == 1);
      assertEquals(newugi, ugi);
    }
  }

  private static UnixUserGroupInformation[] generateUgi(int size) {
    UnixUserGroupInformation[] ugis = new UnixUserGroupInformation[size];
    for (int i = 0; i < size; i++) {
      ugis[i] = new UnixUserGroupInformation("user" + i,
          new String[] { "group" });
    }
    return ugis;
  }
}
