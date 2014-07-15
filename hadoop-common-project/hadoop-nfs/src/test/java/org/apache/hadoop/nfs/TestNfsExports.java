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
package org.apache.hadoop.nfs;

import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.junit.Assert;
import org.junit.Test;

public class TestNfsExports {

  private final String address1 = "192.168.0.1";
  private final String address2 = "10.0.0.1";
  private final String hostname1 = "a.b.com";
  private final String hostname2 = "a.b.org";
  
  private static final long ExpirationPeriod = 
      Nfs3Constant.NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_DEFAULT * 1000 * 1000;
  
  private static final int CacheSize = Nfs3Constant.NFS_EXPORTS_CACHE_SIZE_DEFAULT;
  private static final long NanosPerMillis = 1000000;

  @Test
  public void testWildcardRW() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "* rw");
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname1));
  }

  @Test
  public void testWildcardRO() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, "* ro");
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
  }

  @Test
  public void testExactAddressRW() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, address1
        + " rw");
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertFalse(AccessPrivilege.READ_WRITE == matcher
        .getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testExactAddressRO() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, address1);
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.NONE,
        matcher.getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testExactHostRW() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, hostname1
        + " rw");
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname1));
  }

  @Test
  public void testExactHostRO() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, hostname1);
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
  }

  @Test
  public void testCidrShortRW() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "192.168.0.0/22 rw");
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.NONE,
        matcher.getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testCidrShortRO() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "192.168.0.0/22");
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.NONE,
        matcher.getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testCidrLongRW() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, 
        "192.168.0.0/255.255.252.0 rw");
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.NONE,
        matcher.getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testCidrLongRO() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod, 
        "192.168.0.0/255.255.252.0");
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.NONE,
        matcher.getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testRegexIPRW() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "192.168.0.[0-9]+ rw");
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.NONE,
        matcher.getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testRegexIPRO() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "192.168.0.[0-9]+");
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.NONE,
        matcher.getAccessPrivilege(address2, hostname1));
  }

  @Test
  public void testRegexHostRW() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "[a-z]+.b.com rw");
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname1));
    // address1 will hit the cache
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address1, hostname2));
  }

  @Test
  public void testRegexHostRO() {
    NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "[a-z]+.b.com");
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
    // address1 will hit the cache
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname2));
  }
  
  @Test
  public void testMultiMatchers() throws Exception {
    long shortExpirationPeriod = 1 * 1000 * 1000 * 1000; // 1s
    NfsExports matcher = new NfsExports(CacheSize, shortExpirationPeriod, 
        "192.168.0.[0-9]+;[a-z]+.b.com rw");
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname2));
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, address1));
    Assert.assertEquals(AccessPrivilege.READ_ONLY,
        matcher.getAccessPrivilege(address1, hostname1));
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address2, hostname1));
    // address2 will hit the cache
    Assert.assertEquals(AccessPrivilege.READ_WRITE,
        matcher.getAccessPrivilege(address2, hostname2));
    
    Thread.sleep(1000);
    // no cache for address2 now
    AccessPrivilege ap;
    long startNanos = System.nanoTime();
    do {
      ap = matcher.getAccessPrivilege(address2, address2);
      if (ap == AccessPrivilege.NONE) {
        break;
      }
      Thread.sleep(500);
    } while ((System.nanoTime() - startNanos) / NanosPerMillis < 5000);
    Assert.assertEquals(AccessPrivilege.NONE, ap);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidHost() {
      NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "foo#bar");
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidSeparator() {
      NfsExports matcher = new NfsExports(CacheSize, ExpirationPeriod,
        "foo ro : bar rw");
  }
}
