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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestQuotaUsage {

  // check the empty constructor correctly initialises the object
  @Test
  public void testConstructorEmpty() {
    QuotaUsage quotaUsage = new QuotaUsage.Builder().build();
    assertEquals("getQuota", -1, quotaUsage.getQuota());
    assertEquals("getSpaceConsumed", 0, quotaUsage.getSpaceConsumed());
    assertEquals("getSpaceQuota", -1, quotaUsage.getSpaceQuota());
  }

  // check the full constructor with quota information
  @Test
  public void testConstructorWithQuota() {
    long fileAndDirCount = 22222;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66666;

    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    assertEquals("getFileAndDirectoryCount", fileAndDirCount,
        quotaUsage.getFileAndDirectoryCount());
    assertEquals("getQuota", quota, quotaUsage.getQuota());
    assertEquals("getSpaceConsumed", spaceConsumed,
        quotaUsage.getSpaceConsumed());
    assertEquals("getSpaceQuota", spaceQuota, quotaUsage.getSpaceQuota());
  }

  // check the constructor with quota information
  @Test
  public void testConstructorNoQuota() {
    long spaceConsumed = 11111;
    long fileAndDirCount = 22222;
    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).
        spaceConsumed(spaceConsumed).build();
    assertEquals("getFileAndDirectoryCount", fileAndDirCount,
        quotaUsage.getFileAndDirectoryCount());
    assertEquals("getQuota", -1, quotaUsage.getQuota());
    assertEquals("getSpaceConsumed", spaceConsumed,
        quotaUsage.getSpaceConsumed());
    assertEquals("getSpaceQuota", -1, quotaUsage.getSpaceQuota());
  }

  // check the header
  @Test
  public void testGetHeader() {
    String header = "       QUOTA       REM_QUOTA     SPACE_QUOTA "
        + "REM_SPACE_QUOTA ";
    assertEquals(header, QuotaUsage.getHeader());
  }

  // check the toString method with quotas
  @Test
  public void testToStringWithQuota() {
    long fileAndDirCount = 55555;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66665;

    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected ="       44444          -11111           66665" +
        "           11110 ";
    assertEquals(expected, quotaUsage.toString());
  }

  // check the toString method with quotas
  @Test
  public void testToStringNoQuota() {
    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(1234).build();
    String expected = "        none             inf            none"
        + "             inf ";
    assertEquals(expected, quotaUsage.toString());
  }

  // check the toString method with quotas
  @Test
  public void testToStringHumanWithQuota() {
    long fileAndDirCount = 222255555;
    long quota = 222256578;
    long spaceConsumed = 1073741825;
    long spaceQuota = 1;

    QuotaUsage quotaUsage = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = "     212.0 M            1023               1 "
        + "           -1 G ";
    assertEquals(expected, quotaUsage.toString(true));
  }

  // check the equality
  @Test
  public void testCompareQuotaUsage() {
    long fileAndDirCount = 222255555;
    long quota = 222256578;
    long spaceConsumed = 1073741825;
    long spaceQuota = 1;
    long SSDspaceConsumed = 100000;
    long SSDQuota = 300000;

    QuotaUsage quotaUsage1 = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).
        typeConsumed(StorageType.SSD, SSDQuota).
        typeQuota(StorageType.SSD, SSDQuota).
        build();

    QuotaUsage quotaUsage2 = new QuotaUsage.Builder().
        fileAndDirectoryCount(fileAndDirCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).
        typeConsumed(StorageType.SSD, SSDQuota).
        typeQuota(StorageType.SSD, SSDQuota).
        build();

    assertEquals(quotaUsage1, quotaUsage2);
  }
}
