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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class TestContentSummary {

  // check the empty constructor correctly initialises the object
  @Test
  public void testConstructorEmpty() {
    ContentSummary contentSummary = new ContentSummary.Builder().build();
    assertEquals(0, contentSummary.getLength(), "getLength");
    assertEquals(0, contentSummary.getFileCount(), "getFileCount");
    assertEquals(0, contentSummary.getDirectoryCount(), "getDirectoryCount");
    assertEquals(-1, contentSummary.getQuota(), "getQuota");
    assertEquals(0, contentSummary.getSpaceConsumed(), "getSpaceConsumed");
    assertEquals(-1, contentSummary.getSpaceQuota(), "getSpaceQuota");
  }

  // check the full constructor with quota information
  @Test
  public void testConstructorWithQuota() {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66666;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    assertEquals(length, contentSummary.getLength(), "getLength");
    assertEquals(fileCount, contentSummary.getFileCount(), "getFileCount");
    assertEquals(directoryCount, contentSummary.getDirectoryCount(), "getDirectoryCount");
    assertEquals(quota, contentSummary.getQuota(), "getQuota");
    assertEquals(spaceConsumed,
        contentSummary.getSpaceConsumed(), "getSpaceConsumed");
    assertEquals(spaceQuota, contentSummary.getSpaceQuota(), "getSpaceQuota");
  }

  // check the constructor with quota information
  @Test
  public void testConstructorNoQuota() {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).
        spaceConsumed(length).build();
    assertEquals(length, contentSummary.getLength(), "getLength");
    assertEquals(fileCount, contentSummary.getFileCount(), "getFileCount");
    assertEquals(directoryCount,
        contentSummary.getDirectoryCount(), "getDirectoryCount");
    assertEquals(-1, contentSummary.getQuota(), "getQuota");
    assertEquals(length, contentSummary.getSpaceConsumed(), "getSpaceConsumed");
    assertEquals(-1, contentSummary.getSpaceQuota(), "getSpaceQuota");
  }

  // check the write method
  @Test
  public void testWrite() throws IOException {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66666;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();

    DataOutput out = mock(DataOutput.class);
    InOrder inOrder = inOrder(out);

    contentSummary.write(out);
    inOrder.verify(out).writeLong(length);
    inOrder.verify(out).writeLong(fileCount);
    inOrder.verify(out).writeLong(directoryCount);
    inOrder.verify(out).writeLong(quota);
    inOrder.verify(out).writeLong(spaceConsumed);
    inOrder.verify(out).writeLong(spaceQuota);
  }

  // check the readFields method
  @Test
  public void testReadFields() throws IOException {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66666;

    ContentSummary contentSummary = new ContentSummary.Builder().build();

    DataInput in = mock(DataInput.class);
    when(in.readLong()).thenReturn(length).thenReturn(fileCount)
        .thenReturn(directoryCount).thenReturn(quota).thenReturn(spaceConsumed)
        .thenReturn(spaceQuota);

    contentSummary.readFields(in);
    assertEquals(length, contentSummary.getLength(), "getLength");
    assertEquals(fileCount, contentSummary.getFileCount(), "getFileCount");
    assertEquals(directoryCount, contentSummary.getDirectoryCount(), "getDirectoryCount");
    assertEquals(quota, contentSummary.getQuota(), "getQuota");
    assertEquals(spaceConsumed, contentSummary.getSpaceConsumed(), "getSpaceConsumed");
    assertEquals(spaceQuota, contentSummary.getSpaceQuota(), "getSpaceQuota");
  }

  // check the header with quotas
  @Test
  public void testGetHeaderWithQuota() {
    String header = "       QUOTA       REM_QUOTA     SPACE_QUOTA "
        + "REM_SPACE_QUOTA    DIR_COUNT   FILE_COUNT       CONTENT_SIZE ";
    assertEquals(header, ContentSummary.getHeader(true));
  }

  // check the header without quotas
  @Test
  public void testGetHeaderNoQuota() {
    String header = "   DIR_COUNT   FILE_COUNT       CONTENT_SIZE ";
    assertEquals(header, ContentSummary.getHeader(false));
  }

  // check the toString method with quotas
  @Test
  public void testToStringWithQuota() {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66665;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = "       44444          -11111           66665           11110"
        + "        33333        22222              11111 ";
    assertEquals(expected, contentSummary.toString(true));
  }

  // check the toString method with quotas
  @Test
  public void testToStringNoQuota() {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).build();
    String expected = "        none             inf            none"
        + "             inf        33333        22222              11111 ";
    assertEquals(expected, contentSummary.toString(true));
  }

  // check the toString method with quotas
  @Test
  public void testToStringNoShowQuota() {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66665;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = "       33333        22222              11111 ";
    assertEquals(expected, contentSummary.toString(false));
  }

  // check the toString method (defaults to with quotas)
  @Test
  public void testToString() {
    long length = 11111;
    long fileCount = 22222;
    long directoryCount = 33333;
    long quota = 44444;
    long spaceConsumed = 55555;
    long spaceQuota = 66665;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = "       44444          -11111           66665"
        + "           11110        33333        22222              11111 ";
    assertEquals(expected, contentSummary.toString());
  }

  // check the toString method with quotas
  @Test
  public void testToStringHumanWithQuota() {
    long length = Long.MAX_VALUE;
    long fileCount = 222222222;
    long directoryCount = 33333;
    long quota = 222256578;
    long spaceConsumed = 1073741825;
    long spaceQuota = 1;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = "     212.0 M            1023               1 "
        + "           -1 G       32.6 K      211.9 M              8.0 E ";
    assertEquals(expected, contentSummary.toString(true, true));
  }

  // check the toString method with quotas
  @Test
  public void testToStringHumanNoShowQuota() {
    long length = Long.MAX_VALUE;
    long fileCount = 222222222;
    long directoryCount = 33333;
    long quota = 222256578;
    long spaceConsumed = 55555;
    long spaceQuota = Long.MAX_VALUE;

    ContentSummary contentSummary = new ContentSummary.Builder().length(length).
        fileCount(fileCount).directoryCount(directoryCount).quota(quota).
        spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
    String expected = "      32.6 K      211.9 M              8.0 E ";
    assertEquals(expected, contentSummary.toString(false, true));
  }

  // check the toSnapshot method with human readable.
  @Test
  public void testToSnapshotHumanReadable() {
    long snapshotLength = Long.MAX_VALUE;
    long snapshotFileCount = 222222222;
    long snapshotDirectoryCount = 33333;
    long snapshotSpaceConsumed = 222256578;

    ContentSummary contentSummary = new ContentSummary.Builder()
        .snapshotLength(snapshotLength).snapshotFileCount(snapshotFileCount)
        .snapshotDirectoryCount(snapshotDirectoryCount)
        .snapshotSpaceConsumed(snapshotSpaceConsumed).build();
    String expected =
        "             8.0 E                  211.9 M                   32.6 K "
            + "                     212.0 M ";
    assertEquals(expected, contentSummary.toSnapshot(true));
  }

  // check the toSnapshot method with human readable disabled.
  @Test
  public void testToSnapshotNotHumanReadable() {
    long snapshotLength = 1111;
    long snapshotFileCount = 2222;
    long snapshotDirectoryCount = 3333;
    long snapshotSpaceConsumed = 4444;

    ContentSummary contentSummary = new ContentSummary.Builder()
        .snapshotLength(snapshotLength).snapshotFileCount(snapshotFileCount)
        .snapshotDirectoryCount(snapshotDirectoryCount)
        .snapshotSpaceConsumed(snapshotSpaceConsumed).build();
    String expected =
        "              1111                     2222                     3333 "
            + "                        4444 ";
    assertEquals(expected, contentSummary.toSnapshot(false));
  }
}
