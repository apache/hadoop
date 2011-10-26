/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;

@RunWith(Parameterized.class)
public class TestFixedFileTrailer {

  private static final Log LOG = LogFactory.getLog(TestFixedFileTrailer.class);

  /** The number of used fields by version. Indexed by version minus one. */
  private static final int[] NUM_FIELDS_BY_VERSION = new int[] { 8, 13 };

  private HBaseTestingUtility util = new HBaseTestingUtility();
  private FileSystem fs;
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private int version;

  static {
    assert NUM_FIELDS_BY_VERSION.length == HFile.MAX_FORMAT_VERSION
        - HFile.MIN_FORMAT_VERSION + 1;
  }

  public TestFixedFileTrailer(int version) {
    this.version = version;
  }

  @Parameters
  public static Collection<Object[]> getParameters() {
    List<Object[]> versionsToTest = new ArrayList<Object[]>();
    for (int v = HFile.MIN_FORMAT_VERSION; v <= HFile.MAX_FORMAT_VERSION; ++v)
      versionsToTest.add(new Integer[] { v } );
    return versionsToTest;
  }

  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(util.getConfiguration());
  }

  @Test
  public void testTrailer() throws IOException {
    FixedFileTrailer t = new FixedFileTrailer(version);
    t.setDataIndexCount(3);
    t.setEntryCount(((long) Integer.MAX_VALUE) + 1);

    if (version == 1) {
      t.setFileInfoOffset(876);
    }

    if (version == 2) {
      t.setLastDataBlockOffset(291);
      t.setNumDataIndexLevels(3);
      t.setComparatorClass(KeyValue.KEY_COMPARATOR.getClass());
      t.setFirstDataBlockOffset(9081723123L); // Completely unrealistic.
      t.setUncompressedDataIndexSize(827398717L); // Something random.
    }

    t.setLoadOnOpenOffset(128);
    t.setMetaIndexCount(7);

    t.setTotalUncompressedBytes(129731987);

    {
      DataOutputStream dos = new DataOutputStream(baos); // Limited scope.
      t.serialize(dos);
      dos.flush();
      assertEquals(dos.size(), FixedFileTrailer.getTrailerSize(version));
    }

    byte[] bytes = baos.toByteArray();
    baos.reset();

    assertEquals(bytes.length, FixedFileTrailer.getTrailerSize(version));

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    // Finished writing, trying to read.
    {
      DataInputStream dis = new DataInputStream(bais);
      FixedFileTrailer t2 = new FixedFileTrailer(version);
      t2.deserialize(dis);
      assertEquals(-1, bais.read()); // Ensure we have read everything.
      checkLoadedTrailer(version, t, t2);
    }

    // Now check what happens if the trailer is corrupted.
    Path trailerPath = new Path(util.getDataTestDir(), "trailer_"
        + version);

    {
      for (byte invalidVersion : new byte[] { HFile.MIN_FORMAT_VERSION - 1,
          HFile.MAX_FORMAT_VERSION + 1}) {
        bytes[bytes.length - 1] = invalidVersion;
        writeTrailer(trailerPath, null, bytes);
        try {
          readTrailer(trailerPath);
          fail("Exception expected");
        } catch (IOException ex) {
          // Make it easy to debug this.
          String msg = ex.getMessage();
          String cleanMsg = msg.replaceAll(
              "^(java(\\.[a-zA-Z]+)+:\\s+)?|\\s+\\(.*\\)\\s*$", "");
          assertEquals("Actual exception message is \"" + msg + "\".\n" +
              "Cleaned-up message", // will be followed by " expected: ..."
              "Invalid HFile version: " + invalidVersion, cleanMsg);
          LOG.info("Got an expected exception: " + msg);
        }
      }

    }

    // Now write the trailer into a file and auto-detect the version.
    writeTrailer(trailerPath, t, null);

    FixedFileTrailer t4 = readTrailer(trailerPath);

    checkLoadedTrailer(version, t, t4);

    String trailerStr = t.toString();
    assertEquals("Invalid number of fields in the string representation "
        + "of the trailer: " + trailerStr, NUM_FIELDS_BY_VERSION[version - 1],
        trailerStr.split(", ").length);
    assertEquals(trailerStr, t4.toString());
  }

  private FixedFileTrailer readTrailer(Path trailerPath) throws IOException {
    FSDataInputStream fsdis = fs.open(trailerPath);
    FixedFileTrailer trailerRead = FixedFileTrailer.readFromStream(fsdis,
        fs.getFileStatus(trailerPath).getLen());
    fsdis.close();
    return trailerRead;
  }

  private void writeTrailer(Path trailerPath, FixedFileTrailer t,
      byte[] useBytesInstead) throws IOException {
    assert (t == null) != (useBytesInstead == null); // Expect one non-null.

    FSDataOutputStream fsdos = fs.create(trailerPath);
    fsdos.write(135); // to make deserializer's job less trivial
    if (useBytesInstead != null) {
      fsdos.write(useBytesInstead);
    } else {
      t.serialize(fsdos);
    }
    fsdos.close();
  }

  private void checkLoadedTrailer(int version, FixedFileTrailer expected,
      FixedFileTrailer loaded) throws IOException {
    assertEquals(version, loaded.getVersion());
    assertEquals(expected.getDataIndexCount(), loaded.getDataIndexCount());

    assertEquals(Math.min(expected.getEntryCount(),
        version == 1 ? Integer.MAX_VALUE : Long.MAX_VALUE),
        loaded.getEntryCount());

    if (version == 1) {
      assertEquals(expected.getFileInfoOffset(), loaded.getFileInfoOffset());
    }

    if (version == 2) {
      assertEquals(expected.getLastDataBlockOffset(),
          loaded.getLastDataBlockOffset());
      assertEquals(expected.getNumDataIndexLevels(),
          loaded.getNumDataIndexLevels());
      assertEquals(expected.createComparator().getClass().getName(),
          loaded.createComparator().getClass().getName());
      assertEquals(expected.getFirstDataBlockOffset(),
          loaded.getFirstDataBlockOffset());
      assertTrue(
          expected.createComparator() instanceof KeyValue.KeyComparator);
      assertEquals(expected.getUncompressedDataIndexSize(),
          loaded.getUncompressedDataIndexSize());
    }

    assertEquals(expected.getLoadOnOpenDataOffset(),
        loaded.getLoadOnOpenDataOffset());
    assertEquals(expected.getMetaIndexCount(), loaded.getMetaIndexCount());

    assertEquals(expected.getTotalUncompressedBytes(),
        loaded.getTotalUncompressedBytes());
  }

}
