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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the EditLogFileOutputStream
 */
public class TestEditLogFileOutputStream {
  private final static File TEST_DIR = PathUtils
      .getTestDir(TestEditLogFileOutputStream.class);
  private static final File TEST_EDITS = new File(TEST_DIR,
      "testEditLogFileOutput.log");
  final static int MIN_PREALLOCATION_LENGTH = EditLogFileOutputStream.MIN_PREALLOCATION_LENGTH;

  private Configuration conf;

  @BeforeClass
  public static void disableFsync() {
    // No need to fsync for the purposes of tests. This makes
    // the tests run much faster.
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }

  @Before
  @After
  public void deleteEditsFile() {
    if (TEST_EDITS.exists())
      TEST_EDITS.delete();
  }

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  static void flushAndCheckLength(EditLogFileOutputStream elos,
      long expectedLength) throws IOException {
    elos.setReadyToFlush();
    elos.flushAndSync(true);
    assertEquals(expectedLength, elos.getFile().length());
  }

  /**
   * Tests writing to the EditLogFileOutputStream. Due to preallocation, the
   * length of the edit log will usually be longer than its valid contents.
   */
  @Test
  public void testRawWrites() throws IOException {
    EditLogFileOutputStream elos = new EditLogFileOutputStream(conf,
        TEST_EDITS, 0);
    try {
      byte[] small = new byte[] { 1, 2, 3, 4, 5, 8, 7 };
      elos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      // The first (small) write we make extends the file by 1 MB due to
      // preallocation.
      elos.writeRaw(small, 0, small.length);
      flushAndCheckLength(elos, MIN_PREALLOCATION_LENGTH);
      // The next small write we make goes into the area that was already
      // preallocated.
      elos.writeRaw(small, 0, small.length);
      flushAndCheckLength(elos, MIN_PREALLOCATION_LENGTH);
      // Now we write enough bytes so that we exceed the minimum preallocated
      // length.
      final int BIG_WRITE_LENGTH = 3 * MIN_PREALLOCATION_LENGTH;
      byte[] buf = new byte[4096];
      for (int i = 0; i < buf.length; i++) {
        buf[i] = 0;
      }
      int total = BIG_WRITE_LENGTH;
      while (total > 0) {
        int toWrite = (total > buf.length) ? buf.length : total;
        elos.writeRaw(buf, 0, toWrite);
        total -= toWrite;
      }
      flushAndCheckLength(elos, 4 * MIN_PREALLOCATION_LENGTH);
    } finally {
      if (elos != null)
        elos.close();
    }
  }

  /**
   * Tests EditLogFileOutputStream doesn't throw NullPointerException on
   * close/abort sequence. See HDFS-2011.
   */
  @Test
  public void testEditLogFileOutputStreamCloseAbort() throws IOException {
    // abort after a close should just ignore
    EditLogFileOutputStream editLogStream = new EditLogFileOutputStream(conf,
        TEST_EDITS, 0);
    editLogStream.close();
    editLogStream.abort();
  }

  /**
   * Tests EditLogFileOutputStream doesn't throw NullPointerException on
   * close/close sequence. See HDFS-2011.
   */
  @Test
  public void testEditLogFileOutputStreamCloseClose() throws IOException {
    // close after a close should result in an IOE
    EditLogFileOutputStream editLogStream = new EditLogFileOutputStream(conf,
        TEST_EDITS, 0);
    editLogStream.close();
    try {
      editLogStream.close();
    } catch (IOException ioe) {
      String msg = StringUtils.stringifyException(ioe);
      assertTrue(msg, msg.contains("Trying to use aborted output stream"));
    }
  }

  /**
   * Tests EditLogFileOutputStream doesn't throw NullPointerException on being
   * abort/abort sequence. See HDFS-2011.
   */
  @Test
  public void testEditLogFileOutputStreamAbortAbort() throws IOException {
    // abort after a close should just ignore
    EditLogFileOutputStream editLogStream = null;
    try {
      editLogStream = new EditLogFileOutputStream(conf, TEST_EDITS, 0);
      editLogStream.abort();
      editLogStream.abort();
    } finally {
      IOUtils.cleanupWithLogger(null, editLogStream);
    }
  }
}
