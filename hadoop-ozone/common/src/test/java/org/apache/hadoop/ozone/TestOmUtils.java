/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link OmUtils}.
 */
public class TestOmUtils {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(60_000);

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  /**
   * Test {@link OmUtils#getOmDbDir}.
   */
  @Test
  public void testGetOmDbDir() {
    final File testDir = PathUtils.getTestDir(TestOmUtils.class);
    final File dbDir = new File(testDir, "omDbDir");
    final File metaDir = new File(testDir, "metaDir");   // should be ignored.
    final Configuration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbDir.getPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertEquals(dbDir, OmUtils.getOmDbDir(conf));
      assertTrue(dbDir.exists());          // should have been created.
    } finally {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  /**
   * Test {@link OmUtils#getOmDbDir} with fallback to OZONE_METADATA_DIRS
   * when OZONE_OM_DB_DIRS is undefined.
   */
  @Test
  public void testGetOmDbDirWithFallback() {
    final File testDir = PathUtils.getTestDir(TestOmUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    final Configuration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertEquals(metaDir, OmUtils.getOmDbDir(conf));
      assertTrue(metaDir.exists());        // should have been created.
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testNoOmDbDirConfigured() {
    thrown.expect(IllegalArgumentException.class);
    OmUtils.getOmDbDir(new OzoneConfiguration());
  }

  @Test
  public void testWriteCheckpointToOutputStream() throws Exception {

    FileInputStream fis = null;
    FileOutputStream fos = null;

    try {
      String testDirName = folder.newFolder().getAbsolutePath();
      File file = new File(testDirName + "/temp1.txt");
      FileWriter writer = new FileWriter(file);
      writer.write("Test data 1");
      writer.close();

      file = new File(testDirName + "/temp2.txt");
      writer = new FileWriter(file);
      writer.write("Test data 2");
      writer.close();

      File outputFile =
          new File(Paths.get(testDirName, "output_file.tgz").toString());
      TestDBCheckpoint dbCheckpoint = new TestDBCheckpoint(
          Paths.get(testDirName));
      OmUtils.writeOmDBCheckpointToStream(dbCheckpoint,
          new FileOutputStream(outputFile));
      assertNotNull(outputFile);
    } finally {
      IOUtils.closeStream(fis);
      IOUtils.closeStream(fos);
    }
  }

}

class TestDBCheckpoint implements DBCheckpoint {

  private Path checkpointFile;

  TestDBCheckpoint(Path checkpointFile) {
    this.checkpointFile = checkpointFile;
  }

  @Override
  public Path getCheckpointLocation() {
    return checkpointFile;
  }

  @Override
  public long getCheckpointTimestamp() {
    return 0;
  }

  @Override
  public long getLatestSequenceNumber() {
    return 0;
  }

  @Override
  public long checkpointCreationTimeTaken() {
    return 0;
  }

  @Override
  public void cleanupCheckpoint() throws IOException {
    FileUtils.deleteDirectory(checkpointFile.toFile());
  }

  @Override
  public void setRatisSnapshotIndex(long omRatisSnapshotIndex) {
  }

  @Override
  public long getRatisSnapshotIndex() {
    return 0;
  }
}
