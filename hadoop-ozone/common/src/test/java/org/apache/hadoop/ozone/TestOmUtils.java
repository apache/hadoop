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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link OmUtils}.
 */
public class TestOmUtils {
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
  public void testCreateTarFile() throws Exception {

    File tempSnapshotDir = null;
    FileInputStream fis = null;
    FileOutputStream fos = null;
    File tarFile = null;

    try {
      String testDirName = System.getProperty("java.io.tmpdir");
      if (!testDirName.endsWith("/")) {
        testDirName += "/";
      }
      testDirName += "TestCreateTarFile_Dir" + System.currentTimeMillis();
      tempSnapshotDir = new File(testDirName);
      tempSnapshotDir.mkdirs();

      File file = new File(testDirName + "/temp1.txt");
      FileWriter writer = new FileWriter(file);
      writer.write("Test data 1");
      writer.close();

      file = new File(testDirName + "/temp2.txt");
      writer = new FileWriter(file);
      writer.write("Test data 2");
      writer.close();

      tarFile = OmUtils.createTarFile(Paths.get(testDirName));
      Assert.assertNotNull(tarFile);

    } finally {
      IOUtils.closeStream(fis);
      IOUtils.closeStream(fos);
      FileUtils.deleteDirectory(tempSnapshotDir);
      FileUtils.deleteQuietly(tarFile);
    }
  }
}
