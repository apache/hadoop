/*
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
package org.apache.hadoop.hdds.server;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.test.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ServerUtils}.
 */
public class TestServerUtils {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Test {@link ServerUtils#getScmDbDir}.
   */
  @Test
  public void testGetScmDbDir() {
    final File testDir = PathUtils.getTestDir(TestServerUtils.class);
    final File dbDir = new File(testDir, "scmDbDir");
    final File metaDir = new File(testDir, "metaDir");
    final Configuration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dbDir.getPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertFalse(metaDir.exists());
      assertFalse(dbDir.exists());
      assertEquals(dbDir, ServerUtils.getScmDbDir(conf));
      assertTrue(dbDir.exists());
      assertFalse(metaDir.exists());
    } finally {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  /**
   * Test {@link ServerUtils#getScmDbDir} with fallback to OZONE_METADATA_DIRS
   * when OZONE_SCM_DB_DIRS is undefined.
   */
  @Test
  public void testGetScmDbDirWithFallback() {
    final File testDir = PathUtils.getTestDir(TestServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    final Configuration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());
    try {
      assertFalse(metaDir.exists());
      assertEquals(metaDir, ServerUtils.getScmDbDir(conf));
      assertTrue(metaDir.exists());
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testNoScmDbDirConfigured() {
    thrown.expect(IllegalArgumentException.class);
    ServerUtils.getScmDbDir(new OzoneConfiguration());
  }

  @Test
  public void ozoneMetadataDirIsMandatory() {
    thrown.expect(IllegalArgumentException.class);
    ServerUtils.getOzoneMetaDirPath(new OzoneConfiguration());
  }

  @Test
  public void ozoneMetadataDirAcceptsSingleItem() {
    final File testDir = PathUtils.getTestDir(TestServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    final Configuration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertFalse(metaDir.exists());
      assertEquals(metaDir, ServerUtils.getOzoneMetaDirPath(conf));
      assertTrue(metaDir.exists());
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void ozoneMetadataDirRejectsList() {
    final Configuration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, "/data/meta1,/data/meta2");
    thrown.expect(IllegalArgumentException.class);

    ServerUtils.getOzoneMetaDirPath(conf);
  }

}
