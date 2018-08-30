/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.utils.db.DBConfigFromFile.getOptionsFileNameFromDB;

/**
 * DBConf tests.
 */
public class TestDBConfigFromFile {
  private final static String DB_FILE = "test.db";
  private final static String INI_FILE = getOptionsFileNameFromDB(DB_FILE);
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    System.setProperty(DBConfigFromFile.CONFIG_DIR,
        folder.newFolder().toString());
    ClassLoader classLoader = getClass().getClassLoader();
    File testData = new File(classLoader.getResource(INI_FILE).getFile());
    File dest = Paths.get(
        System.getProperty(DBConfigFromFile.CONFIG_DIR), INI_FILE).toFile();
    FileUtils.copyFile(testData, dest);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void readFromFile() throws IOException {
    final List<String> families =
        Arrays.asList(DFSUtil.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
            "First", "Second", "Third",
            "Fourth", "Fifth",
            "Sixth");
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<>();
    for (String family : families) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(family.getBytes(StandardCharsets.UTF_8),
              new ColumnFamilyOptions()));
    }

    final DBOptions options = DBConfigFromFile.readFromFile(DB_FILE,
        columnFamilyDescriptors);

    // Some Random Values Defined in the test.db.ini, we verify that we are
    // able to get values that are defined in the test.db.ini.
    Assert.assertNotNull(options);
    Assert.assertEquals(551615L, options.maxManifestFileSize());
    Assert.assertEquals(1000L, options.keepLogFileNum());
    Assert.assertEquals(1048576, options.writableFileMaxBufferSize());
  }

  @Test
  public void readFromFileInvalidConfig() throws IOException {
    final List<String> families =
        Arrays.asList(DFSUtil.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
            "First", "Second", "Third",
            "Fourth", "Fifth",
            "Sixth");
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<>();
    for (String family : families) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(family.getBytes(StandardCharsets.UTF_8),
              new ColumnFamilyOptions()));
    }

    final DBOptions options = DBConfigFromFile.readFromFile("badfile.db.ini",
        columnFamilyDescriptors);

    // This has to return a Null, since we have config defined for badfile.db
    Assert.assertNull(options);
  }
}