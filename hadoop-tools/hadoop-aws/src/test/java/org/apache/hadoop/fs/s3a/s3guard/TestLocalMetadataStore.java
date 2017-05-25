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

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * MetadataStore unit test for {@link LocalMetadataStore}.
 */
public class TestLocalMetadataStore extends MetadataStoreTestBase {

  private static final String MAX_ENTRIES_STR = "16";

  private final static class LocalMSContract extends AbstractMSContract {

    private FileSystem fs;

    private LocalMSContract() throws IOException {
      this(new Configuration());
    }

    private LocalMSContract(Configuration config) throws IOException {
      config.set(LocalMetadataStore.CONF_MAX_RECORDS, MAX_ENTRIES_STR);
      fs = FileSystem.getLocal(config);
    }

    @Override
    public FileSystem getFileSystem() {
      return fs;
    }

    @Override
    public MetadataStore getMetadataStore() throws IOException {
      LocalMetadataStore lms = new LocalMetadataStore();
      return lms;
    }
  }

  @Override
  public AbstractMSContract createContract() throws IOException {
    return new LocalMSContract();
  }

  @Override
  public AbstractMSContract createContract(Configuration conf) throws
      IOException {
    return new LocalMSContract(conf);
  }

  @Test
  public void testClearByAncestor() {
    Map<Path, PathMetadata> map = new HashMap<>();

    // 1. Test paths without scheme/host
    assertClearResult(map, "", "/", 0);
    assertClearResult(map, "", "/dirA/dirB", 2);
    assertClearResult(map, "", "/invalid", 5);


    // 2. Test paths w/ scheme/host
    String p = "s3a://fake-bucket-name";
    assertClearResult(map, p, "/", 0);
    assertClearResult(map, p, "/dirA/dirB", 2);
    assertClearResult(map, p, "/invalid", 5);
  }

  private static void populateMap(Map<Path, PathMetadata> map,
      String prefix) {
    populateEntry(map, new Path(prefix + "/dirA/dirB/"));
    populateEntry(map, new Path(prefix + "/dirA/dirB/dirC"));
    populateEntry(map, new Path(prefix + "/dirA/dirB/dirC/file1"));
    populateEntry(map, new Path(prefix + "/dirA/dirB/dirC/file2"));
    populateEntry(map, new Path(prefix + "/dirA/file1"));
  }

  private static void populateEntry(Map<Path, PathMetadata> map,
      Path path) {
    map.put(path, new PathMetadata(new FileStatus(0, true, 0, 0, 0, path)));
  }

  private static int sizeOfMap(Map<Path, PathMetadata> map) {
    int count = 0;
    for (PathMetadata meta : map.values()) {
      if (!meta.isDeleted()) {
        count++;
      }
    }
    return count;
  }

  private static void assertClearResult(Map <Path, PathMetadata> map,
      String prefixStr, String pathStr, int leftoverSize) {
    populateMap(map, prefixStr);
    LocalMetadataStore.deleteHashByAncestor(new Path(prefixStr + pathStr), map,
        true);
    assertEquals(String.format("Map should have %d entries", leftoverSize),
        leftoverSize, sizeOfMap(map));
    map.clear();
  }

  @Override
  protected void verifyFileStatus(FileStatus status, long size) {
    S3ATestUtils.verifyFileStatus(status, size, REPLICATION, getModTime(),
        getAccessTime(),
        BLOCK_SIZE, OWNER, GROUP, PERMISSION);
  }

  @Override
  protected void verifyDirStatus(FileStatus status) {
    S3ATestUtils.verifyDirStatus(status, REPLICATION, getModTime(),
        getAccessTime(), OWNER, GROUP, PERMISSION);
  }

}
