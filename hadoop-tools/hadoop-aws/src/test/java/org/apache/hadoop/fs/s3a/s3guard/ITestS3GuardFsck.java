/*
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

package org.apache.hadoop.fs.s3a.s3guard;

import com.amazonaws.services.dynamodbv2.document.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.junit.Assume.assumeTrue;

public class ITestS3GuardFsck extends AbstractS3ATestBase {

  private S3AFileSystem guardedFs;
  private S3AFileSystem rawFS;

  private MetadataStore metadataStore;

  @Before
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    // These test will fail if no ms
    assumeTrue("FS needs to have a metadatastore.",
        fs.hasMetadataStore());
    assumeTrue("Metadatastore should persist authoritative bit",
        metadataStorePersistsAuthoritativeBit(fs.getMetadataStore()));

    guardedFs = fs;
    metadataStore = fs.getMetadataStore();

    // create raw fs without s3guard
    rawFS = createUnguardedFS();
    assertFalse("Raw FS still has S3Guard " + rawFS,
        rawFS.hasMetadataStore());
  }

  /**
   * Create a test filesystem which is always unguarded.
   * This filesystem MUST be closed in test teardown.
   * @return the new FS
   */
  private S3AFileSystem createUnguardedFS() throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    removeBaseAndBucketOverrides(uri.getHost(), config,
        S3_METADATA_STORE_IMPL);
    removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE);
    S3AFileSystem fs2 = new S3AFileSystem();
    fs2.initialize(uri, config);
    return fs2;
  }

  @Test
  public void testTraverseS3() throws Exception {
    final FileStatus root = rawFS.getFileStatus(path("/"));

    final Queue<FileStatus> queue = new ArrayDeque<>();
    queue.add(root);

    System.out.println("Dir structure: ");

    while (!queue.isEmpty()) {
      // pop front node from the queue and print it
      final FileStatus currentDir = queue.poll();

      System.out.println(currentDir.getPath());

      // get a listing of that dir from s3
      final Path currentDirPath = currentDir.getPath();
      final List<FileStatus> children =
          Arrays.asList(rawFS.listStatus(currentDirPath));

      // add each elem to queue
      children.stream().filter(pm -> pm.isDirectory())
          .forEach(pm -> queue.add(pm));

    }
  }


  @Test
  public void testTraverseDynamo() throws Exception {
    final PathMetadata root = metadataStore.get(path("/"));

    final Queue<PathMetadata> queue = new ArrayDeque<>();
    queue.add(root);

    System.out.println("Dir structure: ");

    while (!queue.isEmpty()) {
      // pop front node from the queue and print it
      final PathMetadata currentDir = queue.poll();

      System.out.println(currentDir.getFileStatus().getPath());

      // get a listing of that dir from dynamo
      final Path currentDirPath = currentDir.getFileStatus().getPath();
      final Collection<PathMetadata> children =
          metadataStore.listChildren(currentDirPath).getListing();

      // add each elem to queue
      children.stream().filter(pm -> pm.getFileStatus().isDirectory())
          .forEach(pm -> queue.add(pm));
    }

  }

  @Test
  public void testBuildGraphFromDynamo() {
    final DynamoDBMetadataStore dynMS =
        (DynamoDBMetadataStore) metadataStore;

    final Table table = dynMS.getTable();
    table.get

  }
}