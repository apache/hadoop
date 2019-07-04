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

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.ScanExpressionSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
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

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.itemToPathMetadata;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.pathToParentKeyAttribute;
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
  public void compareS3toMs() throws Exception {
    final FileStatus root = rawFS.getFileStatus(path("/"));

    final Queue<FileStatus> queue = new ArrayDeque<>();
    queue.add(root);

    while (!queue.isEmpty()) {
      // pop front node from the queue
      final FileStatus currentDir = queue.poll();

      // get a listing of that dir from s3
      final Path currentDirPath = currentDir.getPath();
      final List<FileStatus> children =
          Arrays.asList(rawFS.listStatus(currentDirPath));

      compareS3DirToMs(currentDir, children);

      // add each dir to queue
      children.stream().filter(pm -> pm.isDirectory())
          .forEach(pm -> queue.add(pm));
    }
  }

  private void compareS3DirToMs(FileStatus s3CurrentDir,
      List<FileStatus> children) throws Exception {
    final Path path = s3CurrentDir.getPath();
    final PathMetadata pathMetadata = metadataStore.get(path);
    final DirListingMetadata dirListingMetadata =
        metadataStore.listChildren(path);

    compareFileStatusToPathMetadata(s3CurrentDir, pathMetadata);

    children.forEach(s3ChildMeta -> {
      try {
        final PathMetadata msChildMeta =
            metadataStore.get(s3ChildMeta.getPath());
        compareFileStatusToPathMetadata(s3ChildMeta, msChildMeta);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

  }

  private void compareFileStatusToPathMetadata(FileStatus s3FileStatus,
      PathMetadata msPathMetadata) throws Exception {
    final Path path = s3FileStatus.getPath();
    System.out.println("== Path: " + path);

    if (!path.equals(path("/"))) {
      final Path parentPath = path.getParent();
      final PathMetadata parentPm = metadataStore.get(parentPath);

      if (parentPm == null) {
        LOG.error("Entry does not have a parent entry");
      } else {
        if (!parentPm.getFileStatus().isDirectory()) {
          LOG.error("An entry’s parent is a file");
        }
        if (parentPm.isDeleted()) {
          LOG.error("The entry's parent tombstoned");
        }
      }
    } else {
      System.out.println("Root does not have a parent.");
    }

    if(msPathMetadata == null) {
      LOG.error("No PathMetadata for this path in the MS.");
      return;
    }
    final S3AFileStatus msFileStatus = msPathMetadata.getFileStatus();
    if (s3FileStatus.isDirectory() && !msFileStatus.isDirectory()) {
      LOG.error("A directory in S3 is a file entry in the MS");
    }
    if (!s3FileStatus.isDirectory() && msFileStatus.isDirectory()) {
      LOG.error("A file in S3 is a directory entry in the MS");
    }

    // Attribute check
    if (msPathMetadata.isDeleted()) {
      LOG.error("Path exists where the parent has a tombstone marker.");
    }

    if(s3FileStatus.getLen() != msFileStatus.getLen()) {
      LOG.error("getLen mismatch - s3: {}, ms: {}",
          s3FileStatus.getLen(), msFileStatus.getLen());
    }

    if(s3FileStatus.getModificationTime() != msFileStatus.getModificationTime()) {
      LOG.error("getModificationTime mismatch - s3: {}, ms: {}",
          s3FileStatus.getModificationTime(), msFileStatus.getModificationTime());
    }

    if(s3FileStatus.getBlockSize() != msFileStatus.getBlockSize()) {
      LOG.error("getBlockSize mismatch - s3: {}, ms: {}",
          s3FileStatus.getBlockSize(), msFileStatus.getBlockSize());
    }

    if(s3FileStatus.getOwner() != msFileStatus.getOwner()) {
      LOG.error("getOwner mismatch - s3: {}, ms: {}",
          s3FileStatus.getOwner(), msFileStatus.getOwner());
    }

    if(s3FileStatus.getLen() != msFileStatus.getLen()) {
      LOG.error("getLen mismatch - s3: {}, ms: {}",
          s3FileStatus.getLen(), msFileStatus.getLen());
    }
  }

  @Test
  public void testBuildGraphFromDynamo() throws Exception {
    S3GuardTableAccess tableAccess = new S3GuardTableAccess(
        (DynamoDBMetadataStore) metadataStore);

    ExpressionSpecBuilder builder = new ExpressionSpecBuilder();
    builder.withKeyCondition(
        ExpressionSpecBuilder.S("parent")
        .beginsWith("/")
    );
    final Iterable<DDBPathMetadata> ddbPathMetadata =
        tableAccess.scanMetadata(builder);

    ddbPathMetadata.iterator().forEachRemaining(pmd -> {
      if(!(pmd instanceof S3GuardTableAccess.VersionMarker)) {
        System.out.println(pmd.getFileStatus().getPath());
        // add node ...
      }
    });
  }
}