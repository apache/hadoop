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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.InvalidParameterException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;

public class S3GuardFsck {
  private static final Logger LOG = LoggerFactory.getLogger(S3GuardFsck.class);
  public static final String ROOT_PATH_STRING = "/";

  private S3AFileSystem rawFS;

  private DynamoDBMetadataStore metadataStore;

  /**
   * Creates an S3GuardFsck
   * @param fs guarded filesystem backed by dynamo
   * @param ms metadatastore
   */
  S3GuardFsck(S3AFileSystem fs, MetadataStore ms) throws IOException,
      InvalidParameterException {
    this.rawFS = fs;

    if (ms == null) {
      throw new InvalidParameterException("S3AFileSystem should be guarded by"
          + " a " + DynamoDBMetadataStore.class.getCanonicalName());
    }
    this.metadataStore = (DynamoDBMetadataStore) ms;

    if(rawFS.hasMetadataStore()) {
      throw new InvalidParameterException("Raw fs should not have a "
          + "metadatastore.");
    }
  }

  private static S3AFileSystem createUnguardedFS(S3AFileSystem guardedFs)
      throws IOException {
    Configuration rawConfig = new Configuration(guardedFs.getConf());
    URI uri = guardedFs.getUri();

    rawConfig.unset(S3_METADATA_STORE_IMPL);
    rawConfig.unset(METADATASTORE_AUTHORITATIVE);

    S3AFileSystem fs2 = new S3AFileSystem();
    fs2.initialize(uri, rawConfig);
    return fs2;
  }

  public void compareS3toMs(final Path rootPath) throws IOException {
    final S3AFileStatus root =
        (S3AFileStatus) rawFS.getFileStatus(rootPath);

    final Queue<S3AFileStatus> queue = new ArrayDeque<>();
    queue.add(root);

    while (!queue.isEmpty()) {
      // pop front node from the queue
      final S3AFileStatus currentDir = queue.poll();

      // get a listing of that dir from s3
      final Path currentDirPath = currentDir.getPath();
      final List<S3AFileStatus> children =
          Arrays.asList(rawFS.listStatus(currentDirPath)).stream()
              .map(S3AFileStatus.class::cast).collect(toList());

      compareS3DirToMs(currentDir, children);

      // add each dir to queue
      children.stream().filter(pm -> pm.isDirectory())
          .forEach(pm -> queue.add(pm));
    }
  }

  private void compareS3DirToMs(S3AFileStatus s3CurrentDir,
      List<S3AFileStatus> children) throws IOException {
    final Path path = s3CurrentDir.getPath();
    final PathMetadata pathMetadata = metadataStore.get(path);
    List<ComparePair> violationComparePairs = new ArrayList<>();

    final ComparePair rootComparePair =
        compareFileStatusToPathMetadata(s3CurrentDir, pathMetadata);
    if (rootComparePair.containsViolation()) {
      violationComparePairs.add(rootComparePair);
    }


    children.forEach(s3ChildMeta -> {
      try {
        final PathMetadata msChildMeta =
            metadataStore.get(s3ChildMeta.getPath());
        final ComparePair comparePair =
            compareFileStatusToPathMetadata(s3ChildMeta, msChildMeta);
        if (comparePair.containsViolation()) {
          violationComparePairs.add(comparePair);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  private ComparePair compareFileStatusToPathMetadata(S3AFileStatus s3FileStatus,
      PathMetadata msPathMetadata) throws IOException {
    final Path path = s3FileStatus.getPath();
    System.out.println("== Path: " + path);
    ComparePair comparePair = new ComparePair(s3FileStatus, msPathMetadata);

    if (!path.equals(path(ROOT_PATH_STRING))) {
      final Path parentPath = path.getParent();
      final PathMetadata parentPm = metadataStore.get(parentPath);

      if (parentPm == null) {
        LOG.error("Entry does not have a parent entry (not root)");
        comparePair.violations.add(Violation.NO_PARENT_ENTRY);
      } else {
        if (!parentPm.getFileStatus().isDirectory()) {
          LOG.error("An entry’s parent is a file");
          comparePair.violations.add(Violation.PARENT_IS_A_FILE);
        }
        if (parentPm.isDeleted()) {
          LOG.error("The entry's parent tombstoned");
          comparePair.violations.add(Violation.PARENT_TOMBSTONED);
        }
      }
    } else {
      LOG.info("Entry is in the root, so there's no parent");
    }

    if(msPathMetadata == null) {
      LOG.error("No PathMetadata for this path in the MS.");
      comparePair.violations.add(Violation.NO_METADATA_ENTRY);
      return comparePair;
    }
    final S3AFileStatus msFileStatus = msPathMetadata.getFileStatus();
    if (s3FileStatus.isDirectory() && !msFileStatus.isDirectory()) {
      LOG.error("A directory in S3 is a file entry in the MS");
      comparePair.violations.add(Violation.DIR_IN_S3_FILE_IN_MS);
    }
    if (!s3FileStatus.isDirectory() && msFileStatus.isDirectory()) {
      LOG.error("A file in S3 is a directory entry in the MS");
      comparePair.violations.add(Violation.DIR_IN_S3_FILE_IN_MS);
    }

    /**
     * Attribute check
     */
    if(s3FileStatus.getLen() != msFileStatus.getLen()) {
      LOG.error("getLen mismatch - s3: {}, ms: {}",
          s3FileStatus.getLen(), msFileStatus.getLen());
      comparePair.violations.add(Violation.LENGTH_MISMATCH);
    }

    if(s3FileStatus.getModificationTime() != msFileStatus.getModificationTime()) {
      LOG.error("getModificationTime mismatch - s3: {}, ms: {}",
          s3FileStatus.getModificationTime(), msFileStatus.getModificationTime());
      comparePair.violations.add(Violation.MOD_TIME_MISMATCH);
    }

    if(s3FileStatus.getBlockSize() != msFileStatus.getBlockSize()) {
      LOG.error("getBlockSize mismatch - s3: {}, ms: {}",
          s3FileStatus.getBlockSize(), msFileStatus.getBlockSize());
      comparePair.violations.add(Violation.BLOCKSIZE_MISMATCH);
    }

    if(s3FileStatus.getOwner() != msFileStatus.getOwner()) {
      LOG.error("getOwner mismatch - s3: {}, ms: {}",
          s3FileStatus.getOwner(), msFileStatus.getOwner());
      comparePair.violations.add(Violation.OWNER_MISMATCH);
    }

    if(s3FileStatus.getLen() != msFileStatus.getLen()) {
      LOG.error("getLen mismatch - s3: {}, ms: {}",
          s3FileStatus.getLen(), msFileStatus.getLen());
      comparePair.violations.add(Violation.LENGTH_MISMATCH);
    }

    if(s3FileStatus.getETag() == null) {
      // LOG.error("No ETag");
      comparePair.violations.add(Violation.NO_ETAG);
    } else if (!s3FileStatus.getETag().equals(msFileStatus.getETag())) {
      LOG.error("getETag mismatch - s3: {}, ms: {}",
          s3FileStatus.getETag(), msFileStatus.getETag());
      comparePair.violations.add(Violation.ETAG_MISMATCH);
    }

    if(s3FileStatus.getVersionId() == null) {
      // LOG.error("No versionid.");
      comparePair.violations.add(Violation.NO_VERSIONID);
    } else if(s3FileStatus.getVersionId() != msFileStatus.getVersionId()) {
      LOG.error("getVersionId mismatch - s3: {}, ms: {}",
          s3FileStatus.getVersionId(), msFileStatus.getVersionId());
      comparePair.violations.add(Violation.VERSIONID_MISMATCH);
    }

    return comparePair;
  }

  private Path path(String s) {
    return rawFS.makeQualified(new Path(s));
  }


  public static class ComparePair {
    private S3AFileStatus s3FileStatus;
    private PathMetadata msPathMetadata;

    private Set<Violation> violations = new HashSet<>();

    ComparePair(S3AFileStatus status, PathMetadata pm) {
      this.s3FileStatus = status;
      this.msPathMetadata = pm;
    }

    public S3AFileStatus getS3FileStatus() {
      return s3FileStatus;
    }

    public PathMetadata getMsPathMetadata() {
      return msPathMetadata;
    }

    public Set<Violation> getViolations() {
      return violations;
    }

    public boolean containsViolation() {
      return !violations.isEmpty();
    }
  }

  public enum Violation {
    // No entry in metadatastore
    NO_METADATA_ENTRY,
    // A file or directory entry does not have a parent entry - excluding
    // files and directories in the root.
    NO_PARENT_ENTRY,
    // An entry’s parent is a file
    PARENT_IS_A_FILE,
    // A file exists under a path for which there is a tombstone entry in the
    // MS
     PARENT_TOMBSTONED,
    // A directory in S3 is a file entry in the MS
    DIR_IN_S3_FILE_IN_MS,
    // Attribute mismatch
    LENGTH_MISMATCH,
    MOD_TIME_MISMATCH,
    BLOCKSIZE_MISMATCH,
    OWNER_MISMATCH,
    VERSIONID_MISMATCH,
    ETAG_MISMATCH,
    NO_ETAG,
    NO_VERSIONID
  }
}
