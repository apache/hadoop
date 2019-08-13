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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Main class for the FSCK factored out from S3GuardTool
 * The implementation uses fixed DynamoDBMetadataStore as the backing store
 * for metadata.
 *
 * Functions:
 * <ul>
 *   <li>Checking metadata consistency between S3 and metadatastore</li>
 * </ul>
 */
public class S3GuardFsck {
  private static final Logger LOG = LoggerFactory.getLogger(S3GuardFsck.class);
  public static final String ROOT_PATH_STRING = "/";

  private S3AFileSystem rawFS;
  private DynamoDBMetadataStore metadataStore;

  /**
   * Creates an S3GuardFsck.
   * @param fs the filesystem to compare to
   * @param ms metadatastore the metadatastore to compare with (dynamo)
   */
  S3GuardFsck(S3AFileSystem fs, MetadataStore ms)
      throws InvalidParameterException {
    this.rawFS = fs;

    if (ms == null) {
      throw new InvalidParameterException("S3AFileSystem should be guarded by"
          + " a " + DynamoDBMetadataStore.class.getCanonicalName());
    }
    this.metadataStore = (DynamoDBMetadataStore) ms;

    if (rawFS.hasMetadataStore()) {
      throw new InvalidParameterException("Raw fs should not have a "
          + "metadatastore.");
    }
  }

  /**
   * Compares S3 to MS.
   * Iterative breadth first walk on the S3 structure from a given root.
   * Creates a list of pairs (metadata in S3 and in the MetadataStore) where
   * the consistency or any rule is violated.
   * Uses {@link S3GuardFsckViolationHandler} to handle violations.
   * The violations are listed in Enums: {@link Violation}
   *
   * @param p the root path to start the traversal
   * @throws IOException
   * @return
   */
  public List<ComparePair> compareS3RootToMs(Path p) throws IOException {
    final Path rootPath = rawFS.qualify(p);
    final S3AFileStatus root =
        (S3AFileStatus) rawFS.getFileStatus(rootPath);
    final List<ComparePair> comparePairs = new ArrayList<>();
    final Queue<S3AFileStatus> queue = new ArrayDeque<>();
    queue.add(root);

    while (!queue.isEmpty()) {
      // pop front node from the queue
      final S3AFileStatus currentDir = queue.poll();

      // Get a listing of that dir from s3 and add just the files.
      // (Each directory will be added as a root.)
      // Files should be casted to S3AFileStatus instead of plain FileStatus
      // to get the VersionID and Etag.
      final Path currentDirPath = currentDir.getPath();

      final FileStatus[] s3DirListing = rawFS.listStatus(currentDirPath);
      final List<S3AFileStatus> children =
          Arrays.asList(s3DirListing).stream()
              .filter(status -> !status.isDirectory())
              .map(S3AFileStatus.class::cast).collect(toList());

      // Compare the directory contents if the listing is authoritative
      final DirListingMetadata msDirListing =
          metadataStore.listChildren(currentDirPath);
      if (msDirListing != null && msDirListing.isAuthoritative()) {
        final ComparePair cP =
            compareAuthDirListing(s3DirListing, msDirListing);
        if (cP.containsViolation()) {
          comparePairs.add(cP);
        }
      }

      // Compare directory and contents, but not the listing
      final List<ComparePair> compareResult =
          compareS3DirToMs(currentDir, children).stream()
              .filter(comparePair -> comparePair.containsViolation())
              .collect(toList());
      comparePairs.addAll(compareResult);

      // Add each dir to queue
      children.stream().filter(pm -> pm.isDirectory())
          .forEach(pm -> queue.add(pm));
    }

    // Create a handler and handle each violated pairs
    S3GuardFsckViolationHandler handler =
        new S3GuardFsckViolationHandler(rawFS, metadataStore);
    comparePairs.forEach(handler::handle);

    return comparePairs;
  }

  private ComparePair compareAuthDirListing(FileStatus[] s3DirListing,
      DirListingMetadata msDirListing) {
    ComparePair cP = new ComparePair(s3DirListing, msDirListing);

    if (!msDirListing.isAuthoritative()) {
      return cP;
    }

    if (s3DirListing.length != msDirListing.numEntries()) {
      cP.violations.add(Violation.AUTHORITATIVE_DIRECTORY_CONTENT_MISMATCH);
    } else {
      final Set<Path> msPaths = msDirListing.getListing().stream()
              .map(pm -> pm.getFileStatus().getPath()).collect(toSet());
      final Set<Path> s3Paths = Arrays.stream(s3DirListing)
              .map(pm -> pm.getPath()).collect(toSet());
      if (!s3Paths.equals(msPaths)) {
        cP.violations.add(Violation.AUTHORITATIVE_DIRECTORY_CONTENT_MISMATCH);
      }
    }

    return cP;
  }

  protected List<ComparePair> compareS3DirToMs(S3AFileStatus s3CurrentDir,
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

    return violationComparePairs;
  }

  protected ComparePair compareFileStatusToPathMetadata(
      S3AFileStatus s3FileStatus,
      PathMetadata msPathMetadata) throws IOException {
    final Path path = s3FileStatus.getPath();
    System.out.println("== Path: " + path);
    ComparePair comparePair = new ComparePair(s3FileStatus, msPathMetadata);

    if (!path.equals(path(ROOT_PATH_STRING))) {
      final Path parentPath = path.getParent();
      final PathMetadata parentPm = metadataStore.get(parentPath);

      if (parentPm == null) {
        comparePair.violations.add(Violation.NO_PARENT_ENTRY);
      } else {
        if (!parentPm.getFileStatus().isDirectory()) {
          comparePair.violations.add(Violation.PARENT_IS_A_FILE);
        }
        if (parentPm.isDeleted()) {
          comparePair.violations.add(Violation.PARENT_TOMBSTONED);
        }
      }
    } else {
      LOG.info("Entry is in the root, so there's no parent");
    }

    if (msPathMetadata == null) {
      comparePair.violations.add(Violation.NO_METADATA_ENTRY);
      return comparePair;
    }
    final S3AFileStatus msFileStatus = msPathMetadata.getFileStatus();
    if (s3FileStatus.isDirectory() && !msFileStatus.isDirectory()) {
      comparePair.violations.add(Violation.DIR_IN_S3_FILE_IN_MS);
    }
    if (!s3FileStatus.isDirectory() && msFileStatus.isDirectory()) {
      comparePair.violations.add(Violation.FILE_IN_S3_DIR_IN_MS);
    }

    /**
     * Attribute check
     */
    if (s3FileStatus.getLen() != msFileStatus.getLen()) {
      comparePair.violations.add(Violation.LENGTH_MISMATCH);
    }

    if (s3FileStatus.getModificationTime() !=
        msFileStatus.getModificationTime()) {
      comparePair.violations.add(Violation.MOD_TIME_MISMATCH);
    }

    if(msPathMetadata.getFileStatus().getVersionId() == null) {
      LOG.debug("Missing versionIDs are skipped right now. A HEAD request is "
          + "required for each object to get the versionID.");
    } else if(s3FileStatus.getVersionId() != msFileStatus.getVersionId()) {
      comparePair.violations.add(Violation.VERSIONID_MISMATCH);
    }


    if (msPathMetadata.getFileStatus().getETag() == null) {
      comparePair.violations.add(Violation.NO_ETAG);
    } else if (s3FileStatus.getETag() != null &&
        !s3FileStatus.getETag().equals(msFileStatus.getETag())) {
      comparePair.violations.add(Violation.ETAG_MISMATCH);
    }

    return comparePair;
  }

  private Path path(String s) {
    return rawFS.makeQualified(new Path(s));
  }

  /**
   * A compare pair with the pair of metadata and the list of violations.
   */
  public static class ComparePair {
    private final S3AFileStatus s3FileStatus;
    private final PathMetadata msPathMetadata;

    private final List<FileStatus> s3DirListing;
    private final DirListingMetadata msDirListing;

    private final Path path;

    private Set<Violation> violations = new HashSet<>();

    ComparePair(S3AFileStatus status, PathMetadata pm) {
      this.s3FileStatus = status;
      this.msPathMetadata = pm;
      this.s3DirListing = null;
      this.msDirListing = null;
      this.path = status.getPath();
    }

    ComparePair(FileStatus[] s3DirListing, DirListingMetadata msDirListing) {
      this.s3DirListing = Arrays.asList(s3DirListing);
      this.msDirListing = msDirListing;
      this.s3FileStatus = null;
      this.msPathMetadata = null;
      this.path = msDirListing.getPath();
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

    public DirListingMetadata getMsDirListing() {
      return msDirListing;
    }

    public List<FileStatus> getS3DirListing() {
      return s3DirListing;
    }

    public Path getPath() {
      return path;
    }

    @Override public String toString() {
      return "ComparePair{" + "s3FileStatus=" + s3FileStatus
          + ", msPathMetadata=" + msPathMetadata + ", s3DirListing=" +
          s3DirListing + ", msDirListing=" + msDirListing + ", path="
          + path + ", violations=" + violations + '}';
    }
  }

  /**
   * Violation with severity and the handler.
   * Defines the severity of the violation between 0-2
   * where 0 is the most severe and 2 is the least severe.
   */
  public enum Violation {
    // No entry in metadatastore
    NO_METADATA_ENTRY(1,
        S3GuardFsckViolationHandler.NoMetadataEntry.class),
    // A file or directory entry does not have a parent entry - excluding
    // files and directories in the root.
    NO_PARENT_ENTRY(0,
        S3GuardFsckViolationHandler.NoParentEntry.class),
    // An entry’s parent is a file
    PARENT_IS_A_FILE(0,
        S3GuardFsckViolationHandler.ParentIsAFile.class),
    // A file exists under a path for which there is
    // a tombstone entry in the MS
    PARENT_TOMBSTONED(0,
        S3GuardFsckViolationHandler.ParentTombstoned.class),
    // A directory in S3 is a file entry in the MS
    DIR_IN_S3_FILE_IN_MS(0,
        S3GuardFsckViolationHandler.DirInS3FileInMs.class),
    // A file in S3 is a directory in the MS
    FILE_IN_S3_DIR_IN_MS(0,
        S3GuardFsckViolationHandler.FileInS3DirInMs.class),
    AUTHORITATIVE_DIRECTORY_CONTENT_MISMATCH(1,
        S3GuardFsckViolationHandler.AuthDirContentMismatch.class),
    // Attribute mismatch
    LENGTH_MISMATCH(0,
        S3GuardFsckViolationHandler.LengthMismatch.class),
    MOD_TIME_MISMATCH(2,
        S3GuardFsckViolationHandler.ModTimeMismatch.class),
    // If there's a versionID the mismatch is severe
    VERSIONID_MISMATCH(0,
        S3GuardFsckViolationHandler.VersionIdMismatch.class),
    // If there's an etag the mismatch is severe
    ETAG_MISMATCH(0,
        S3GuardFsckViolationHandler.EtagMismatch.class),
    // Don't worry too much if we don't have an etag
    NO_ETAG(2,
        S3GuardFsckViolationHandler.NoEtag.class);

    private int severity;
    private Class<? extends S3GuardFsckViolationHandler.ViolationHandler> handler;

    Violation(int s,
        Class<? extends S3GuardFsckViolationHandler.ViolationHandler> h) {
      this.severity = s;
      this.handler = h;
    }

    public int getSeverity() {
      return severity;
    }

    public Class<? extends S3GuardFsckViolationHandler.ViolationHandler> getHandler() {
      return handler;
    }
  }
}
