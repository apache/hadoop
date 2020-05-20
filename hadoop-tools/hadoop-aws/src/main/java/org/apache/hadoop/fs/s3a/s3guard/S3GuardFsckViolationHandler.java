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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

/**
 * Violation handler for the S3Guard's fsck.
 */
public class S3GuardFsckViolationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      S3GuardFsckViolationHandler.class);

  // The rawFS and metadataStore are here to prepare when the ViolationHandlers
  // will not just log, but fix the violations, so they will have access.
  private final S3AFileSystem rawFs;
  private final DynamoDBMetadataStore metadataStore;

  private static String newLine = System.getProperty("line.separator");

  public enum HandleMode {
    FIX, LOG
  }

  public S3GuardFsckViolationHandler(S3AFileSystem fs,
      DynamoDBMetadataStore ddbms) {

    this.metadataStore = ddbms;
    this.rawFs = fs;
  }

  public void logError(S3GuardFsck.ComparePair comparePair) throws IOException {
    if (!comparePair.containsViolation()) {
      LOG.debug("There is no violation in the compare pair: {}", comparePair);
      return;
    }

    StringBuilder sB = new StringBuilder();
    sB.append(newLine)
        .append("On path: ").append(comparePair.getPath()).append(newLine);

    handleComparePair(comparePair, sB, HandleMode.LOG);

    LOG.error(sB.toString());
  }

  public void doFix(S3GuardFsck.ComparePair comparePair) throws IOException {
    if (!comparePair.containsViolation()) {
      LOG.debug("There is no violation in the compare pair: {}", comparePair);
      return;
    }

    StringBuilder sB = new StringBuilder();
    sB.append(newLine)
        .append("On path: ").append(comparePair.getPath()).append(newLine);

    handleComparePair(comparePair, sB, HandleMode.FIX);

    LOG.info(sB.toString());
  }

  /**
   * Create a new instance of the violation handler for all the violations
   * found in the compare pair and use it.
   *
   * @param comparePair the compare pair with violations
   * @param sB StringBuilder to append error strings from violations.
   */
  protected void handleComparePair(S3GuardFsck.ComparePair comparePair,
      StringBuilder sB, HandleMode handleMode) throws IOException {

    for (S3GuardFsck.Violation violation : comparePair.getViolations()) {
      try {
        ViolationHandler handler = violation.getHandler()
            .getDeclaredConstructor(S3GuardFsck.ComparePair.class)
            .newInstance(comparePair);

        switch (handleMode) {
          case FIX:
            final String errorStr = handler.getError();
            sB.append(errorStr);
            break;
          case LOG:
            final String fixStr = handler.fixViolation(rawFs, metadataStore);
            sB.append(fixStr);
            break;
          default:
            throw new UnsupportedOperationException("Unknown handleMode: " + handleMode);
        }

      } catch (NoSuchMethodException e) {
        LOG.error("Can not find declared constructor for handler: {}",
            violation.getHandler());
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
        LOG.error("Can not instantiate handler: {}",
            violation.getHandler());
      }
      sB.append(newLine);
    }
  }

  /**
   * Violation handler abstract class.
   * This class should be extended for violation handlers.
   */
  public static abstract class ViolationHandler {
    private final PathMetadata pathMetadata;
    private final S3AFileStatus s3FileStatus;
    private final S3AFileStatus msFileStatus;
    private final List<FileStatus> s3DirListing;
    private final DirListingMetadata msDirListing;

    public ViolationHandler(S3GuardFsck.ComparePair comparePair) {
      pathMetadata = comparePair.getMsPathMetadata();
      s3FileStatus = comparePair.getS3FileStatus();
      if (pathMetadata != null) {
        msFileStatus = pathMetadata.getFileStatus();
      } else {
        msFileStatus = null;
      }
      s3DirListing = comparePair.getS3DirListing();
      msDirListing = comparePair.getMsDirListing();
    }

    public abstract String getError();

    public PathMetadata getPathMetadata() {
      return pathMetadata;
    }

    public S3AFileStatus getS3FileStatus() {
      return s3FileStatus;
    }

    public S3AFileStatus getMsFileStatus() {
      return msFileStatus;
    }

    public List<FileStatus> getS3DirListing() {
      return s3DirListing;
    }

    public DirListingMetadata getMsDirListing() {
      return msDirListing;
    }

    public String fixViolation(S3AFileSystem fs,
        DynamoDBMetadataStore ddbms) throws IOException {
      return String.format("Fixing of violation: %s is not supported yet.",
          this.getClass().getSimpleName());
    }
  }

  /**
   * The violation handler when there's no matching metadata entry in the MS.
   */
  public static class NoMetadataEntry extends ViolationHandler {

    public NoMetadataEntry(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "No PathMetadata for this path in the MS.";
    }
  }

  /**
   * The violation handler when there's no parent entry.
   */
  public static class NoParentEntry extends ViolationHandler {

    public NoParentEntry(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "Entry does not have a parent entry (not root)";
    }
  }

  /**
   * The violation handler when the parent of an entry is a file.
   */
  public static class ParentIsAFile extends ViolationHandler {

    public ParentIsAFile(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "The entry's parent in the metastore database is a file.";
    }
  }

  /**
   * The violation handler when the parent of an entry is tombstoned.
   */
  public static class ParentTombstoned extends ViolationHandler {

    public ParentTombstoned(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "The entry in the metastore database has a parent entry " +
          "which is a tombstone marker";
    }
  }

  /**
   * The violation handler when there's a directory is a file metadata in MS.
   */
  public static class DirInS3FileInMs extends ViolationHandler {

    public DirInS3FileInMs(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "A directory in S3 is a file entry in the MS";
    }
  }

  /**
   * The violation handler when a file metadata is a directory in MS.
   */
  public static class FileInS3DirInMs extends ViolationHandler {

    public FileInS3DirInMs(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "A file in S3 is a directory entry in the MS";
    }
  }

  /**
   * The violation handler when there's a directory listing content mismatch.
   */
  public static class AuthDirContentMismatch extends ViolationHandler {

    public AuthDirContentMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      final String str = String.format(
          "The content of an authoritative directory listing does "
              + "not match the content of the S3 listing. S3: %s, MS: %s",
          Arrays.asList(getS3DirListing()), getMsDirListing().getListing());
      return str;
    }
  }

  /**
   * The violation handler when there's a length mismatch.
   */
  public static class LengthMismatch extends ViolationHandler {

    public LengthMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return String.format("File length mismatch - S3: %s, MS: %s",
          getS3FileStatus().getLen(), getMsFileStatus().getLen());
    }
  }

  /**
   * The violation handler when there's a modtime mismatch.
   */
  public static class ModTimeMismatch extends ViolationHandler {

    public ModTimeMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return String.format("File timestamp mismatch - S3: %s, MS: %s",
          getS3FileStatus().getModificationTime(),
          getMsFileStatus().getModificationTime());
    }
  }

  /**
   * The violation handler when there's a version id mismatch.
   */
  public static class VersionIdMismatch extends ViolationHandler {

    public VersionIdMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return String.format("getVersionId mismatch - S3: %s, MS: %s",
          getS3FileStatus().getVersionId(), getMsFileStatus().getVersionId());
    }
  }

  /**
   * The violation handler when there's an etag mismatch.
   */
  public static class EtagMismatch extends ViolationHandler {

    public EtagMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return String.format("Etag mismatch - S3: %s, MS: %s",
        getS3FileStatus().getETag(), getMsFileStatus().getETag());
    }
  }

  /**
   * The violation handler when there's no etag.
   */
  public static class NoEtag extends ViolationHandler {

    public NoEtag(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "No etag.";
    }
  }

  /**
   * The violation handler when there's a tombstoned entry in the ms is
   * present, but the object is not deleted in S3.
   */
  public static class TombstonedInMsNotDeletedInS3 extends ViolationHandler {

    public TombstonedInMsNotDeletedInS3(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "The entry for the path is tombstoned in the MS.";
    }
  }

  /**
   * The violation handler there's no parent in the MetadataStore.
   */
  public static class OrphanDDBEntry extends ViolationHandler {

    public OrphanDDBEntry(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "The DDB entry is orphan - there is no parent in the MS.";
    }

    @Override
    public String fixViolation(S3AFileSystem fs, DynamoDBMetadataStore ddbms)
        throws IOException {
      final Path path = getPathMetadata().getFileStatus().getPath();
      ddbms.forgetMetadata(path);
      return String.format(
          "Fixing violation by removing metadata entry from the " +
              "MS on path: %s", path);
    }
  }

  /**
   * The violation handler when there's no last updated field for the entry.
   */
  public static class NoLastUpdatedField extends ViolationHandler {

    public NoLastUpdatedField(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "No lastUpdated field provided for the entry.";
    }
  }
}
