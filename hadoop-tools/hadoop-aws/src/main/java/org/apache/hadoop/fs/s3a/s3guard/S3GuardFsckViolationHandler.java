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

import org.apache.commons.math3.ode.UnknownParameterException;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Violation handler for the S3Guard's fsck
 * 
 */
public class S3GuardFsckViolationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      S3GuardFsckViolationHandler.class);

  private S3AFileSystem rawFs;
  private DynamoDBMetadataStore metadataStore;
  private static String NEWLINE = System.getProperty("line.separator");

  public S3GuardFsckViolationHandler(S3AFileSystem fs,
      DynamoDBMetadataStore ddbms) {

    this.metadataStore = ddbms;
    this.rawFs = fs;
  }

  public void handle(S3GuardFsck.ComparePair comparePair) {
    if (!comparePair.containsViolation()) {
      LOG.debug("There is no violation in the compare pair: " + toString());
      return;
    }

    ViolationHandler handler;

    StringBuilder sB = new StringBuilder(
        String.format("%sOn path: %s%n", NEWLINE,
            comparePair.getS3FileStatus().getPath())
    );

    for (S3GuardFsck.Violation violation : comparePair.getViolations()) {
      switch (violation) {
      case NO_METADATA_ENTRY:
        handler = new NoMetadataEntryViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case NO_PARENT_ENTRY:
        handler = new NoParentEntryViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case PARENT_IS_A_FILE:
        handler = new ParentIsAFileViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case PARENT_TOMBSTONED:
        handler = new ParentTombstonedViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case DIR_IN_S3_FILE_IN_MS:
        handler = new DirInS3FileInMsViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case LENGTH_MISMATCH:
        handler = new LengthMismatchViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case MOD_TIME_MISMATCH:
        handler = new ModTimeMismatchViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case BLOCKSIZE_MISMATCH:
        handler = new BlockSizeMismatchViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case OWNER_MISMATCH:
        handler = new OwnerMismatchViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case VERSIONID_MISMATCH:
        handler = new VersionIdMismatchViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case ETAG_MISMATCH:
        handler = new EtagMismatchViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case NO_ETAG:
        handler = new NoEtagViolation(comparePair);
        sB.append(handler.getError());;
        break;
      case NO_VERSIONID:
        handler = new NoVersionIdViolation(comparePair);
        sB.append(handler.getError());;
        break;
      default:
        LOG.error("UNKNOWN VIOLATION: {}", violation.toString());
        throw new UnknownParameterException("Unknown Violation: " +
            violation.toString());
      }
      sB.append(NEWLINE);
    }

    LOG.error(sB.toString());
  }

  public static abstract class ViolationHandler {
    final PathMetadata pathMetadata;
    final S3AFileStatus s3FileStatus;
    final S3AFileStatus msFileStatus;

    public ViolationHandler(S3GuardFsck.ComparePair comparePair) {
      pathMetadata = comparePair.getMsPathMetadata();
      s3FileStatus = comparePair.getS3FileStatus();
      if (pathMetadata != null) {
        msFileStatus = pathMetadata.getFileStatus();
      } else {
        msFileStatus = null;
      }
    }

    abstract String getError();
  }

  public static class NoMetadataEntryViolation extends ViolationHandler {

    public NoMetadataEntryViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "No PathMetadata for this path in the MS.";
    }
  }

  public static class NoParentEntryViolation extends ViolationHandler {

    public NoParentEntryViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "Entry does not have a parent entry (not root)";
    }
  }

  public static class ParentIsAFileViolation extends ViolationHandler {

    public ParentIsAFileViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "An entry’s parent is a file";
    }
  }

  public static class ParentTombstonedViolation extends ViolationHandler {

    public ParentTombstonedViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "The entry's parent tombstoned";
    }
  }

  public static class DirInS3FileInMsViolation extends ViolationHandler {

    public DirInS3FileInMsViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "A directory in S3 is a file entry in the MS";
    }
  }

  public static class LengthMismatchViolation extends ViolationHandler {

    public LengthMismatchViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return String.format("getLen mismatch - s3: %s, ms: %s",
          s3FileStatus.getLen(), msFileStatus.getLen());
    }
  }

  public static class ModTimeMismatchViolation extends ViolationHandler {

    public ModTimeMismatchViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return String.format("getModificationTime mismatch - s3: %s, ms: %s",
          s3FileStatus.getModificationTime(),
          msFileStatus.getModificationTime());
    }
  }

  public static class BlockSizeMismatchViolation extends ViolationHandler {

    public BlockSizeMismatchViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return String.format("getBlockSize mismatch - s3: %s, ms: %s",
          s3FileStatus.getBlockSize(), msFileStatus.getBlockSize());
    }
  }

  public static class OwnerMismatchViolation extends ViolationHandler {

    public OwnerMismatchViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return String.format("getOwner mismatch - s3: %s, ms: %s",
          s3FileStatus.getOwner(), msFileStatus.getOwner());
    }
  }

  public static class VersionIdMismatchViolation extends ViolationHandler {

    public VersionIdMismatchViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return String.format("getVersionId mismatch - s3: %s, ms: %s",
          s3FileStatus.getVersionId(), msFileStatus.getVersionId());
    }
  }

  public static class EtagMismatchViolation extends ViolationHandler {

    public EtagMismatchViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return String.format("getETag mismatch - s3: %s, ms: %s",
        s3FileStatus.getETag(), msFileStatus.getETag());
    }
  }

  public static class NoEtagViolation extends ViolationHandler {

    public NoEtagViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return "No etag.";
    }
  }

  public static class NoVersionIdViolation extends ViolationHandler {

    public NoVersionIdViolation(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return "No versionid.";
    }
  }

}
