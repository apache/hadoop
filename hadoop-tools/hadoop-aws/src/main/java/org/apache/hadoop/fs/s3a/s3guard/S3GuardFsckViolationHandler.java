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
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 * Violation handler for the S3Guard's fsck.
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

    StringBuilder sB = new StringBuilder();
    sB.append(NEWLINE)
        .append("On path: ").append(comparePair.getPath()).append(NEWLINE);

    // Create a new instance of the handler and use it.
    for (S3GuardFsck.Violation violation : comparePair.getViolations()) {
      try {
        ViolationHandler handler =
            violation.handler.getDeclaredConstructor(S3GuardFsck.ComparePair.class)
            .newInstance(comparePair);
        final String errorStr = handler.getError();
        sB.append(errorStr);
      } catch (NoSuchMethodException e) {
        LOG.error("Can not find declared constructor for handler: {}",
            violation.handler);
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
        LOG.error("Can not instantiate handler: {}",
            violation.handler);
      }
      sB.append(NEWLINE);
    }
    LOG.error(sB.toString());
  }

  public static abstract class ViolationHandler {
    final PathMetadata pathMetadata;
    final S3AFileStatus s3FileStatus;
    final S3AFileStatus msFileStatus;
    final FileStatus[] s3DirListing;
    final DirListingMetadata msDirListing;

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

    abstract String getError();
  }

  public static class NoMetadataEntry extends ViolationHandler {

    public NoMetadataEntry(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "No PathMetadata for this path in the MS.";
    }
  }

  public static class NoParentEntry extends ViolationHandler {

    public NoParentEntry(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "Entry does not have a parent entry (not root)";
    }
  }

  public static class ParentIsAFile extends ViolationHandler {

    public ParentIsAFile(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "An entry’s parent is a file";
    }
  }

  public static class ParentTombstoned extends ViolationHandler {

    public ParentTombstoned(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "The entry's parent tombstoned";
    }
  }

  public static class DirInS3FileInMs extends ViolationHandler {

    public DirInS3FileInMs(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "A directory in S3 is a file entry in the MS";
    }
  }

  public static class FileInS3DirInMs extends ViolationHandler {

    public FileInS3DirInMs(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "A file in S3 is a directory entry in the MS";
    }
  }

  public static class AuthDirContentMismatch extends ViolationHandler {

    public AuthDirContentMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      final String str = String.format(
          "The content of an authoritative directory listing does "
              + "not match the content of the S3 listing. S3: %s, MS: %s",
          Arrays.asList(s3DirListing), msDirListing.getListing());
      return str;
    }
  }

  public static class LengthMismatch extends ViolationHandler {

    public LengthMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override public String getError() {
      return String.format("getLen mismatch - s3: %s, ms: %s",
          s3FileStatus.getLen(), msFileStatus.getLen());
    }
  }

  public static class ModTimeMismatch extends ViolationHandler {

    public ModTimeMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return String.format("getModificationTime mismatch - s3: %s, ms: %s",
          s3FileStatus.getModificationTime(),
          msFileStatus.getModificationTime());
    }
  }

  public static class VersionIdMismatch extends ViolationHandler {

    public VersionIdMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return String.format("getVersionId mismatch - s3: %s, ms: %s",
          s3FileStatus.getVersionId(), msFileStatus.getVersionId());
    }
  }

  public static class EtagMismatch extends ViolationHandler {

    public EtagMismatch(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return String.format("getETag mismatch - s3: %s, ms: %s",
        s3FileStatus.getETag(), msFileStatus.getETag());
    }
  }

  public static class NoEtag extends ViolationHandler {

    public NoEtag(S3GuardFsck.ComparePair comparePair) {
      super(comparePair);
    }

    @Override
    public String getError() {
      return "No etag.";
    }
  }
}
