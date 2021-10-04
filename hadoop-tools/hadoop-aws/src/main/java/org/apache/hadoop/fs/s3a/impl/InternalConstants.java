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

package org.apache.hadoop.fs.s3a.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.Constants;

/**
 * Internal constants private only to the S3A codebase.
 * Please don't refer to these outside of this module &amp; its tests.
 * If you find you need to then either the code is doing something it
 * should not, or these constants need to be uprated to being
 * public and stable entries.
 */
public final class InternalConstants {

  /**
   * This declared delete as idempotent.
   * This is an "interesting" topic in past Hadoop FS work.
   * Essentially: with a single caller, DELETE is idempotent
   * but in a shared filesystem, it is is very much not so.
   * Here, on the basis that isn't a filesystem with consistency guarantees,
   * retryable results in files being deleted.
  */
  public static final boolean DELETE_CONSIDERED_IDEMPOTENT = true;

  private InternalConstants() {
  }

  /**
   * This is an arbitrary value: {@value}.
   * It declares how many parallel copy operations
   * in a single rename can be queued before the operation pauses
   * and awaits completion.
   * A very large value wouldn't just starve other threads from
   * performing work, there's a risk that the S3 store itself would
   * throttle operations (which all go to the same shard).
   * It is not currently configurable just to avoid people choosing values
   * which work on a microbenchmark (single rename, no other work, ...)
   * but don't scale well to execution in a large process against a common
   * store, all while separate processes are working with the same shard
   * of storage.
   *
   * It should be a factor of {@link #MAX_ENTRIES_TO_DELETE} so that
   * all copies will have finished before deletion is contemplated.
   * (There's always a block for that, it just makes more sense to
   * perform the bulk delete after another block of copies have completed).
   */
  public static final int RENAME_PARALLEL_LIMIT = 10;

  /**
   * The maximum number of entries that can be deleted in any bulk delete
   * call to S3: {@value}.
   */
  public static final int MAX_ENTRIES_TO_DELETE = 1000;

  /**
   * Default blocksize as used in blocksize and FS status queries: {@value}.
   */
  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;


  /**
   * The known keys used in a standard openFile call.
   * if there's a select marker in there then the keyset
   * used becomes that of the select operation.
   */
  @InterfaceStability.Unstable
  public static final Set<String> STANDARD_OPENFILE_KEYS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(Constants.INPUT_FADVISE,
                  Constants.READAHEAD_RANGE)));

  /** 403 error code. */
  public static final int SC_403 = 403;

  /** 404 error code. */
  public static final int SC_404 = 404;

  /** Name of the log for throttling events. Value: {@value}. */
  public static final String THROTTLE_LOG_NAME =
      "org.apache.hadoop.fs.s3a.throttled";

  /** Directory marker attribute: see HADOOP-16613. Value: {@value}. */
  public static final String X_DIRECTORY =
      "application/x-directory";

  /**
   * A configuration option for test use only: maximum
   * part count on block writes/uploads.
   * Value: {@value}.
   */
  @VisibleForTesting
  public static final String UPLOAD_PART_COUNT_LIMIT =
          "fs.s3a.internal.upload.part.count.limit";

  /**
   * Maximum entries you can upload in a single file write/copy/upload.
   * Value: {@value}.
   */
  public static final int DEFAULT_UPLOAD_PART_COUNT_LIMIT = 10000;

  /**
   * The system property used by the AWS SDK to identify the region.
   */
  public static final String AWS_REGION_SYSPROP = "aws.region";

  /**
   * S3 client side encryption adds padding to the content length of constant
   * length of 16 bytes (at the moment, since we have only 1 content
   * encryption algorithm). Use this to subtract while listing the content
   * length when certain conditions are met.
   */
  public static final int CSE_PADDING_LENGTH = 16;

  /**
   * Error message to indicate S3-CSE is incompatible with S3Guard.
   */
  public static final String CSE_S3GUARD_INCOMPATIBLE = "S3-CSE cannot be "
      + "used with S3Guard";

  /**
   * Error message to indicate Access Points are incompatible with S3Guard.
   */
  public static final String AP_S3GUARD_INCOMPATIBLE = "Access Points cannot be used with S3Guard";

  /**
   * Error message to indicate Access Points are required to be used for S3 access.
   */
  public static final String AP_REQUIRED_EXCEPTION = "Access Points usage is required" +
      " but not configured for the bucket.";

  /**
   * Error message to indicate Access Points are not accessible or don't exist.
   */
  public static final String AP_INACCESSIBLE = "Could not access through this access point";

  /**
   * AccessPoint ARN for the bucket. When set as a bucket override the requests for that bucket
   * will go through the AccessPoint.
   */
  public static final String ARN_BUCKET_OPTION = "fs.s3a.bucket.%s.accesspoint.arn";
}
