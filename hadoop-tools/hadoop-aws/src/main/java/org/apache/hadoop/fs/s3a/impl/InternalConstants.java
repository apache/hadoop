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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.Constants;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_STANDARD_OPTIONS;

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

  /**
   * size of a buffer to create when draining the stream.
   */
  public static final int DRAIN_BUFFER_SIZE = 16384;

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
  public static final Set<String> S3A_OPENFILE_KEYS;

  static {
    Set<String> keys = Stream.of(
        Constants.ASYNC_DRAIN_THRESHOLD,
        Constants.INPUT_FADVISE,
        Constants.READAHEAD_RANGE)
        .collect(Collectors.toSet());
    keys.addAll(FS_OPTION_OPENFILE_STANDARD_OPTIONS);
    S3A_OPENFILE_KEYS = Collections.unmodifiableSet(keys);
  }

  /** 200 status code: OK. */
  public static final int SC_200_OK = 200;

  /** 301 status code: Moved Permanently. */
  public static final int SC_301_MOVED_PERMANENTLY = 301;

  /** 307 status code: Temporary Redirect. */
  public static final int SC_307_TEMPORARY_REDIRECT = 307;

  /** 400 status code: Bad Request. */
  public static final int SC_400_BAD_REQUEST = 400;

  /** 401 status code: Unauthorized. */
  public static final int SC_401_UNAUTHORIZED = 401;

  /** 403 status code: Forbidden. */
  public static final int SC_403_FORBIDDEN = 403;

  /** 403 error code. */
  @Deprecated
  public static final int SC_403 = SC_403_FORBIDDEN;

  /** 404 status code: Not Found. */
  public static final int SC_404_NOT_FOUND = 404;

  /** 404 error code. */
  @Deprecated
  public static final int SC_404 = SC_404_NOT_FOUND;

  /** 405 status code: Method Not Allowed. */
  public static final int SC_405_METHOD_NOT_ALLOWED = 405;

  /** 410 status code: Gone. */
  public static final int SC_410_GONE = 410;

  /** 412 status code: Precondition Failed. */
  public static final int SC_412_PRECONDITION_FAILED = 412;

  /** 415 status code: Content type unsupported by this store. */
  public static final int SC_415_UNSUPPORTED_MEDIA_TYPE = 415;

  /** 416 status code: Range Not Satisfiable. */
  public static final int SC_416_RANGE_NOT_SATISFIABLE = 416;

  /** 429 status code: This is the google GCS throttle message. */
  public static final int SC_429_TOO_MANY_REQUESTS_GCS = 429;

  /** 443 status code: No Response (unofficial). */
  public static final int SC_443_NO_RESPONSE = 443;

  /** 444 status code: No Response (unofficial). */
  public static final int SC_444_NO_RESPONSE = 444;

  /** 500 status code: Internal Server Error. */
  public static final int SC_500_INTERNAL_SERVER_ERROR = 500;

  /** 501 status code: method not implemented. */
  public static final int SC_501_NOT_IMPLEMENTED = 501;

  /** 503 status code: Service Unavailable. on AWS S3: throttle response. */
  public static final int SC_503_SERVICE_UNAVAILABLE = 503;

  /** Name of the log for throttling events. Value: {@value}. */
  public static final String THROTTLE_LOG_NAME =
      "org.apache.hadoop.fs.s3a.throttled";

  /**
   * Name of the log for events related to the SDK V2 upgrade.
   */
  public static final String SDK_V2_UPGRADE_LOG_NAME =
      "org.apache.hadoop.fs.s3a.SDKV2Upgrade";

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

  /**
   * The known keys used in a createFile call.
   */
  public static final Set<String> CREATE_FILE_KEYS =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(Constants.FS_S3A_CREATE_PERFORMANCE)));

}
