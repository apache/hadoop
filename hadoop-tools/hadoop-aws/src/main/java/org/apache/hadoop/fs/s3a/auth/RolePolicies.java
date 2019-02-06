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

package org.apache.hadoop.fs.s3a.auth;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.s3a.auth.RoleModel.*;

/**
 * Operations, statements and policies covering the operations
 * needed to work with S3 and S3Guard.
 */
@InterfaceAudience.LimitedPrivate("Tests")
@InterfaceStability.Unstable
public final class RolePolicies {

  private RolePolicies() {
  }

  /** All KMS operations: {@value}.*/
  public static final String KMS_ALL_OPERATIONS = "kms:*";

  /** KMS encryption. This is <i>Not</i> used by SSE-KMS: {@value}. */
  public static final String KMS_ENCRYPT = "kms:Encrypt";

  /**
   * Decrypt data encrypted with SSE-KMS: {@value}.
   */
  public static final String KMS_DECRYPT = "kms:Decrypt";

  /**
   * Arn for all KMS keys: {@value}.
   */
  public static final String KMS_ALL_KEYS = "arn:aws:kms:*";

  /**
   * This is used by S3 to generate a per-object encryption key and
   * the encrypted value of this, the latter being what it tags
   * the object with for later decryption: {@value}.
   */
  public static final String KMS_GENERATE_DATA_KEY = "kms:GenerateDataKey";

  /**
   * Actions needed to read and write SSE-KMS data.
   */
  private static final String[] KMS_KEY_RW =
      new String[]{KMS_DECRYPT, KMS_GENERATE_DATA_KEY};

  /**
   * Actions needed to read SSE-KMS data.
   */
  private static final String[] KMS_KEY_READ =
      new String[] {KMS_DECRYPT};

  /**
   * Statement to allow KMS R/W access access, so full use of
   * SSE-KMS.
   */
  public static final Statement STATEMENT_ALLOW_SSE_KMS_RW =
      statement(true, KMS_ALL_KEYS, KMS_KEY_RW);

  /**
   * Statement to allow read access to KMS keys, so the ability
   * to read SSE-KMS data,, but not decrypt it.
   */
  public static final Statement STATEMENT_ALLOW_SSE_KMS_READ =
      statement(true, KMS_ALL_KEYS, KMS_KEY_READ);

  /**
   * All S3 operations: {@value}.
   */
  public static final String S3_ALL_OPERATIONS = "s3:*";

  /**
   * All S3 buckets: {@value}.
   */
  public static final String S3_ALL_BUCKETS = "arn:aws:s3:::*";

  /**
   * All bucket list operations, including
   * {@link #S3_BUCKET_LIST_BUCKET} and
   * {@link #S3_BUCKET_LIST_MULTIPART_UPLOADS}.
   */
  public static final String S3_BUCKET_ALL_LIST = "s3:ListBucket*";

  /**
   * List the contents of a bucket.
   * It applies to a bucket, not to a path in a bucket.
   */
  public static final String S3_BUCKET_LIST_BUCKET = "s3:ListBucket";

  /**
   * This is used by the abort operation in S3A commit work.
   * It applies to a bucket, not to a path in a bucket.
   */
  public static final String S3_BUCKET_LIST_MULTIPART_UPLOADS =
      "s3:ListBucketMultipartUploads";

  /**
   * List multipart upload is needed for the S3A Commit protocols.
   * It applies to a path in a bucket.
   */
  public static final String S3_LIST_MULTIPART_UPLOAD_PARTS
      = "s3:ListMultipartUploadParts";

  /**
   * Abort multipart upload is needed for the S3A Commit protocols.
   * It applies to a path in a bucket.
   */
  public static final String S3_ABORT_MULTIPART_UPLOAD
      = "s3:AbortMultipartUpload";

  /**
   * All s3:Delete* operations.
   */
  public static final String S3_ALL_DELETE = "s3:Delete*";


  public static final String S3_DELETE_OBJECT = "s3:DeleteObject";

  public static final String S3_DELETE_OBJECT_TAGGING
      = "s3:DeleteObjectTagging";

  public static final String S3_DELETE_OBJECT_VERSION
      = "s3:DeleteObjectVersion";

  public static final String S3_DELETE_OBJECT_VERSION_TAGGING
      = "s3:DeleteObjectVersionTagging";

  /**
   * All s3:Get* operations.
   */
  public static final String S3_ALL_GET = "s3:Get*";

  public static final String S3_GET_OBJECT = "s3:GetObject";

  public static final String S3_GET_OBJECT_ACL = "s3:GetObjectAcl";

  public static final String S3_GET_OBJECT_TAGGING = "s3:GetObjectTagging";

  public static final String S3_GET_OBJECT_TORRENT = "s3:GetObjectTorrent";

  public static final String S3_GET_OBJECT_VERSION = "s3:GetObjectVersion";

  public static final String S3_GET_BUCKET_LOCATION = "s3:GetBucketLocation";

  public static final String S3_GET_OBJECT_VERSION_ACL
      = "s3:GetObjectVersionAcl";

  public static final String S3_GET_OBJECT_VERSION_TAGGING
      = "s3:GetObjectVersionTagging";

  public static final String S3_GET_OBJECT_VERSION_TORRENT
      = "s3:GetObjectVersionTorrent";


  /**
   * S3 Put*.
   * This covers single an multipart uploads, but not list/abort of the latter.
   */
  public static final String S3_ALL_PUT = "s3:Put*";

  public static final String S3_PUT_OBJECT = "s3:PutObject";

  public static final String S3_PUT_OBJECT_ACL = "s3:PutObjectAcl";

  public static final String S3_PUT_OBJECT_TAGGING = "s3:PutObjectTagging";

  public static final String S3_PUT_OBJECT_VERSION_ACL
      = "s3:PutObjectVersionAcl";

  public static final String S3_PUT_OBJECT_VERSION_TAGGING
      = "s3:PutObjectVersionTagging";

  public static final String S3_RESTORE_OBJECT = "s3:RestoreObject";

  /**
   * Actions needed to read a file in S3 through S3A, excluding
   * S3Guard and SSE-KMS.
   */
  private static final String[] S3_PATH_READ_OPERATIONS =
      new String[]{
          S3_GET_OBJECT,
      };

  /**
   * Base actions needed to read data from S3 through S3A,
   * excluding:
   * <ol>
   *   <li>bucket-level operations</li>
   *   <li>SSE-KMS key operations</li>
   *   <li>DynamoDB operations for S3Guard.</li>
   * </ol>
   * As this excludes the bucket list operations, it is not sufficient
   * to read from a bucket on its own.
   */
  private static final String[] S3_ROOT_READ_OPERATIONS =
      new String[]{
          S3_ALL_GET,
      };

  public static final List<String> S3_ROOT_READ_OPERATIONS_LIST =
      Collections.unmodifiableList(Arrays.asList(S3_ALL_GET));

  /**
   * Policies which can be applied to bucket resources for read operations.
   * <ol>
   *   <li>SSE-KMS key operations</li>
   *   <li>DynamoDB operations for S3Guard.</li>
   * </ol>
   */
  public static final String[] S3_BUCKET_READ_OPERATIONS =
      new String[]{
          S3_ALL_GET,
          S3_BUCKET_ALL_LIST,
      };

  /**
   * Actions needed to write data to an S3A Path.
   * This includes the appropriate read operations, but
   * not SSE-KMS or S3Guard support.
   */
  public static final List<String> S3_PATH_RW_OPERATIONS =
      Collections.unmodifiableList(Arrays.asList(new String[]{
          S3_ALL_GET,
          S3_PUT_OBJECT,
          S3_DELETE_OBJECT,
          S3_ABORT_MULTIPART_UPLOAD,
      }));

  /**
   * Actions needed to write data to an S3A Path.
   * This is purely the extra operations needed for writing atop
   * of the read operation set.
   * Deny these and a path is still readable, but not writeable.
   * Excludes: bucket-ARN, SSE-KMS and S3Guard permissions.
   */
  public static final List<String> S3_PATH_WRITE_OPERATIONS =
      Collections.unmodifiableList(Arrays.asList(new String[]{
          S3_PUT_OBJECT,
          S3_DELETE_OBJECT,
          S3_ABORT_MULTIPART_UPLOAD
      }));

  /**
   * Actions needed for R/W IO from the root of a bucket.
   * Excludes: bucket-ARN, SSE-KMS and S3Guard permissions.
   */
  public static final List<String> S3_ROOT_RW_OPERATIONS =
      Collections.unmodifiableList(Arrays.asList(new String[]{
          S3_ALL_GET,
          S3_PUT_OBJECT,
          S3_DELETE_OBJECT,
          S3_ABORT_MULTIPART_UPLOAD,
      }));

  /**
   * All DynamoDB operations: {@value}.
   */
  public static final String DDB_ALL_OPERATIONS = "dynamodb:*";

  /**
   * Operations needed for DDB/S3Guard Admin.
   * For now: make this {@link #DDB_ALL_OPERATIONS}.
   */
  public static final String DDB_ADMIN = DDB_ALL_OPERATIONS;

  /**
   * Permission for DDB describeTable() operation: {@value}.
   * This is used during initialization.
   */
  public static final String DDB_DESCRIBE_TABLE = "dynamodb:DescribeTable";

  /**
   * Permission to query the DDB table: {@value}.
   */
  public static final String DDB_QUERY = "dynamodb:Query";

  /**
   * Permission for DDB operation to get a record: {@value}.
   */
  public static final String DDB_GET_ITEM = "dynamodb:GetItem";

  /**
   * Permission for DDB write record operation: {@value}.
   */
  public static final String DDB_PUT_ITEM = "dynamodb:PutItem";

  /**
   * Permission for DDB update single item operation: {@value}.
   */
  public static final String DDB_UPDATE_ITEM = "dynamodb:UpdateItem";

  /**
   * Permission for DDB delete operation: {@value}.
   */
  public static final String DDB_DELETE_ITEM = "dynamodb:DeleteItem";

  /**
   * Permission for DDB operation: {@value}.
   */
  public static final String DDB_BATCH_GET_ITEM = "dynamodb:BatchGetItem";

  /**
   * Batch write permission for DDB: {@value}.
   */
  public static final String DDB_BATCH_WRITE_ITEM = "dynamodb:BatchWriteItem";

  /**
   * All DynamoDB tables: {@value}.
   */
  public static final String ALL_DDB_TABLES = "arn:aws:dynamodb:*";

  /**
   * Statement to allow all DDB access.
   */
  public static final Statement STATEMENT_ALL_DDB =
      allowAllDynamoDBOperations(ALL_DDB_TABLES);

  /**
   * Statement to allow all client operations needed for S3Guard,
   * but none of the admin operations.
   */
  public static final Statement STATEMENT_S3GUARD_CLIENT =
      allowS3GuardClientOperations(ALL_DDB_TABLES);

  /**
   * Allow all S3 Operations.
   * This does not cover DDB or S3-KMS
   */
  public static final Statement STATEMENT_ALL_S3 = statement(true,
      S3_ALL_BUCKETS,
      S3_ALL_OPERATIONS);

  /**
   * The s3:GetBucketLocation permission is for all buckets, not for
   * any named bucket, which complicates permissions.
   */
  public static final Statement STATEMENT_ALL_S3_GET_BUCKET_LOCATION =
      statement(true,
          S3_ALL_BUCKETS,
          S3_GET_BUCKET_LOCATION);

  /**
   * Policy for all S3 and S3Guard operations, and SSE-KMS.
   */
  public static final Policy ALLOW_S3_AND_SGUARD = policy(
      STATEMENT_ALL_S3,
      STATEMENT_ALL_DDB,
      STATEMENT_ALLOW_SSE_KMS_RW,
      STATEMENT_ALL_S3_GET_BUCKET_LOCATION
  );

  public static Statement allowS3GuardClientOperations(String tableArn) {
    return statement(true,
        tableArn,
        DDB_BATCH_GET_ITEM,
        DDB_BATCH_WRITE_ITEM,
        DDB_DELETE_ITEM,
        DDB_DESCRIBE_TABLE,
        DDB_GET_ITEM,
        DDB_PUT_ITEM,
        DDB_QUERY,
        DDB_UPDATE_ITEM
    );
  }

  public static Statement allowAllDynamoDBOperations(String tableArn) {
    return statement(true,
        tableArn,
        DDB_ALL_OPERATIONS);
  }

  /**
   * From an S3 bucket name, build an ARN to refer to it.
   * @param bucket bucket name.
   * @param write are write permissions required
   * @return return statement granting access.
   */
  public static List<Statement> allowS3Operations(String bucket,
      boolean write) {
    // add the bucket operations for the specific bucket ARN
    ArrayList<Statement> statements =
        Lists.newArrayList(
            statement(true,
                bucketToArn(bucket),
                S3_GET_BUCKET_LOCATION, S3_BUCKET_ALL_LIST));
    // then add the statements for objects in the buckets
    if (write) {
      statements.add(
          statement(true,
              bucketObjectsToArn(bucket),
              S3_ROOT_RW_OPERATIONS));
    } else {
      statements.add(
          statement(true,
              bucketObjectsToArn(bucket),
              S3_ROOT_READ_OPERATIONS_LIST));
    }
    return statements;
  }

  /**
   * From an S3 bucket name, build an ARN to refer to all objects in
   * it.
   * @param bucket bucket name.
   * @return return the ARN to use in statements.
   */
  public static String bucketObjectsToArn(String bucket) {
    return String.format("arn:aws:s3:::%s/*", bucket);
  }


  /**
   * From an S3 bucket name, build an ARN to refer to it.
   * @param bucket bucket name.
   * @return return the ARN to use in statements.
   */
  public static String bucketToArn(String bucket) {
    return String.format("arn:aws:s3:::%s", bucket);
  }

}
