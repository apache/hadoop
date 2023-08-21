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

/**
 * Common S3 HTTP header values used throughout the Amazon Web Services S3 Java client.
 */
public interface AWSHeaders {

  /*
   * Standard HTTP Headers.
   */

  String CACHE_CONTROL = "Cache-Control";
  String CONTENT_DISPOSITION = "Content-Disposition";
  String CONTENT_ENCODING = "Content-Encoding";
  String CONTENT_LENGTH = "Content-Length";
  String CONTENT_RANGE = "Content-Range";
  String CONTENT_MD5 = "Content-MD5";
  String CONTENT_TYPE = "Content-Type";
  String CONTENT_LANGUAGE = "Content-Language";
  String DATE = "Date";
  String ETAG = "ETag";
  String LAST_MODIFIED = "Last-Modified";

  /*
   * Amazon HTTP Headers used by S3A.
   */

  /** S3's version ID header. */
  String S3_VERSION_ID = "x-amz-version-id";

  /** Header describing what class of storage a user wants. */
  String STORAGE_CLASS = "x-amz-storage-class";

  /** Header describing what archive tier the object is in, if any. */
  String ARCHIVE_STATUS = "x-amz-archive-status";

  /** Header for optional server-side encryption algorithm. */
  String SERVER_SIDE_ENCRYPTION = "x-amz-server-side-encryption";

  /** Range header for the get object request. */
  String RANGE = "Range";

  /**
   * Encrypted symmetric key header that is used in the Encryption Only (EO) envelope
   * encryption mechanism.
   */
  @Deprecated
  String CRYPTO_KEY = "x-amz-key";

  /** JSON-encoded description of encryption materials used during encryption. */
  String MATERIALS_DESCRIPTION = "x-amz-matdesc";

  /** Header for the optional restore information of an object. */
  String RESTORE = "x-amz-restore";

  /**
   * Key wrapping algorithm such as "AESWrap" and "RSA/ECB/OAEPWithSHA-256AndMGF1Padding".
   */
  String CRYPTO_KEYWRAP_ALGORITHM = "x-amz-wrap-alg";
  /**
   * Content encryption algorithm, such as "AES/GCM/NoPadding".
   */
  String CRYPTO_CEK_ALGORITHM = "x-amz-cek-alg";

  /**
   * Headers in request indicating that the requester must be charged for data
   * transfer.
   */
  String REQUESTER_PAYS_HEADER = "x-amz-request-payer";

  /** Header for the replication status of an Amazon S3 Object.*/
  String OBJECT_REPLICATION_STATUS = "x-amz-replication-status";

  String OBJECT_LOCK_MODE = "x-amz-object-lock-mode";

  String OBJECT_LOCK_RETAIN_UNTIL_DATE = "x-amz-object-lock-retain-until-date";

  String OBJECT_LOCK_LEGAL_HOLD_STATUS = "x-amz-object-lock-legal-hold";

}