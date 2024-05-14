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

package org.apache.hadoop.fs.s3a.audit;

import java.util.List;

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateSessionRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_HEAD_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.MULTIPART_UPLOAD_ABORTED;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.MULTIPART_UPLOAD_COMPLETED;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.MULTIPART_UPLOAD_LIST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.MULTIPART_UPLOAD_PART_PUT;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.MULTIPART_UPLOAD_STARTED;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_PUT_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.STORE_EXISTS_PROBE;

/**
 * Extract information from a request.
 * Intended for reporting and error logs.
 */
public class AWSRequestAnalyzer {

  /**
   * Given an AWS request, try to analyze it to operation,
   * read/write and path.
   * @param request request.
   * @return information about the request.
   */
  public RequestInfo analyze(SdkRequest request) {

    // this is where Scala's case statement would massively
    // simplify life.
    // Please Keep in Alphabetical Order.
    if (request instanceof AbortMultipartUploadRequest) {
      return writing(MULTIPART_UPLOAD_ABORTED,
          ((AbortMultipartUploadRequest) request).key(),
          0);
    } else if (request instanceof CompleteMultipartUploadRequest) {
      CompleteMultipartUploadRequest r
          = (CompleteMultipartUploadRequest) request;
      return writing(MULTIPART_UPLOAD_COMPLETED,
          r.key(),
          r.multipartUpload().parts().size());
    } else if (request instanceof CreateMultipartUploadRequest) {
      return writing(MULTIPART_UPLOAD_STARTED,
          ((CreateMultipartUploadRequest) request).key(),
          0);
    } else if (request instanceof DeleteObjectRequest) {
      // DeleteObject: single object
      return writing(OBJECT_DELETE_REQUEST,
          ((DeleteObjectRequest) request).key(),
          1);
    } else if (request instanceof DeleteObjectsRequest) {
      // DeleteObjects: bulk delete
      // use first key as the path
      DeleteObjectsRequest r = (DeleteObjectsRequest) request;
      List<ObjectIdentifier> objectIdentifiers
          = r.delete().objects();
      return writing(OBJECT_BULK_DELETE_REQUEST,
          objectIdentifiers.isEmpty() ? null : objectIdentifiers.get(0).key(),
          objectIdentifiers.size());
    } else if (request instanceof GetBucketLocationRequest) {
      GetBucketLocationRequest r = (GetBucketLocationRequest) request;
      return reading(STORE_EXISTS_PROBE,
          r.bucket(),
          0);
    } else if (request instanceof GetObjectRequest) {
      GetObjectRequest r = (GetObjectRequest) request;
      return reading(ACTION_HTTP_GET_REQUEST,
          r.key(),
          sizeFromRangeHeader(r.range()));
    } else if (request instanceof HeadObjectRequest) {
      return reading(ACTION_HTTP_HEAD_REQUEST,
          ((HeadObjectRequest) request).key(), 0);
    } else if (request instanceof ListMultipartUploadsRequest) {
      ListMultipartUploadsRequest r
          = (ListMultipartUploadsRequest) request;
      return reading(MULTIPART_UPLOAD_LIST,
          r.prefix(),
          r.maxUploads());
    } else if (request instanceof ListObjectsRequest) {
      ListObjectsRequest r = (ListObjectsRequest) request;
      return reading(OBJECT_LIST_REQUEST,
          r.prefix(),
          r.maxKeys());
    } else if (request instanceof ListObjectsV2Request) {
      ListObjectsV2Request r = (ListObjectsV2Request) request;
      return reading(OBJECT_LIST_REQUEST,
          r.prefix(),
          r.maxKeys());
    } else if (request instanceof PutObjectRequest) {
      PutObjectRequest r = (PutObjectRequest) request;
      return writing(OBJECT_PUT_REQUEST,
          r.key(),
          0);
    } else if (request instanceof UploadPartRequest) {
      UploadPartRequest r = (UploadPartRequest) request;
      return writing(MULTIPART_UPLOAD_PART_PUT,
          r.key(),
          r.contentLength());
    }
    // no explicit support, return classname
    return writing(request.getClass().getName(), null, 0);
  }

  /**
   * A request.
   * @param verb verb
   * @param mutating does this update the store
   * @param key object/prefix, etc.
   * @param size nullable size
   * @return request info
   */
  private RequestInfo request(final String verb,
      final boolean mutating,
      final String key,
      final Number size) {
    return new RequestInfo(verb, mutating, key, size);
  }

  /**
   * A read request.
   * @param verb verb
   * @param key object/prefix, etc.
   * @param size nullable size
   * @return request info
   */
  private RequestInfo reading(final String verb,
      final String key, final Number size) {
    return request(verb, false, key, size);
  }

  /**
   * A write request of some form.
   * @param verb verb
   * @param key object/prefix, etc.
   * @param size nullable size
   * @return request info
   */
  private RequestInfo writing(final String verb,
      final String key, final Number size) {
    return request(verb, true, key, size);
  }

  /**
   * Predicate which returns true if the request is of a kind which
   * could be outside a span because of how the AWS SDK generates them.
   * @param request request
   * @return true if the transfer manager creates them.
   */
  public static boolean
      isRequestNotAlwaysInSpan(final Object request) {
    return request instanceof UploadPartCopyRequest
        || request instanceof CompleteMultipartUploadRequest
        || request instanceof GetBucketLocationRequest
        || request instanceof CreateSessionRequest;
  }

  /**
   * Predicate which returns true if the request is part of the
   * multipart upload API -and which therefore must be rejected
   * if multipart upload is disabled.
   * @param request request
   * @return true if the transfer manager creates them.
   */
  public static boolean isRequestMultipartIO(final Object request) {
    return request instanceof UploadPartCopyRequest
        || request instanceof CompleteMultipartUploadRequest
        || request instanceof  CreateMultipartUploadRequest
        || request instanceof UploadPartRequest;
  }

  /**
   * Info about a request.
   */
  public static final class RequestInfo {

    /**
     * Verb.
     */
    private String verb;

    /**
     * Is this a mutating call?
     */
    private boolean mutating;

    /**
     * Key if there is one; maybe first key in a list.
     */
    private String key;

    /**
     * Size, where the meaning of size depends on the request.
     */
    private long size;

    /**
     * Construct.
     * @param verb operation/classname, etc.
     * @param mutating does this update S3 State.
     * @param key key/path/bucket operated on.
     * @param size size of request (bytes, elements, limit...). Nullable.
     */
    private RequestInfo(final String verb,
        final boolean mutating,
        final String key,
        final Number size) {
      this.verb = verb;
      this.mutating = mutating;
      this.key = key;
      this.size = toSafeLong(size);
    }

    public String getVerb() {
      return verb;
    }

    public boolean isMutating() {
      return mutating;
    }

    public String getKey() {
      return key;
    }

    public long getSize() {
      return size;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "{");
      sb.append(verb);
      if (key != null) {
        sb.append(" '").append(key).append('\'');
      }
      sb.append(" size=").append(size);
      sb.append(", mutating=").append(mutating);
      sb.append('}');
      return sb.toString();
    }
  }

  private static long toSafeLong(final Number size) {
    return size != null ? size.longValue() : 0;
  }

  private static final String BYTES_PREFIX = "bytes=";

  /**
   * Given a range header, determine the size of the request.
   * @param rangeHeader header string
   * @return parsed size or -1 for problems
   */
  private static Number sizeFromRangeHeader(String rangeHeader) {
    if (rangeHeader != null && rangeHeader.startsWith(BYTES_PREFIX)) {
      String[] values = rangeHeader
          .substring(BYTES_PREFIX.length())
          .split("-");
      if (values.length == 2) {
        try {
          long start = Long.parseUnsignedLong(values[0]);
          long end = Long.parseUnsignedLong(values[1]);
          return end - start;
        } catch(NumberFormatException e) {
        }
      }
    }
    return -1;
  }
}
