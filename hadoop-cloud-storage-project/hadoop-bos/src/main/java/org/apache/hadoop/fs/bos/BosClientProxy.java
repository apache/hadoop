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

package org.apache.hadoop.fs.bos;

import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.CompleteMultipartUploadResponse;
import com.baidubce.services.bos.model.CopyObjectRequest;
import com.baidubce.services.bos.model.CopyObjectResponse;
import com.baidubce.services.bos.model.DeleteDirectoryResponse;
import com.baidubce.services.bos.model.DeleteMultipleObjectsRequest;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.PutObjectResponse;
import com.baidubce.services.bos.model.RenameObjectResponse;
import com.baidubce.services.bos.model.UploadPartCopyRequest;
import com.baidubce.services.bos.model.UploadPartCopyResponse;
import com.baidubce.services.bos.model.UploadPartRequest;
import com.baidubce.services.bos.model.UploadPartResponse;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Interface defining the client operations for interacting
 * with the Baidu Object Storage (BOS) service.
 */
public interface BosClientProxy {

  /**
   * Initializes the BOS client with the given URI and
   * configuration.
   *
   * @param uri  the URI of the BOS filesystem
   * @param conf the Hadoop configuration
   */
  void init(URI uri, Configuration conf);

  /**
   * Uploads an object to BOS.
   *
   * @param bucketName the name of the bucket
   * @param key        the object key
   * @param input      the input stream containing object data
   * @param metadata   the object metadata
   * @return the response from the put operation
   * @throws IOException if an I/O error occurs
   */
  PutObjectResponse putObject(
      String bucketName, String key,
      InputStream input, ObjectMetadata metadata)
      throws IOException;

  /**
   * Uploads a zero-length object to BOS.
   *
   * @param bucketName the name of the bucket
   * @param key        the object key
   * @param metadata   the object metadata
   * @throws IOException if an I/O error occurs
   */
  void putEmptyObject(
      String bucketName, String key,
      ObjectMetadata metadata) throws IOException;

  /**
   * Retrieves the metadata for an object in BOS.
   *
   * @param bucketName the name of the bucket
   * @param key        the object key
   * @return the object metadata
   * @throws IOException if an I/O error occurs
   */
  ObjectMetadata getObjectMetadata(
      String bucketName, String key) throws IOException;

  /**
   * Retrieves an object from BOS.
   *
   * @param request the get object request
   * @return the BOS object
   * @throws IOException if an I/O error occurs
   */
  BosObject getObject(GetObjectRequest request)
      throws IOException;

  /**
   * Lists objects in a BOS bucket.
   *
   * @param request the list objects request
   * @return the list objects response
   * @throws IOException if an I/O error occurs
   */
  ListObjectsResponse listObjects(ListObjectsRequest request)
      throws IOException;

  /**
   * Uploads a part in a multipart upload.
   *
   * @param request the upload part request
   * @return the upload part response
   * @throws IOException if an I/O error occurs
   */
  UploadPartResponse uploadPart(UploadPartRequest request)
      throws IOException;

  /**
   * Deletes an object from BOS.
   *
   * @param bucketName the name of the bucket
   * @param key        the object key
   * @throws IOException if an I/O error occurs
   */
  void deleteObject(String bucketName, String key)
      throws IOException;

  /**
   * Deletes a directory from a hierarchy-enabled BOS bucket.
   *
   * @param bucketName        the name of the bucket
   * @param key               the directory key
   * @param isDeleteRecursive whether to delete recursively
   * @param marker            the marker for pagination
   * @return the delete directory response
   * @throws IOException if an I/O error occurs
   */
  DeleteDirectoryResponse deleteDirectory(
      String bucketName, String key,
      boolean isDeleteRecursive, String marker)
      throws IOException;

  /**
   * Deletes multiple objects from BOS in a single request.
   *
   * @param request the delete multiple objects request
   * @throws IOException if an I/O error occurs
   */
  void deleteMultipleObjects(
      DeleteMultipleObjectsRequest request)
      throws IOException;

  /**
   * Copies an object within BOS using bucket names and keys.
   *
   * @param sourceBucketName      the source bucket name
   * @param sourceKey             the source object key
   * @param destinationBucketName the destination bucket name
   * @param destinationKey        the destination object key
   * @return the copy object response
   * @throws IOException if an I/O error occurs
   */
  CopyObjectResponse copyObject(
      String sourceBucketName, String sourceKey,
      String destinationBucketName,
      String destinationKey) throws IOException;

  /**
   * Copies an object within BOS using a copy request.
   *
   * @param request the copy object request
   * @return the copy object response
   * @throws IOException if an I/O error occurs
   */
  CopyObjectResponse copyObject(CopyObjectRequest request)
      throws IOException;

  /**
   * Renames an object in a BOS bucket.
   *
   * @param bucketName     the name of the bucket
   * @param sourceKey      the source object key
   * @param destinationKey the destination object key
   * @return the rename object response
   * @throws IOException if an I/O error occurs
   */
  RenameObjectResponse renameObject(
      String bucketName, String sourceKey,
      String destinationKey) throws IOException;

  /**
   * Completes a multipart upload.
   *
   * @param request the complete multipart upload request
   * @return the complete multipart upload response
   * @throws IOException if an I/O error occurs
   */
  CompleteMultipartUploadResponse completeMultipartUpload(
      CompleteMultipartUploadRequest request)
      throws IOException;

  /**
   * Initiates a multipart upload.
   *
   * @param request the initiate multipart upload request
   * @return the initiate multipart upload response
   * @throws IOException if an I/O error occurs
   */
  InitiateMultipartUploadResponse initiateMultipartUpload(
      InitiateMultipartUploadRequest request)
      throws IOException;

  /**
   * Checks whether the specified bucket is a hierarchy
   * (namespace) bucket.
   *
   * @param bucketName the name of the bucket
   * @return true if the bucket is a hierarchy bucket
   * @throws IOException if an I/O error occurs
   */
  boolean isHierarchyBucket(String bucketName)
      throws IOException;

  /**
   * Closes the BOS client and releases resources.
   */
  void close();

  /**
   * Copies a part of an object using a multipart copy request.
   *
   * @param uploadPartCopyRequest the upload part copy request
   * @return the upload part copy response
   * @throws IOException if an I/O error occurs
   */
  UploadPartCopyResponse uploadPartCopy(
      UploadPartCopyRequest uploadPartCopyRequest)
      throws IOException;
}
