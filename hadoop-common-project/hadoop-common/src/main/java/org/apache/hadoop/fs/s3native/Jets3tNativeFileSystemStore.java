/**
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

package org.apache.hadoop.fs.s3native;

import static org.apache.hadoop.fs.s3native.NativeS3FileSystem.PATH_DELIMITER;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.fs.s3.S3Exception;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Jets3tNativeFileSystemStore implements NativeFileSystemStore {
  
  private S3Service s3Service;
  private S3Bucket bucket;
  public static final Log LOG =
      LogFactory.getLog(Jets3tNativeFileSystemStore.class);

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    S3Credentials s3Credentials = new S3Credentials();
    s3Credentials.initialize(uri, conf);
    try {
      AWSCredentials awsCredentials =
        new AWSCredentials(s3Credentials.getAccessKey(),
            s3Credentials.getSecretAccessKey());
      this.s3Service = new RestS3Service(awsCredentials);
    } catch (S3ServiceException e) {
      handleS3ServiceException(e);
    }
    bucket = new S3Bucket(uri.getHost());
  }
  
  @Override
  public void storeFile(String key, File file, byte[] md5Hash)
    throws IOException {
    
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      S3Object object = new S3Object(key);
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(file.length());
      if (md5Hash != null) {
        object.setMd5Hash(md5Hash);
      }
      s3Service.putObject(bucket, object);
    } catch (S3ServiceException e) {
      handleS3ServiceException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  @Override
  public void storeEmptyFile(String key) throws IOException {
    try {
      S3Object object = new S3Object(key);
      object.setDataInputStream(new ByteArrayInputStream(new byte[0]));
      object.setContentType("binary/octet-stream");
      object.setContentLength(0);
      s3Service.putObject(bucket, object);
    } catch (S3ServiceException e) {
      handleS3ServiceException(e);
    }
  }
  
  @Override
  public FileMetadata retrieveMetadata(String key) throws IOException {
    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Getting metadata for key: " + key + " from bucket:" + bucket.getName());
      }
      S3Object object = s3Service.getObject(bucket.getName(), key);
      return new FileMetadata(key, object.getContentLength(),
          object.getLastModifiedDate().getTime());
    } catch (S3ServiceException e) {
      // Following is brittle. Is there a better way?
      if (e.getS3ErrorCode().matches("NoSuchKey")) {
        return null; //return null if key not found
      }
      handleS3ServiceException(e);
      return null; //never returned - keep compiler happy
    }
  }

  /**
   * @param key
   * The key is the object name that is being retrieved from the S3 bucket
   * @return
   * This method returns null if the key is not found
   * @throws IOException
   */

  @Override
  public InputStream retrieve(String key) throws IOException {
    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Getting key: " + key + " from bucket:" + bucket.getName());
      }
      S3Object object = s3Service.getObject(bucket.getName(), key);
      return object.getDataInputStream();
    } catch (S3ServiceException e) {
      handleS3ServiceException(key, e);
      return null; //never returned - keep compiler happy
    } catch (ServiceException e) {
      handleServiceException(e);
      return null; //return null if key not found
    }
  }

  /**
   *
   * @param key
   * The key is the object name that is being retrieved from the S3 bucket
   * @return
   * This method returns null if the key is not found
   * @throws IOException
   */

  @Override
  public InputStream retrieve(String key, long byteRangeStart)
          throws IOException {
    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Getting key: " + key + " from bucket:" + bucket.getName() + " with byteRangeStart: " + byteRangeStart);
      }
      S3Object object = s3Service.getObject(bucket, key, null, null, null,
                                            null, byteRangeStart, null);
      return object.getDataInputStream();
    } catch (S3ServiceException e) {
      handleS3ServiceException(key, e);
      return null; //never returned - keep compiler happy
    } catch (ServiceException e) {
      handleServiceException(e);
      return null; //return null if key not found
    }
  }

  @Override
  public PartialListing list(String prefix, int maxListingLength)
          throws IOException {
    return list(prefix, maxListingLength, null, false);
  }
  
  @Override
  public PartialListing list(String prefix, int maxListingLength, String priorLastKey,
      boolean recurse) throws IOException {

    return list(prefix, recurse ? null : PATH_DELIMITER, maxListingLength, priorLastKey);
  }

  /**
   *
   * @return
   * This method returns null if the list could not be populated
   * due to S3 giving ServiceException
   * @throws IOException
   */

  private PartialListing list(String prefix, String delimiter,
      int maxListingLength, String priorLastKey) throws IOException {
    try {
      if (prefix.length() > 0 && !prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      StorageObjectsChunk chunk = s3Service.listObjectsChunked(bucket.getName(),
          prefix, delimiter, maxListingLength, priorLastKey);
      
      FileMetadata[] fileMetadata =
        new FileMetadata[chunk.getObjects().length];
      for (int i = 0; i < fileMetadata.length; i++) {
        StorageObject object = chunk.getObjects()[i];
        fileMetadata[i] = new FileMetadata(object.getKey(),
            object.getContentLength(), object.getLastModifiedDate().getTime());
      }
      return new PartialListing(chunk.getPriorLastKey(), fileMetadata,
          chunk.getCommonPrefixes());
    } catch (S3ServiceException e) {
      handleS3ServiceException(e);
      return null; //never returned - keep compiler happy
    } catch (ServiceException e) {
      handleServiceException(e);
      return null; //return null if list could not be populated
    }
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Deleting key:" + key + "from bucket" + bucket.getName());
      }
      s3Service.deleteObject(bucket, key);
    } catch (S3ServiceException e) {
      handleS3ServiceException(key, e);
    }
  }
  
  @Override
  public void copy(String srcKey, String dstKey) throws IOException {
    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Copying srcKey: " + srcKey + "to dstKey: " + dstKey + "in bucket: " + bucket.getName());
      }
      s3Service.copyObject(bucket.getName(), srcKey, bucket.getName(),
          new S3Object(dstKey), false);
    } catch (S3ServiceException e) {
      handleS3ServiceException(srcKey, e);
    } catch (ServiceException e) {
      handleServiceException(e);
    }
  }

  @Override
  public void purge(String prefix) throws IOException {
    try {
      S3Object[] objects = s3Service.listObjects(bucket.getName(), prefix, null);
      for (S3Object object : objects) {
        s3Service.deleteObject(bucket, object.getKey());
      }
    } catch (S3ServiceException e) {
      handleS3ServiceException(e);
    }
  }

  @Override
  public void dump() throws IOException {
    StringBuilder sb = new StringBuilder("S3 Native Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      S3Object[] objects = s3Service.listObjects(bucket.getName());
      for (S3Object object : objects) {
        sb.append(object.getKey()).append("\n");
      }
    } catch (S3ServiceException e) {
      handleS3ServiceException(e);
    }
    System.out.println(sb);
  }

  private void handleS3ServiceException(String key, S3ServiceException e) throws IOException {
    if ("NoSuchKey".equals(e.getS3ErrorCode())) {
      throw new FileNotFoundException("Key '" + key + "' does not exist in S3");
    } else {
      handleS3ServiceException(e);
    }
  }

  private void handleS3ServiceException(S3ServiceException e) throws IOException {
    if (e.getCause() instanceof IOException) {
      throw (IOException) e.getCause();
    }
    else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("S3 Error code: " + e.getS3ErrorCode() + "; S3 Error message: " + e.getS3ErrorMessage());
      }
      throw new S3Exception(e);
    }
  }

  private void handleServiceException(ServiceException e) throws IOException {
    if (e.getCause() instanceof IOException) {
      throw (IOException) e.getCause();
    }
    else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Got ServiceException with Error code: " + e.getErrorCode() + ";and Error message: " + e.getErrorMessage());
      }
    }
  }
}
