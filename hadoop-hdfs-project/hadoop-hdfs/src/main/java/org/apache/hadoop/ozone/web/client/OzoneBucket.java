/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.client;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.web.request.OzoneAcl;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.utils.OzoneConsts;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.util.List;

/**
 * A Bucket class the represents an Ozone Bucket.
 */
public class OzoneBucket {

  private BucketInfo bucketInfo;
  private OzoneVolume volume;

  /**
   * Constructor for bucket.
   *
   * @param info   - BucketInfo
   * @param volume - OzoneVolume Object that contains this bucket
   */
  public OzoneBucket(BucketInfo info, OzoneVolume volume) {
    this.bucketInfo = info;
    this.volume = volume;
  }

  /**
   * Gets bucket Info.
   *
   * @return BucketInfo
   */
  public BucketInfo getBucketInfo() {
    return bucketInfo;
  }

  /**
   * Sets Bucket Info.
   *
   * @param bucketInfo BucketInfo
   */
  public void setBucketInfo(BucketInfo bucketInfo) {
    this.bucketInfo = bucketInfo;
  }

  /**
   * Returns the parent volume class.
   *
   * @return - OzoneVolume
   */
  OzoneVolume getVolume() {
    return volume;
  }

  /**
   * Returns bucket name.
   *
   * @return Bucket Name
   */
  public String getBucketName() {
    return bucketInfo.getBucketName();
  }

  /**
   * Returns the Acls on the bucket.
   *
   * @return - Acls
   */
  public List<OzoneAcl> getAcls() {
    return bucketInfo.getAcls();
  }

  /**
   * Return versioning info on the bucket - Enabled or disabled.
   *
   * @return - Version Enum
   */
  public OzoneConsts.Versioning getVersioning() {
    return bucketInfo.getVersioning();
  }

  /**
   * Gets the Storage class for the bucket.
   *
   * @return Storage Class Enum
   */
  public StorageType getStorageClass() {
    return bucketInfo.getStorageType();
  }

  private static class ContentLengthHeaderRemover implements
      HttpRequestInterceptor {
    @Override
    public void process(HttpRequest request, HttpContext context)
        throws HttpException, IOException {

      // fighting org.apache.http.protocol
      // .RequestContent's ProtocolException("Content-Length header
      // already present");
      request.removeHeaders(HTTP.CONTENT_LEN);
    }
  }
}
