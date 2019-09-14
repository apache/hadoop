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

package org.apache.hadoop.ozone.s3.endpoint;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.ozone.s3.commontypes.IsoDateAdapter;
import org.apache.hadoop.ozone.s3.util.S3StorageType;

/**
 * AWS compatible REST response for list multipart upload.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListMultipartUploadsResult", namespace =
    "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListMultipartUploadsResult {

  public static final Owner
      NOT_SUPPORTED_OWNER = new Owner("NOT-SUPPORTED", "Not Supported");

  @XmlElement(name = "Bucket")
  private String bucket;

  @XmlElement(name = "KeyMarker")
  private String keyMarker;

  @XmlElement(name = "UploadIdMarker")
  private String uploadIdMarker;

  @XmlElement(name = "NextKeyMarker")
  private String nextKeyMarker;

  @XmlElement(name = "NextUploadIdMarker")
  private String nextUploadIdMarker;

  @XmlElement(name = "MaxUploads")
  private int maxUploads = 1000;

  @XmlElement(name = "IsTruncated")
  private boolean isTruncated = false;

  @XmlElement(name = "Upload")
  private List<Upload> uploads = new ArrayList<>();

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getKeyMarker() {
    return keyMarker;
  }

  public void setKeyMarker(String keyMarker) {
    this.keyMarker = keyMarker;
  }

  public String getUploadIdMarker() {
    return uploadIdMarker;
  }

  public void setUploadIdMarker(String uploadIdMarker) {
    this.uploadIdMarker = uploadIdMarker;
  }

  public String getNextKeyMarker() {
    return nextKeyMarker;
  }

  public void setNextKeyMarker(String nextKeyMarker) {
    this.nextKeyMarker = nextKeyMarker;
  }

  public String getNextUploadIdMarker() {
    return nextUploadIdMarker;
  }

  public void setNextUploadIdMarker(String nextUploadIdMarker) {
    this.nextUploadIdMarker = nextUploadIdMarker;
  }

  public int getMaxUploads() {
    return maxUploads;
  }

  public void setMaxUploads(int maxUploads) {
    this.maxUploads = maxUploads;
  }

  public boolean isTruncated() {
    return isTruncated;
  }

  public void setTruncated(boolean truncated) {
    isTruncated = truncated;
  }

  public List<Upload> getUploads() {
    return uploads;
  }

  public void setUploads(
      List<Upload> uploads) {
    this.uploads = uploads;
  }

  public void addUpload(Upload upload) {
    this.uploads.add(upload);
  }

  /**
   * Upload information.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Upload")
  public static class Upload {

    @XmlElement(name = "Key")
    private String key;

    @XmlElement(name = "UploadId")
    private String uploadId;

    @XmlElement(name = "Owner")
    private Owner owner = NOT_SUPPORTED_OWNER;

    @XmlElement(name = "Initiator")
    private Owner initiator = NOT_SUPPORTED_OWNER;

    @XmlElement(name = "StorageClass")
    private String storageClass = "STANDARD";

    @XmlJavaTypeAdapter(IsoDateAdapter.class)
    @XmlElement(name = "Initiated")
    private Instant initiated;

    public Upload() {
    }

    public Upload(String key, String uploadId, Instant initiated) {
      this.key = key;
      this.uploadId = uploadId;
      this.initiated = initiated;
    }

    public Upload(String key, String uploadId, Instant initiated,
        S3StorageType storageClass) {
      this.key = key;
      this.uploadId = uploadId;
      this.initiated = initiated;
      this.storageClass = storageClass.toString();
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getUploadId() {
      return uploadId;
    }

    public void setUploadId(String uploadId) {
      this.uploadId = uploadId;
    }

    public Owner getOwner() {
      return owner;
    }

    public void setOwner(
        Owner owner) {
      this.owner = owner;
    }

    public Owner getInitiator() {
      return initiator;
    }

    public void setInitiator(
        Owner initiator) {
      this.initiator = initiator;
    }

    public String getStorageClass() {
      return storageClass;
    }

    public void setStorageClass(String storageClass) {
      this.storageClass = storageClass;
    }

    public Instant getInitiated() {
      return initiated;
    }

    public void setInitiated(Instant initiated) {
      this.initiated = initiated;
    }
  }

  /**
   * Upload information.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Owner")
  public static class Owner {

    @XmlElement(name = "ID")
    private String id;

    @XmlElement(name = "DisplayName")
    private String displayName;

    public Owner() {
    }

    public Owner(String id, String displayName) {
      this.id = id;
      this.displayName = displayName;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getDisplayName() {
      return displayName;
    }

    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }
  }
}
