/**
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

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.s3.commontypes.IsoDateAdapter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Request for list parts of a multipart upload request.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListPartsResult", namespace = "http://s3.amazonaws"
    + ".com/doc/2006-03-01/")
public class ListPartsResponse {

  @XmlElement(name = "Bucket")
  private String bucket;

  @XmlElement(name = "Key")
  private String key;

  @XmlElement(name = "UploadId")
  private String uploadID;

  @XmlElement(name = "StorageClass")
  private String storageClass;

  @XmlElement(name = "PartNumberMarker")
  private int partNumberMarker;

  @XmlElement(name = "NextPartNumberMarker")
  private int nextPartNumberMarker;

  @XmlElement(name = "MaxParts")
  private int maxParts;

  @XmlElement(name = "IsTruncated")
  private boolean truncated;

  @XmlElement(name = "Part")
  private List<Part> partList = new ArrayList<>();

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getUploadID() {
    return uploadID;
  }

  public void setUploadID(String uploadID) {
    this.uploadID = uploadID;
  }

  public String getStorageClass() {
    return storageClass;
  }

  public void setStorageClass(String storageClass) {
    this.storageClass = storageClass;
  }

  public int getPartNumberMarker() {
    return partNumberMarker;
  }

  public void setPartNumberMarker(int partNumberMarker) {
    this.partNumberMarker = partNumberMarker;
  }

  public int getNextPartNumberMarker() {
    return nextPartNumberMarker;
  }

  public void setNextPartNumberMarker(int nextPartNumberMarker) {
    this.nextPartNumberMarker = nextPartNumberMarker;
  }

  public int getMaxParts() {
    return maxParts;
  }

  public void setMaxParts(int maxParts) {
    this.maxParts = maxParts;
  }

  public boolean getTruncated() {
    return truncated;
  }

  public void setTruncated(boolean truncated) {
    this.truncated = truncated;
  }

  public List<Part> getPartList() {
    return partList;
  }

  public void setPartList(List<Part> partList) {
    this.partList = partList;
  }

  public void addPart(Part part) {
    this.partList.add(part);
  }

  /**
   * Part information.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Part")
  public static class Part {

    @XmlElement(name = "PartNumber")
    private int partNumber;

    @XmlJavaTypeAdapter(IsoDateAdapter.class)
    @XmlElement(name = "LastModified")
    private Instant lastModified;

    @XmlElement(name = "ETag")
    private String eTag;


    @XmlElement(name = "Size")
    private long size;

    public int getPartNumber() {
      return partNumber;
    }

    public void setPartNumber(int partNumber) {
      this.partNumber = partNumber;
    }

    public Instant getLastModified() {
      return lastModified;
    }

    public void setLastModified(Instant lastModified) {
      this.lastModified = lastModified;
    }

    public String getETag() {
      return eTag;
    }

    public void setETag(String tag) {
      this.eTag = tag;
    }

    public long getSize() {
      return size;
    }

    public void setSize(long size) {
      this.size = size;
    }
  }
}
