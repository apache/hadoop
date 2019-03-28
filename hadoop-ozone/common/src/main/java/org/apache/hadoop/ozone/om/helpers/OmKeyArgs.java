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
package org.apache.hadoop.ozone.om.helpers;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Args for key. Client use this to specify key's attributes on  key creation
 * (putKey()).
 */
public final class OmKeyArgs implements Auditable {
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private long dataSize;
  private final ReplicationType type;
  private final ReplicationFactor factor;
  private List<OmKeyLocationInfo> locationInfoList;
  private final boolean isMultipartKey;
  private final String multipartUploadID;
  private final int multipartUploadPartNumber;
  private Map<String, String> metadata;
  private boolean refreshPipeline;

  @SuppressWarnings("parameternumber")
  private OmKeyArgs(String volumeName, String bucketName, String keyName,
      long dataSize, ReplicationType type, ReplicationFactor factor,
      List<OmKeyLocationInfo> locationInfoList, boolean isMultipart,
      String uploadID, int partNumber,
      Map<String, String> metadataMap, boolean refreshPipeline) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.type = type;
    this.factor = factor;
    this.locationInfoList = locationInfoList;
    this.isMultipartKey = isMultipart;
    this.multipartUploadID = uploadID;
    this.multipartUploadPartNumber = partNumber;
    this.metadata = metadataMap;
    this.refreshPipeline = refreshPipeline;
  }

  public boolean getIsMultipartKey() {
    return isMultipartKey;
  }

  public String getMultipartUploadID() {
    return multipartUploadID;
  }

  public int getMultipartUploadPartNumber() {
    return multipartUploadPartNumber;
  }

  public ReplicationType getType() {
    return type;
  }

  public ReplicationFactor getFactor() {
    return factor;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long size) {
    dataSize = size;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public void setLocationInfoList(List<OmKeyLocationInfo> locationInfoList) {
    this.locationInfoList = locationInfoList;
  }

  public List<OmKeyLocationInfo> getLocationInfoList() {
    return locationInfoList;
  }

  public boolean getRefreshPipeline() {
    return refreshPipeline;
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volumeName);
    auditMap.put(OzoneConsts.BUCKET, this.bucketName);
    auditMap.put(OzoneConsts.KEY, this.keyName);
    auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(this.dataSize));
    auditMap.put(OzoneConsts.REPLICATION_TYPE,
        (this.type != null) ? this.type.name() : null);
    auditMap.put(OzoneConsts.REPLICATION_FACTOR,
        (this.factor != null) ? this.factor.name() : null);
    auditMap.put(OzoneConsts.KEY_LOCATION_INFO,
        (this.locationInfoList != null) ? locationInfoList.toString() : null);
    return auditMap;
  }

  @VisibleForTesting
  public void addLocationInfo(OmKeyLocationInfo locationInfo) {
    if (this.locationInfoList == null) {
      locationInfoList = new ArrayList<>();
    }
    locationInfoList.add(locationInfo);
  }

  /**
   * Builder class of OmKeyArgs.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long dataSize;
    private ReplicationType type;
    private ReplicationFactor factor;
    private List<OmKeyLocationInfo> locationInfoList;
    private boolean isMultipartKey;
    private String multipartUploadID;
    private int multipartUploadPartNumber;
    private Map<String, String> metadata = new HashMap<>();
    private boolean refreshPipeline;

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setKeyName(String key) {
      this.keyName = key;
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setType(ReplicationType replicationType) {
      this.type = replicationType;
      return this;
    }

    public Builder setFactor(ReplicationFactor replicationFactor) {
      this.factor = replicationFactor;
      return this;
    }

    public Builder setLocationInfoList(List<OmKeyLocationInfo> locationInfos) {
      this.locationInfoList = locationInfos;
      return this;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public Builder setMultipartUploadID(String uploadID) {
      this.multipartUploadID = uploadID;
      return this;
    }

    public Builder setMultipartUploadPartNumber(int partNumber) {
      this.multipartUploadPartNumber = partNumber;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> metadatamap) {
      this.metadata.putAll(metadatamap);
      return this;
    }

    public Builder setRefreshPipeline(boolean refresh) {
      this.refreshPipeline = refresh;
      return this;
    }

    public OmKeyArgs build() {
      return new OmKeyArgs(volumeName, bucketName, keyName, dataSize, type,
          factor, locationInfoList, isMultipartKey, multipartUploadID,
          multipartUploadPartNumber, metadata, refreshPipeline);
    }

  }
}
