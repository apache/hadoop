package org.apache.hadoop.ozone.om.helpers;

import java.time.Instant;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Information about one initialized upload.
 */
public class OmMultipartUpload {

  private String volumeName;

  private String bucketName;

  private String keyName;

  private String uploadId;

  private Instant creationTime;

  public OmMultipartUpload(String volumeName, String bucketName,
      String keyName, String uploadId) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.uploadId = uploadId;
  }

  public OmMultipartUpload(String volumeName, String bucketName,
      String keyName, String uploadId, Instant creationDate) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.uploadId = uploadId;
    this.creationTime = creationDate;
  }

  public static OmMultipartUpload from(String key) {
    String[] split = key.split(OM_KEY_PREFIX);
    if (split.length < 5) {
      throw new IllegalArgumentException("Key " + key
          + " doesn't have enough segments to be a valid multpart upload key");
    }
    String uploadId = split[split.length - 1];
    String volume = split[1];
    String bucket = split[2];
    return new OmMultipartUpload(volume, bucket,
        key.substring(volume.length() + bucket.length() + 3,
            key.length() - uploadId.length() - 1), uploadId);
  }

  public String getDbKey() {
    return OmMultipartUpload
        .getDbKey(volumeName, bucketName, keyName, uploadId);
  }

  public static String getDbKey(String volume, String bucket, String key,
      String uploadId) {
    return getDbKey(volume, bucket, key) + OM_KEY_PREFIX + uploadId;

  }

  public static String getDbKey(String volume, String bucket, String key) {
    return OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + key;
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

  public String getUploadId() {
    return uploadId;
  }

  public Instant getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Instant creationTime) {
    this.creationTime = creationTime;
  }
}
