package org.apache.hadoop.ozone.fsck;

import java.util.Objects;

/**
 * Getter and Setter for BlockDetails.
 */

public class BlockIdDetails {

  private String bucketName;
  private String blockVol;
  private  String keyName;

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getBlockVol() {
    return blockVol;
  }

  public void setBlockVol(String blockVol) {
    this.blockVol = blockVol;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  @Override
  public String toString() {
    return "BlockIdDetails{" +
        "bucketName='" + bucketName + '\'' +
        ", blockVol='" + blockVol + '\'' +
        ", keyName='" + keyName + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockIdDetails that = (BlockIdDetails) o;
    return Objects.equals(bucketName, that.bucketName) &&
        Objects.equals(blockVol, that.blockVol) &&
        Objects.equals(keyName, that.keyName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucketName, blockVol, keyName);
  }
}