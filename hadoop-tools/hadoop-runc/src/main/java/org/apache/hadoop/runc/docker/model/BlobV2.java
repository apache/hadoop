package org.apache.hadoop.runc.docker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BlobV2 {

  private String mediaType;
  private long size;
  private String digest;

  @JsonProperty
  public String getMediaType() {
    return mediaType;
  }

  public void setMediaType(String mediaType) {
    this.mediaType = mediaType;
  }

  @JsonProperty
  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  @JsonProperty
  public String getDigest() {
    return digest;
  }

  public void setDigest(String digest) {
    this.digest = digest;
  }

  @Override
  public String toString() {
    return String.format(
        "{ mediaType=%s, size=%d, digest=%s }", mediaType, size, digest);
  }

}
