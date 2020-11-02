package org.apache.hadoop.runc.docker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestRefV2 {

  public static final String CONTENT_TYPE
      = "application/vnd.docker.distribution.manifest.v2+json";

  private String digest;
  private String mediaType;
  private PlatformV2 platform;
  private long size;

  @JsonProperty
  public String getDigest() {
    return digest;
  }

  public void setDigest(String digest) {
    this.digest = digest;
  }

  @JsonProperty
  public String getMediaType() {
    return mediaType;
  }

  public void setMediaType(String mediaType) {
    this.mediaType = mediaType;
  }

  @JsonProperty
  public PlatformV2 getPlatform() {
    return platform;
  }

  public void setPlatform(PlatformV2 platform) {
    this.platform = platform;
  }

  @JsonProperty
  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  @Override public String toString() {
    return String.format(
        "{ digest=%s, mediaType=%s, platform=%s, size=%d }",
        digest, mediaType, platform, size);
  }

}
