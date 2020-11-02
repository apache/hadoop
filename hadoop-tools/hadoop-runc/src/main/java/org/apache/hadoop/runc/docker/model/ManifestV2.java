package org.apache.hadoop.runc.docker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestV2 {

  public static String CONTENT_TYPE =
      "application/vnd.docker.distribution.manifest.v2+json";

  public static boolean matches(String contentType) {
    return CONTENT_TYPE.equals(contentType);
  }

  private int schemaVersion;
  private String mediaType;
  private BlobV2 config;
  private List<BlobV2> layers = new ArrayList<>();

  @JsonProperty
  public int getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(int schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  @JsonProperty
  public String getMediaType() {
    return mediaType;
  }

  public void setMediaType(String mediaType) {
    this.mediaType = mediaType;
  }

  @JsonProperty
  public BlobV2 getConfig() {
    return config;
  }

  public void setConfig(BlobV2 config) {
    this.config = config;
  }

  @JsonProperty
  public List<BlobV2> getLayers() {
    return layers;
  }

  @Override
  public String toString() {
    return String.format(
        "{%n"
            + "  schemaVersion=%d,%n"
            + "  mediaType=%s,%n"
            + "  config=%s,%n"
            + "  layers=%s%n"
            + "}",
        schemaVersion,
        mediaType,
        config,
        layers
            .stream()
            .map(Objects::toString)
            .collect(Collectors.joining(",\n    ", "[\n    ", "\n  ]")));
  }

}
