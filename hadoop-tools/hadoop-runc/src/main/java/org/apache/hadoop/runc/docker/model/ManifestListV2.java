package org.apache.hadoop.runc.docker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestListV2 {

  public static String CONTENT_TYPE =
      "application/vnd.docker.distribution.manifest.list.v2+json";

  public static boolean matches(String contentType) {
    return CONTENT_TYPE.equals(contentType);
  }

  private String digest;

  private int schemaVersion;
  private String mediaType;
  private List<ManifestRefV2> manifests = new ArrayList<>();

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
  public List<ManifestRefV2> getManifests() {
    return manifests;
  }

  @Override
  public String toString() {
    return String.format(
        "{%n"
            + "  schemaVersion=%d,%n"
            + "  mediaType=%s,%n"
            + "  manifests=%s%n"
            + "}",
        schemaVersion,
        mediaType,
        manifests
            .stream()
            .map(Objects::toString)
            .collect(Collectors.joining(",\n    ", "[\n    ", "\n  ]")));
  }

}
