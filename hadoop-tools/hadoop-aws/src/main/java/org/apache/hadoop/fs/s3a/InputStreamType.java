package org.apache.hadoop.fs.s3a;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum InputStreamType {
  CLASSIC("classic"),
  PREFETCH("prefetch"),
  ANALYTICS("analytics");

  private final String name;

  private static final Logger LOG = LoggerFactory.getLogger(InputStreamType.class);

  InputStreamType(String name) {
    this.name = name;
  }

  public static InputStreamType fromString(String inputStreamType) {
    for (InputStreamType value : values()) {
      if (value.name.equalsIgnoreCase(inputStreamType)) {
        return value;
      }
    }
    LOG.warn("Unknown input stream type {}, using default classic stream.", inputStreamType);

    return CLASSIC;
  }
}
