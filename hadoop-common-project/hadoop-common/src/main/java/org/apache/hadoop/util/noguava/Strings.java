package org.apache.hadoop.util.noguava;

import javax.annotation.Nullable;
import org.slf4j.LoggerFactory;

public final class Strings {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(Strings.class);

  private Strings() {
  }

  public static String nullToEmpty(@Nullable final String str) {
    return (str == null) ? "" : str;
  }

  public static @Nullable String emptyToNull(@Nullable final String str) {
    return (str == null || str.isEmpty()) ? null : str;
  }

  public static boolean isNullOrEmpty(@Nullable final String str) {
    return str == null || str.isEmpty();
  }
}
