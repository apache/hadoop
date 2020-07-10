package org.apache.hadoop.util.noguava;

import javax.annotation.Nullable;
import org.slf4j.LoggerFactory;


public final class Strings {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(Strings.class);
  private Strings() {}

  // TODO(diamondm) consider using Arrays.toString() for array parameters
  public static String lenientFormat(
      @Nullable String template, @Nullable Object... args) {
    template = String.valueOf(template); // null -> "null"

    if (args == null) {
      args = new Object[] {"(Object[])null"};
    } else {
      for (int i = 0; i < args.length; i++) {
        args[i] = lenientToString(args[i]);
      }
    }

    // start substituting the arguments into the '%s' placeholders
    StringBuilder builder = new StringBuilder(template.length() + 16 * args.length);
    int templateStart = 0;
    int i = 0;
    while (i < args.length) {
      int placeholderStart = template.indexOf("%s", templateStart);
      if (placeholderStart == -1) {
        break;
      }
      builder.append(template, templateStart, placeholderStart);
      builder.append(args[i++]);
      templateStart = placeholderStart + 2;
    }
    builder.append(template, templateStart, template.length());

    // if we run out of placeholders, append the extra args in square braces
    if (i < args.length) {
      builder.append(" [");
      builder.append(args[i++]);
      while (i < args.length) {
        builder.append(", ");
        builder.append(args[i++]);
      }
      builder.append(']');
    }

    return builder.toString();
  }

  private static String lenientToString(@Nullable Object o) {
    try {
      return String.valueOf(o);
    } catch (Exception e) {
      // Default toString() behavior - see Object.toString()
      String objectToString =
          o.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(o));
      // Logger is created inline with fixed name to avoid forcing Proguard to create another class.
      LOG.warn("Exception during lenientFormat for {}", objectToString, e);
      return "<" + objectToString + " threw " + e.getClass().getName() + ">";
    }
  }

  public static String nullToEmpty(@Nullable String string) {
    return (string == null) ? "" : string;
  }

  public static @Nullable String emptyToNull(@Nullable String string) {
    return (string == null || string.isEmpty()) ? null : string;
  }

  public static boolean isNullOrEmpty(@Nullable String string) {
    return string == null || string.isEmpty();
  }
}
