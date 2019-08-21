package org.apache.hadoop.ozone.web.ozShell;

import java.io.IOException;

import org.apache.hadoop.ozone.web.utils.JsonUtils;

/**
 * Utility to print out response object in human readable form.
 */
public final class ObjectPrinter {
  private ObjectPrinter() {
  }

  public static String getObjectAsJson(Object o) throws IOException {
    return JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(o));
  }

  public static void printObjectAsJson(Object o) throws IOException {
    System.out.println(getObjectAsJson(o));
  }
}
