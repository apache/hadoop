package org.apache.hadoop.fs.azurebfs.services;

import java.net.HttpURLConnection;

public final class AbfsHttpOpTestUtil {
  private AbfsHttpOpTestUtil() {

  }

  public static void setConnection(AbfsHttpOperation op, AbfsHttpOperation copyFrom) {
    op.setConnection(copyFrom.getConnection());
  }
}
