package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.yarn.webapp.WebApps;

public class TestHSWebApp {
  public static void main(String[] args) {
    WebApps.$for("yarn").at(19888).start().joinThread();
  }
}
