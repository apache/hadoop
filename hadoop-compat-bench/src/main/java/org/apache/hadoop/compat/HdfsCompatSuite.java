package org.apache.hadoop.compat;


public interface HdfsCompatSuite {
  String getSuiteName();

  Class<? extends AbstractHdfsCompatCase>[] getApiCases();

  String[] getShellCases();
}