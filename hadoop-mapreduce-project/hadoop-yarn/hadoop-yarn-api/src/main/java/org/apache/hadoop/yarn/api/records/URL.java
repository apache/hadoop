package org.apache.hadoop.yarn.api.records;

public interface URL {
  public abstract String getScheme();
  public abstract String getHost();
  public abstract int getPort();
  public abstract String getFile();
  
  public abstract void setScheme(String scheme);
  public abstract void setHost(String host);
  public abstract void setPort(int port);
  public abstract void setFile(String file);
}
