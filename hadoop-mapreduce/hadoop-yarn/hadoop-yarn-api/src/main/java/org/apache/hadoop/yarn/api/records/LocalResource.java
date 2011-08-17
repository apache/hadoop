package org.apache.hadoop.yarn.api.records;

public interface LocalResource {
  public abstract URL getResource();
  public abstract long getSize();
  public abstract long getTimestamp();
  public abstract LocalResourceType getType();
  public abstract LocalResourceVisibility getVisibility();
  
  public abstract void setResource(URL resource);
  public abstract void setSize(long size);
  public abstract void setTimestamp(long timestamp);
  public abstract void setType(LocalResourceType type);
  public abstract void setVisibility(LocalResourceVisibility visibility);
}
