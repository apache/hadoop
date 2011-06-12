package org.apache.hadoop.yarn.factories;

import org.apache.hadoop.yarn.YarnException;


public interface RecordFactory {
  public <T> T newRecordInstance(Class<T> clazz) throws YarnException;
}
