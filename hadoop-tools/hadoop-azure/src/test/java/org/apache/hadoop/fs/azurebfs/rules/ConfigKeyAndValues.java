package org.apache.hadoop.fs.azurebfs.rules;

import java.util.List;

public class ConfigKeyAndValues {

  private String key;
  private List<String> values;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public List<String> getValues() {
    return values;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }
}
