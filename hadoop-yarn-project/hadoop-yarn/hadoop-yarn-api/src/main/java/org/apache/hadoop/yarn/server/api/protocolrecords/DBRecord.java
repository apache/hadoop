package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

/**
 * DBRecord is setup as a Key Value pair because RM data is stored in this way
 */
public abstract class DBRecord {

  public static DBRecord newInstance(String key, String value) {
    DBRecord record = Records.newRecord(DBRecord.class);
    record.setKey(key);
    record.setValue(value);
    return record;
  }

  public abstract void setKey(String key);

  public abstract void setValue(String value);

  public abstract String getKey();

  public abstract String getValue();
}
