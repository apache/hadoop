package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;
import org.apache.hadoop.yarn.util.Records;


public abstract class DatabaseAccessResponse {

  public static DatabaseAccessResponse newInstance(List<DBRecord> records) {
    DatabaseAccessResponse response = Records.newRecord(DatabaseAccessResponse.class);
    response.setRecords(records);
    return response;
  }

  public abstract void setRecords(List<DBRecord> records);

  public abstract List<DBRecord> getRecords();
}
