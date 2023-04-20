package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;


/**
 * The request used by Admins to access the underlying RM database
 * The request contains details of the operation (get / set / delete), database (RMStateStore or YarnConfigurationStore), key & value
 */
public abstract class DatabaseAccessRequest {

  /**
   *
   * @param operation - get / set / del
   * @param database - unique identifier for the database to query
   *
   * Can add dataStore later if required to access other datastores like ZK / mysql etc
   */
  public static DatabaseAccessRequest newInstance(String operation, String database, String key, String value) {
    DatabaseAccessRequest request = Records.newRecord(DatabaseAccessRequest.class);
    request.setOperation(operation);
    request.setDatabase(database);
    request.setKey(key);
    request.setValue(value);
    return request;
  }

  public abstract void setOperation(String operation);

  public abstract void setDatabase(String database);

  public abstract void setKey(String key);

  public abstract void setValue(String value);

  public abstract String getOperation();

  public abstract String getDatabase();

  public abstract String getKey();

  public abstract String getValue();
}
