package org.apache.hadoop.hbase.stargate.auth;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HTableAuthenticator extends Authenticator {

  static final byte[] USER = Bytes.toBytes("user");
  static final byte[] NAME = Bytes.toBytes("name");
  static final byte[] ADMIN = Bytes.toBytes("admin");
  static final byte[] DISABLED = Bytes.toBytes("disabled");

  HBaseConfiguration conf;
  String tableName;
  HTable table;

  /**
   * Default constructor
   */
  public HTableAuthenticator() {
    this(new HBaseConfiguration());
  }

  /**
   * Constructor
   * @param conf
   */
  public HTableAuthenticator(HBaseConfiguration conf) {
    this.conf = conf;
    this.tableName = conf.get("stargate.auth.htable.name",
      "stargate.users");
  }

  /**
   * Constructor
   * @param conf
   * @param tableName
   */
  public HTableAuthenticator(HBaseConfiguration conf, String tableName) {
    this.conf = conf;
    this.tableName = tableName;
  }

  /**
   * Constructor
   * @param conf
   * @param table
   */
  public HTableAuthenticator(HBaseConfiguration conf, HTable table) {
    this.conf = conf;
    this.table = table;
    this.tableName = Bytes.toString(table.getTableName());
  }

  @Override
  public User getUserForToken(String token) throws IOException {
    if (table == null) {
      this.table = new HTable(conf, tableName);
    }
    Get get = new Get(Bytes.toBytes(token));
    get.addColumn(USER, NAME);
    get.addColumn(USER, ADMIN);
    get.addColumn(USER, DISABLED);
    Result result = table.get(get);
    byte[] value = result.getValue(USER, NAME);
    if (value == null) {
      return null;
    }
    String name = Bytes.toString(value);
    boolean admin = false;
    value = result.getValue(USER, ADMIN);
    if (value != null) {
      admin = Bytes.toBoolean(value);
    }
    boolean disabled = false;
    value = result.getValue(USER, DISABLED);
    if (value != null) {
      disabled = Bytes.toBoolean(value);
    }
    return new User(name, token, admin, disabled);
  }

}
