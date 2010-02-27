package org.apache.hadoop.hbase.stargate.auth;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

public class JDBCAuthenticator extends Authenticator {

  static final int MAX_RETRIES = 5;
  static final long RETRY_SLEEP_TIME = 1000 * 2;

  String url;
  String table;
  String user;
  String password;
  Connection connection;
  PreparedStatement userFetchStmt;

  /**
   * Constructor
   * @param conf
   */
  public JDBCAuthenticator(HBaseConfiguration conf) {
    this(conf.get("stargate.auth.jdbc.url"),
      conf.get("stargate.auth.jdbc.table"),
      conf.get("stargate.auth.jdbc.user"),
      conf.get("stargate.auth.jdbc.password"));
  }

  /**
   * Constructor
   * @param url
   * @param table
   * @param user
   * @param password
   */
  public JDBCAuthenticator(String url, String table, String user,
      String password) {
    this.url = url;
    this.table = table;
    this.user = user;
    this.password = password;
  }

  @Override
  public User getUserForToken(String token) throws IOException {
    int retries = 0;
    while (true)  try {
      if (connection == null) {
        connection = DriverManager.getConnection(url, user, password);
        userFetchStmt = connection.prepareStatement(
          "SELECT name, admin, disabled FROM " + table + " WHERE token = ?");
      }
      ResultSet results;
      synchronized (userFetchStmt) {
        userFetchStmt.setString(1, token);
        results = userFetchStmt.executeQuery();
      }
      if (!results.next()) {
        return null;
      }
      return new User(results.getString(1), token, results.getBoolean(2),
        results.getBoolean(3));
    } catch (SQLException e) {
      connection = null;
      if (++retries > MAX_RETRIES) {
        throw new IOException(e);
      } else try {
        Log.warn(StringUtils.stringifyException(e));
        Thread.sleep(RETRY_SLEEP_TIME);
      } catch (InterruptedException ex) {
        // ignore
      }
    }
  }

}
