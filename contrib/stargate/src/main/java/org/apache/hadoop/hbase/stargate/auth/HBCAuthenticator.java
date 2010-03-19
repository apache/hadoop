package org.apache.hadoop.hbase.stargate.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.stargate.User;

public class HBCAuthenticator extends Authenticator {

  Configuration conf;

  /**
   * Default constructor
   */
  public HBCAuthenticator() {
    this(HBaseConfiguration.create());
  }

  /**
   * Constructor
   * @param conf
   */
  public HBCAuthenticator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public User getUserForToken(String token) {
    String name = conf.get("stargate.auth.token." + token);
    if (name == null) {
      return null;
    }
    boolean admin = conf.getBoolean("stargate.auth.user." + name + ".admin",
      false);
    boolean disabled = conf.getBoolean("stargate.auth.user." + name + ".disabled",
      false);
    return new User(name, token, admin, disabled);
  }

}
