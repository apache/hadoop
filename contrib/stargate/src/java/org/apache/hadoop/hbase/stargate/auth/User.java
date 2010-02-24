package org.apache.hadoop.hbase.stargate.auth;

import java.security.MessageDigest;

import org.apache.hadoop.hbase.util.Bytes;

/** Representation of an authorized user */
public class User {

  public static final User DEFAULT_USER = new User("default",
    "00000000000000000000000000000000", false, true);

  private String name;
  private String token;
  private boolean admin;
  private boolean disabled = false;

  /**
   * Constructor
   * <p>
   * Creates an access token. (Normally, you don't want this.)
   * @param name user name
   * @param admin true if user has administrator privilege
   * @throws Exception 
   */
  public User(String name, boolean admin) throws Exception {
    this.name = name;
    this.admin = admin;
    byte[] digest = MessageDigest.getInstance("MD5")
      .digest(Bytes.toBytes(name));
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < digest.length; i++) {
      sb.append(Integer.toHexString(0xff & digest[i]));
    }
    this.token = sb.toString();
  }

  /**
   * Constructor
   * @param name user name
   * @param token access token, a 16 char hex string
   * @param admin true if user has administrator privilege
   */
  public User(String name, String token, boolean admin) {
    this(name, token, admin, false);
  }

  /**
   * Constructor
   * @param name user name
   * @param token access token, a 16 char hex string
   * @param admin true if user has administrator privilege
   * @param disabled true if user is disabled
   */
  public User(String name, String token, boolean admin, boolean disabled) {
    this.name = name;
    this.token = token;
    this.admin = admin;
    this.disabled = disabled;
  }

  /**
   * @return user name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name user name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return access token, a 16 char hex string
   */
  public String getToken() {
    return token;
  }

  /**
   * @param token access token, a 16 char hex string
   */
  public void setToken(String token) {
    this.token = token;
  }

  /**
   * @return true if user has administrator privilege
   */
  public boolean isAdmin() {
    return admin;
  }

  /**
   * @param admin true if user has administrator privilege
   */
  public void setAdmin(boolean admin) {
    this.admin = admin;
  }

  /**
   * @return true if user is disabled
   */
  public boolean isDisabled() {
    return disabled;
  }

  /**
   * @param admin true if user is disabled
   */
  public void setDisabled(boolean disabled) {
    this.disabled = disabled;
  }

}
