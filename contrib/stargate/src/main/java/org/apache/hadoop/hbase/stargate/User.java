package org.apache.hadoop.hbase.stargate;

import java.security.MessageDigest;

import org.apache.hadoop.hbase.util.Bytes;

/** Representation of an authorized user */
public class User implements Constants {

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
  public void setName(final String name) {
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
  public void setToken(final String token) {
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
  public void setAdmin(final boolean admin) {
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (admin ? 1231 : 1237);
    result = prime * result + (disabled ? 1231 : 1237);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((token == null) ? 0 : token.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    User other = (User) obj;
    if (admin != other.admin)
      return false;
    if (disabled != other.disabled)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (token == null) {
      if (other.token != null)
        return false;
    } else if (!token.equals(other.token))
      return false;
    return true;
  }

}
