package org.apache.hadoop.security.oauth2;

import org.apache.hadoop.util.Time;

import java.util.Date;

/**
 * Class representing an Azure AD token used to access resources in Azure.
 */
public class AzureADToken {

  private final String accessToken;
  private final Date expiry;

  public AzureADToken(String accessToken, Date expiry) {
    this.accessToken = accessToken;
    this.expiry = expiry;
  }

  /**
   * Return the token payload.
   * @return token payload.
   */
  public String getAccessToken() {
    return accessToken;
  }

  /**
   * Return the expiry date of this token.
   * @return expiry date.
   */
  public Date getExpiry() {
    return expiry;
  }

  /**
   * Check whether the token is expired.
   * @return true if the token has expired.
   */
  public boolean isExpired() {
    return expiry != null && expiry.getTime() < Time.now();
  }

  @Override
  public String toString() {
    return "[AzureADToken, expires: " + expiry + "]";
  }
}
