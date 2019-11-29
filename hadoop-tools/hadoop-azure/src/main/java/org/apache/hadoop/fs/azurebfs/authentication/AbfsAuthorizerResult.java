package org.apache.hadoop.fs.azurebfs.authentication;

public class AbfsAuthorizerResult {
  public String getSASToken() {
    return SASToken;
  }

  public void setSASToken(String SASToken) {
    this.SASToken = SASToken;
  }

  public boolean isAuthorized() {
    return isAuthorized;
  }

  public void setAuthorized(boolean authorized) {
    isAuthorized = authorized;
  }

  public boolean isSasTokenAvailable() {
    return sasTokenAvailable;
  }

  public void setSasTokenAvailable(boolean sasTokenAvailable) {
    this.sasTokenAvailable = sasTokenAvailable;
  }

  String SASToken = null;
  boolean isAuthorized = false;
  boolean sasTokenAvailable = false;

  public boolean isAuthorizationStatusFetched() {
    return authorizationStatusFetched;
  }

  public void setAuthorizationStatusFetched(
      boolean authorizationStatusFetched) {
    this.authorizationStatusFetched = authorizationStatusFetched;
  }

  boolean authorizationStatusFetched = false;


}
