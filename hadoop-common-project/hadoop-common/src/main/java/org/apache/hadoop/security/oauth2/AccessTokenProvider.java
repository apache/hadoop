package org.apache.hadoop.security.oauth2;

import com.microsoft.graph.authentication.IAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for retrieving AzureADTokens to access Azure resources.
 *
 * Implements token refresh when tokens are about to expire.
 * The token retrieval/refresh mechanism itself is implementation specific.
 */
public abstract class AccessTokenProvider implements IAuthenticationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
      AccessTokenProvider.class);

  protected AzureADToken token;

  public AccessTokenProvider() {
  }

  public synchronized AzureADToken getToken() throws IOException {
    if (this.isTokenAboutToExpire()) {
      LOG.debug("AAD Token is missing or expired: Calling refresh-token " +
          "from abstract base class");
      this.token = this.refreshToken();
    }

    return this.token;
  }

  protected abstract AzureADToken refreshToken() throws IOException;

  private boolean isTokenAboutToExpire() {
    if (this.token == null) {
      LOG.debug("AADToken: no token. Returning expiring=true.");
      return true;
    }
    Date expiry = this.token.getExpiry();

    if (expiry == null) {
      LOG.debug("AADToken: no token expiry set. Returning expiring=true.");
      return true;
    }

    Calendar c = Calendar.getInstance();
    c.add(Calendar.MILLISECOND, (int) TimeUnit.MINUTES.toMillis(5));

    boolean expiring = c.getTime().after(expiry);

    if (expiring) {
      LOG.debug("AADToken: token expiring: {} : Five-minute window: {}.",
          expiry, c.getTime());
    }

    return expiring;
  }
}
