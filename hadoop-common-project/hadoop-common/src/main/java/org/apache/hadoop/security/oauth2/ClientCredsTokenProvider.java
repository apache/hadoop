package org.apache.hadoop.security.oauth2;

import com.microsoft.graph.http.IHttpRequest;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of AccessTokenProvider that uses client (application)
 * credentials to request tokens from Azure AD.
 */
public class ClientCredsTokenProvider extends AccessTokenProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
      ClientCredsTokenProvider.class);

  private final String authEndpoint;
  private final String clientId;
  private final String clientSecret;
  private final String grantType;
  private final String resource;

  public ClientCredsTokenProvider(
      String authEndpoint, String clientId, String clientSecret,
      String grantType, String resource) {
    this.authEndpoint = authEndpoint;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.grantType = grantType;
    this.resource = resource;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing client-credential based token");
    return AzureADAuthenticator.getTokenUsingClientCreds(authEndpoint,
        clientId, clientSecret, grantType, resource);
  }

  @Override // IAuthenticationProvider
  public void authenticateRequest(IHttpRequest iHttpRequest) {
    AzureADToken token;
    try {
      token = getToken();
    } catch (IOException e) {
      LOG.error("Couldn't get token, skipping authenticating: {}",
          iHttpRequest.getRequestUrl(), e);
      return;
    }
    iHttpRequest.addHeader(HttpHeader.AUTHORIZATION.asString(),
        "Bearer " + token.getAccessToken());
  }
}
