/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.server;

import java.io.IOException;

import javax.servlet.http.Cookie;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.text.ParseException;

import java.security.interfaces.RSAPublicKey;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.CertificateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;

/**
 * The {@link JWTRedirectAuthenticationHandler} extends
 * AltKerberosAuthenticationHandler to add WebSSO behavior for UIs. The expected
 * SSO token is a JsonWebToken (JWT). The supported algorithm is RS256 which
 * uses PKI between the token issuer and consumer. The flow requires a redirect
 * to a configured authentication server URL and a subsequent request with the
 * expected JWT token. This token is cryptographically verified and validated.
 * The user identity is then extracted from the token and used to create an
 * AuthenticationToken - as expected by the AuthenticationFilter.
 *
 * <p>
 * The supported configuration properties are:
 * </p>
 * <ul>
 * <li>authentication.provider.url: the full URL to the authentication server.
 * This is the URL that the handler will redirect the browser to in order to
 * authenticate the user. It does not have a default value.</li>
 * <li>public.key.pem: This is the PEM formatted public key of the issuer of the
 * JWT token. It is required for verifying that the issuer is a trusted party.
 * DO NOT include the PEM header and footer portions of the PEM encoded
 * certificate. It does not have a default value.</li>
 * <li>expected.jwt.audiences: This is a list of strings that identify
 * acceptable audiences for the JWT token. The audience is a way for the issuer
 * to indicate what entity/s that the token is intended for. Default value is
 * null which indicates that all audiences will be accepted.</li>
 * <li>jwt.cookie.name: the name of the cookie that contains the JWT token.
 * Default value is "hadoop-jwt".</li>
 * </ul>
 */
public class JWTRedirectAuthenticationHandler extends
    AltKerberosAuthenticationHandler {
  private static Logger LOG = LoggerFactory
      .getLogger(JWTRedirectAuthenticationHandler.class);

  public static final String AUTHENTICATION_PROVIDER_URL =
      "authentication.provider.url";
  public static final String PUBLIC_KEY_PEM = "public.key.pem";
  public static final String EXPECTED_JWT_AUDIENCES = "expected.jwt.audiences";
  public static final String JWT_COOKIE_NAME = "jwt.cookie.name";
  private static final String ORIGINAL_URL_QUERY_PARAM = "originalUrl=";
  private String authenticationProviderUrl = null;
  private RSAPublicKey publicKey = null;
  private List<String> audiences = null;
  private String cookieName = "hadoop-jwt";

  /**
   * Primarily for testing, this provides a way to set the publicKey for
   * signature verification without needing to get a PEM encoded value.
   *
   * @param pk publicKey for the token signtature verification
   */
  public void setPublicKey(RSAPublicKey pk) {
    publicKey = pk;
  }

  /**
   * Initializes the authentication handler instance.
   * <p>
   * This method is invoked by the {@link AuthenticationFilter#init} method.
   * </p>
   * @param config
   *          configuration properties to initialize the handler.
   *
   * @throws ServletException
   *           thrown if the handler could not be initialized.
   */
  @Override
  public void init(Properties config) throws ServletException {
    super.init(config);
    // setup the URL to redirect to for authentication
    authenticationProviderUrl = config
        .getProperty(AUTHENTICATION_PROVIDER_URL);
    if (authenticationProviderUrl == null) {
      throw new ServletException(
          "Authentication provider URL must not be null - configure: "
              + AUTHENTICATION_PROVIDER_URL);
    }

    // setup the public key of the token issuer for verification
    if (publicKey == null) {
      String pemPublicKey = config.getProperty(PUBLIC_KEY_PEM);
      if (pemPublicKey == null) {
        throw new ServletException(
            "Public key for signature validation must be provisioned.");
      }
      publicKey = CertificateUtil.parseRSAPublicKey(pemPublicKey);
    }
    // setup the list of valid audiences for token validation
    String auds = config.getProperty(EXPECTED_JWT_AUDIENCES);
    if (auds != null) {
      // parse into the list
      String[] audArray = auds.split(",");
      audiences = new ArrayList<String>();
      for (String a : audArray) {
        audiences.add(a);
      }
    }

    // setup custom cookie name if configured
    String customCookieName = config.getProperty(JWT_COOKIE_NAME);
    if (customCookieName != null) {
      cookieName = customCookieName;
    }
  }

  @Override
  public AuthenticationToken alternateAuthenticate(HttpServletRequest request,
      HttpServletResponse response) throws IOException,
      AuthenticationException {
    AuthenticationToken token = null;

    String serializedJWT = null;
    HttpServletRequest req = (HttpServletRequest) request;
    serializedJWT = getJWTFromCookie(req);
    if (serializedJWT == null) {
      String loginURL = constructLoginURL(request);
      LOG.info("sending redirect to: " + loginURL);
      ((HttpServletResponse) response).sendRedirect(loginURL);
    } else {
      String userName = null;
      SignedJWT jwtToken = null;
      boolean valid = false;
      try {
        jwtToken = SignedJWT.parse(serializedJWT);
        valid = validateToken(jwtToken);
        if (valid) {
          userName = jwtToken.getJWTClaimsSet().getSubject();
          LOG.info("USERNAME: " + userName);
        } else {
          LOG.warn("jwtToken failed validation: " + jwtToken.serialize());
        }
      } catch(ParseException pe) {
        // unable to parse the token let's try and get another one
        LOG.warn("Unable to parse the JWT token", pe);
      }
      if (valid) {
        LOG.debug("Issuing AuthenticationToken for user.");
        token = new AuthenticationToken(userName, userName, getType());
      } else {
        String loginURL = constructLoginURL(request);
        LOG.info("token validation failed - sending redirect to: " + loginURL);
        ((HttpServletResponse) response).sendRedirect(loginURL);
      }
    }
    return token;
  }

  /**
   * Encapsulate the acquisition of the JWT token from HTTP cookies within the
   * request.
   *
   * @param req servlet request to get the JWT token from
   * @return serialized JWT token
   */
  protected String getJWTFromCookie(HttpServletRequest req) {
    String serializedJWT = null;
    Cookie[] cookies = req.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          LOG.info(cookieName
              + " cookie has been found and is being processed");
          serializedJWT = cookie.getValue();
          break;
        }
      }
    }
    return serializedJWT;
  }

  /**
   * Create the URL to be used for authentication of the user in the absence of
   * a JWT token within the incoming request.
   *
   * @param request for getting the original request URL
   * @return url to use as login url for redirect
   */
  @VisibleForTesting
  String constructLoginURL(HttpServletRequest request) {
    String delimiter = "?";
    if (authenticationProviderUrl.contains("?")) {
      delimiter = "&";
    }
    String loginURL = authenticationProviderUrl + delimiter
        + ORIGINAL_URL_QUERY_PARAM
        + request.getRequestURL().toString() + getOriginalQueryString(request);
    return loginURL;
  }

  private String getOriginalQueryString(HttpServletRequest request) {
    String originalQueryString = request.getQueryString();
    return (originalQueryString == null) ? "" : "?" + originalQueryString;
  }

  /**
   * This method provides a single method for validating the JWT for use in
   * request processing. It provides for the override of specific aspects of
   * this implementation through submethods used within but also allows for the
   * override of the entire token validation algorithm.
   *
   * @param jwtToken the token to validate
   * @return true if valid
   */
  protected boolean validateToken(SignedJWT jwtToken) {
    boolean sigValid = validateSignature(jwtToken);
    if (!sigValid) {
      LOG.warn("Signature could not be verified");
    }
    boolean audValid = validateAudiences(jwtToken);
    if (!audValid) {
      LOG.warn("Audience validation failed.");
    }
    boolean expValid = validateExpiration(jwtToken);
    if (!expValid) {
      LOG.info("Expiration validation failed.");
    }

    return sigValid && audValid && expValid;
  }

  /**
   * Verify the signature of the JWT token in this method. This method depends
   * on the public key that was established during init based upon the
   * provisioned public key. Override this method in subclasses in order to
   * customize the signature verification behavior.
   *
   * @param jwtToken the token that contains the signature to be validated
   * @return valid true if signature verifies successfully; false otherwise
   */
  protected boolean validateSignature(SignedJWT jwtToken) {
    boolean valid = false;
    if (JWSObject.State.SIGNED == jwtToken.getState()) {
      LOG.debug("JWT token is in a SIGNED state");
      if (jwtToken.getSignature() != null) {
        LOG.debug("JWT token signature is not null");
        try {
          JWSVerifier verifier = new RSASSAVerifier(publicKey);
          if (jwtToken.verify(verifier)) {
            valid = true;
            LOG.debug("JWT token has been successfully verified");
          } else {
            LOG.warn("JWT signature verification failed.");
          }
        } catch (JOSEException je) {
          LOG.warn("Error while validating signature", je);
        }
      }
    }
    return valid;
  }

  /**
   * Validate whether any of the accepted audience claims is present in the
   * issued token claims list for audience. Override this method in subclasses
   * in order to customize the audience validation behavior.
   *
   * @param jwtToken
   *          the JWT token where the allowed audiences will be found
   * @return true if an expected audience is present, otherwise false
   */
  protected boolean validateAudiences(SignedJWT jwtToken) {
    boolean valid = false;
    try {
      List<String> tokenAudienceList = jwtToken.getJWTClaimsSet()
          .getAudience();
      // if there were no expected audiences configured then just
      // consider any audience acceptable
      if (audiences == null) {
        valid = true;
      } else {
        // if any of the configured audiences is found then consider it
        // acceptable
        boolean found = false;
        for (String aud : tokenAudienceList) {
          if (audiences.contains(aud)) {
            LOG.debug("JWT token audience has been successfully validated");
            valid = true;
            break;
          }
        }
        if (!valid) {
          LOG.warn("JWT audience validation failed.");
        }
      }
    } catch (ParseException pe) {
      LOG.warn("Unable to parse the JWT token.", pe);
    }
    return valid;
  }

  /**
   * Validate that the expiration time of the JWT token has not been violated.
   * If it has then throw an AuthenticationException. Override this method in
   * subclasses in order to customize the expiration validation behavior.
   *
   * @param jwtToken the token that contains the expiration date to validate
   * @return valid true if the token has not expired; false otherwise
   */
  protected boolean validateExpiration(SignedJWT jwtToken) {
    boolean valid = false;
    try {
      Date expires = jwtToken.getJWTClaimsSet().getExpirationTime();
      if (expires == null || new Date().before(expires)) {
        LOG.debug("JWT token expiration date has been "
            + "successfully validated");
        valid = true;
      } else {
        LOG.warn("JWT expiration date validation failed.");
      }
    } catch (ParseException pe) {
      LOG.warn("JWT expiration date validation failed.", pe);
    }
    return valid;
  }
}
