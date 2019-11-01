package org.apache.hadoop.security.oauth2;

import com.fasterxml.jackson.core.JsonFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for constructing token requests to Azure AD.
 */
public class AzureADAuthenticator {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureADAuthenticator.class.getName());

  public static AzureADToken getTokenUsingClientCreds(
      String authEndpoint, String clientId, String clientSecret,
      String grantType, String resource)
      throws IOException {
    QueryParams qp = new QueryParams();
    qp.add("resource", resource);
    qp.add("grant_type", grantType);
    qp.add("client_id", clientId);
    qp.add("client_secret", clientSecret);
    LOG.debug("AADToken: starting to fetch token using client credentials " +
        "for client ID: {}.", clientId);
    return getToken(authEndpoint, qp.serialize());
  }

  private static AzureADToken getToken(String authEndpoint, String body)
      throws IOException {
    AzureADToken token = null;
    RetryPolicy retryPolicy = new ExponentialBackoffPolicy(3, 1000, 2);

    IOException lastException;
    boolean succeeded;
    int httperror;

    do {
      httperror = 0;
      lastException = null;

      try {
        token = getTokenImpl(authEndpoint, body);
      } catch (HttpException httpException) {
        lastException = httpException;
        httperror = httpException.httpErrorCode;
      } catch (IOException e) {
        lastException = e;
      }

      succeeded = lastException == null;
    } while (!succeeded && retryPolicy.shouldRetry(httperror, lastException));

    if (!succeeded) {
      throw lastException;
    }

    return token;
  }


  private static AzureADToken getTokenImpl(
      String authEndpoint, String payload) throws IOException {
    AzureADToken token;
    HttpURLConnection conn = null;

    try {
      URL url = new URL(authEndpoint);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(HttpMethod.POST.asString());
      conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(30));
      conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(30));
      String requestId;

      conn.setRequestProperty(HttpHeader.CONNECTION.asString(), "close");
      conn.setDoOutput(true);
      conn.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));

      int httpResponseCode = conn.getResponseCode();
      requestId = conn.getHeaderField("x-ms-request-id");
      String responseContentType = conn.getHeaderField(
          HttpHeader.CONTENT_TYPE.asString());
      long responseContentLength = conn.getHeaderFieldLong(
          HttpHeader.CONTENT_LENGTH.asString(), 0L);
      requestId = requestId == null ? "" : requestId;

      if (httpResponseCode != HttpStatus.OK_200 ||
          !responseContentType.startsWith(
              MimeTypes.Type.APPLICATION_JSON.asString()) ||
          responseContentLength <= 0L) {
        String responseBody = consumeInputStream(conn.getInputStream());
        String proxies = "none";
        String httpProxy = System.getProperty("http.proxy");
        String httpsProxy = System.getProperty("https.proxy");
        if (httpProxy != null || httpsProxy != null) {
          proxies = "http:" + httpProxy + ";https:" + httpsProxy;
        }

        String logMessage = String.format("AADToken: HTTP connection failed " +
                "for getting token from AzureAD. Http response: %d %s " +
                ", Content-Type: %s, Content-Length: %d, Request ID: %s" +
                ", Proxies: %s, First 1K of Body: %s.",
            httpResponseCode, conn.getResponseMessage(), responseContentType,
            responseContentLength, requestId, proxies, responseBody);
        LOG.debug(logMessage);
        throw new HttpException(httpResponseCode, requestId, logMessage);
      }

      InputStream httpResponseStream = conn.getInputStream();
      token = parseTokenFromStream(httpResponseStream);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }

    return token;
  }

  private static String consumeInputStream(InputStream inStream)
      throws IOException {
    final int bufferSize = 1024;

    int totalBytesRead = 0;
    int bytesRead;
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    do {
      byte[] b = new byte[bufferSize];
      bytesRead = inStream.read(b, totalBytesRead, bufferSize);
      if (bytesRead > 0) {
        totalBytesRead += bytesRead;
        bytes.write(b);
      }
    } while (bytesRead > 0);

    return new String(bytes.toByteArray(), 0, totalBytesRead);
  }

  private static AzureADToken parseTokenFromStream(
      InputStream httpResponseStream) throws IOException {
    AzureADToken token;

    try {
      Integer expiryPeriod = null;
      String accessToken = null;

      JsonFactory jf = new JsonFactory();
      JsonParser jp = jf.createParser(httpResponseStream);
      jp.nextToken();

      for(; jp.hasCurrentToken(); jp.nextToken()) {
        if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
          String fieldName = jp.getCurrentName();
          jp.nextToken();
          String fieldValue = jp.getText();
          if (fieldName.equals("access_token")) {
            accessToken = fieldValue;
          }

          if (fieldName.equals("expires_in")) {
            expiryPeriod = Integer.parseInt(fieldValue);
          }
        }
      }

      if (accessToken == null) {
        throw new IOException("Could not retrieve the access token");
      }

      if (expiryPeriod == null) {
        throw new IOException("Could not retrieve the expiry period");
      }

      jp.close();
      long expiry = System.currentTimeMillis();
      expiry += expiryPeriod * 1000L;

      token = new AzureADToken(accessToken, new Date(expiry));
      LOG.debug("AADToken: fetched token with expiry {}.", token.getExpiry());
    } catch (Exception e) {
      LOG.warn("AADToken: got exception when parsing json token {}.",
          e.getMessage());
      throw e;
    } finally {
      httpResponseStream.close();
    }

    return token;
  }

  private static class HttpException extends IOException {
    int httpErrorCode;
    String requestId;

    HttpException(int httpErrorCode, String requestId, String message) {
      super(message);
      this.httpErrorCode = httpErrorCode;
      this.requestId = requestId;
    }
  }
}
