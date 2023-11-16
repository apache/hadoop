package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

public class AbfsAHCHttpOperation extends HttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsAHCHttpOperation.class);

  private final URL url;
  private final String method;
  private final List<AbfsHttpHeader> requestHeaders;

  private byte[] sendBuffer;

  private static AbfsApacheHttpClient abfsApacheHttpClient;

  private HttpRequestBase httpRequestBase;

  private HttpResponse httpResponse;

  private static final int CONNECT_TIMEOUT = 30 * 1000;
  private static final int READ_TIMEOUT = 30 * 1000;

  private static final int CLEAN_UP_BUFFER_SIZE = 64 * 1024;

  private static final int ONE_THOUSAND = 1000;
  private static final int ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND;

  private String maskedUrl;
  private String maskedEncodedUrl;
  private int statusCode;
  private String statusDescription;
  private String storageErrorCode = "";
  private String storageErrorMessage  = "";
  private String requestId  = "";
  private String expectedAppendPos = "";
  private ListResultSchema listResultSchema = null;

  // metrics
  private int bytesSent;
  private int expectedBytesToBeSent;
  private long bytesReceived;

  private long connectionTimeMs;
  private long sendRequestTimeMs;
  private long recvResponseTimeMs;
  private boolean shouldMask = false;;

  public synchronized  static void setAbfsApacheHttpClient(
      DelegatingSSLSocketFactory delegatingSSLSocketFactory) {
    if(abfsApacheHttpClient == null) {
      abfsApacheHttpClient = new AbfsApacheHttpClient(delegatingSSLSocketFactory);
    }
  }


  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders) {
    super(LOG);
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
  }


  public static AbfsAHCHttpOperation getAbfsApacheHttpClientHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsAHCHttpOperation abfsApacheHttpClientHttpOperation = new AbfsAHCHttpOperation(url, method, new ArrayList<>());
    abfsApacheHttpClientHttpOperation.statusCode = httpStatus;
    return abfsApacheHttpClientHttpOperation;
  }

  @Override
  protected InputStream getErrorStream() throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if(entity == null) {
      return null;
    }
    return entity.getContent();
  }

  @Override
  String getConnProperty(final String key) {
    return null;
  }

  @Override
  URL getConnUrl() {
    return null;
  }

  @Override
  String getConnRequestMethod() {
    return null;
  }

  @Override
  Integer getConnResponseCode() throws IOException {
    return null;
  }

  @Override
  String getConnResponseMessage() throws IOException {
    return null;
  }

  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    httpResponse = abfsApacheHttpClient.execute(httpRequestBase);
    // get the response
    long startTime = 0;
    startTime = System.nanoTime();

    this.statusCode = httpResponse.getStatusLine().getStatusCode();
    this.recvResponseTimeMs = 0L;

    this.statusDescription = httpResponse.getStatusLine().getReasonPhrase();

    this.requestId = getHeaderValue(HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (this.requestId == null) {
      this.requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    // dump the headers
//    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
//        connection.getHeaderFields());

    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(this.method)) {
      // If it is HEAD, and it is ERROR
      return;
    }

    startTime = System.nanoTime();

    if (statusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
      processStorageErrorResponse();
//      this.recvResponseTimeMs += elapsedTimeMs(startTime);
      String contentLength = getHeaderValue(
          HttpHeaderConfigurations.CONTENT_LENGTH);
      if(contentLength != null) {
        this.bytesReceived = Long.parseLong(contentLength);
      } else {
        this.bytesReceived = 0L;
      }

    } else {
      // consume the input stream to release resources
      int totalBytesRead = 0;

      try (InputStream stream = getContentInputStream(httpResponse)) {
        if (isNullInputStream(stream)) {
          return;
        }
        boolean endOfStream = false;

        // this is a list operation and need to retrieve the data
        // need a better solution
        if (AbfsHttpConstants.HTTP_METHOD_GET.equals(this.method) && buffer == null) {
          parseListFilesResponse(stream);
        } else {
          if (buffer != null) {
            while (totalBytesRead < length) {
              int bytesRead = stream.read(buffer, offset + totalBytesRead, length - totalBytesRead);
              if (bytesRead == -1) {
                endOfStream = true;
                break;
              }
              totalBytesRead += bytesRead;
            }
          }
          if (!endOfStream && stream.read() != -1) {
            // read and discard
            int bytesRead = 0;
            byte[] b = new byte[CLEAN_UP_BUFFER_SIZE];
            while ((bytesRead = stream.read(b)) >= 0) {
              totalBytesRead += bytesRead;
            }
          }
        }
      } catch (IOException ex) {
//        LOG.warn("IO/Network error: {} {}: {}",
//            method, getMaskedUrl(), ex.getMessage());
        LOG.debug("IO Error: ", ex);
        throw ex;
      } finally {
//        this.recvResponseTimeMs += elapsedTimeMs(startTime);
        this.bytesReceived = totalBytesRead;
      }
    }
  }

  @Override
  public void setRequestProperty(final String key, final String value) {

  }

  public String getHeaderValue(final String headerName) {
    Header header = httpResponse.getFirstHeader(headerName);
    if(header != null) {
      return header.getValue();
    }
    return null;
  }



  private InputStream getContentInputStream(final HttpResponse httpResponse)
      throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if(entity != null) {
      return httpResponse.getEntity().getContent();
    }
    return null;
  }

  /**
   * Check null stream, this is to pass findbugs's redundant check for NULL
   * @param stream InputStream
   */
  private boolean isNullInputStream(InputStream stream) {
    return stream == null ? true : false;
  }

  public void sendRequest(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    try {
      HttpRequestBase httpRequestBase = null;
      if (HTTP_METHOD_PUT.equals(method)) {
        httpRequestBase = new HttpPut(url.toURI());
      }
      if(HTTP_METHOD_PATCH.equals(method)) {
        httpRequestBase = new HttpPatch(url.toURI());
      }
      if(HTTP_METHOD_POST.equals(method)) {
        httpRequestBase = new HttpPost(url.toURI());
      }
      if(httpRequestBase != null) {

        this.expectedBytesToBeSent = length;
        this.bytesSent = length;
        if(buffer != null) {
          HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length,
              TEXT_PLAIN);
          ((HttpEntityEnclosingRequestBase)httpRequestBase).setEntity(httpEntity);
        }
      } else {
        if(HTTP_METHOD_GET.equals(method)) {
          httpRequestBase = new HttpGet(url.toURI());
        }
        if(HTTP_METHOD_DELETE.equals(method)) {
          httpRequestBase = new HttpDelete((url.toURI()));
        }
        if(HTTP_METHOD_HEAD.equals(method)) {
          httpRequestBase = new HttpHead(url.toURI());
        }
      }
      translateHeaders(httpRequestBase, requestHeaders);
      this.httpRequestBase = httpRequestBase;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void translateHeaders(final HttpRequestBase httpRequestBase, final List<AbfsHttpHeader> requestHeaders) {
    for(AbfsHttpHeader header : requestHeaders) {
      httpRequestBase.setHeader(header.getName(), header.getValue());
    }
  }

  public void setHeader(String name, String val) {
    requestHeaders.add(new AbfsHttpHeader(name, val));
  }

  public URL getURL() {
    return url;
  }

  public Map<String, List<String>> getRequestHeaders() {
    Map<String, List<String>> map = new HashMap<>();
    for(AbfsHttpHeader header : requestHeaders) {
      map.put(header.getName(), new ArrayList<String>(){{add(header.getValue());}});
    }
    return map;
  }

  public String getRequestHeader(String name) {
    for(AbfsHttpHeader header : requestHeaders) {
      if(header.getName().equals(name)) {
        return header.getValue();
      }
    }
    return "";
  }

  public String getClientRequestId() {
    for(AbfsHttpHeader header : requestHeaders) {
      if(X_MS_CLIENT_REQUEST_ID.equals(header.getName())) {
        return header.getValue();
      }
    }
    return "";
  }

  @Override
  public String getResponseHeader(final String httpHeader) {
    return null;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(statusCode);
    sb.append(",");
    sb.append(storageErrorCode);
    sb.append(",");
    sb.append(expectedAppendPos);
    sb.append(",cid=");
    sb.append(getClientRequestId());
    sb.append(",rid=");
    sb.append(requestId);
    sb.append(",connMs=");
    sb.append(connectionTimeMs);
    sb.append(",sendMs=");
    sb.append(sendRequestTimeMs);
    sb.append(",recvMs=");
    sb.append(recvResponseTimeMs);
    sb.append(",sent=");
    sb.append(bytesSent);
    sb.append(",recv=");
    sb.append(bytesReceived);
    sb.append(",");
    sb.append(method);
    sb.append(",");
    sb.append(getMaskedUrl());
    return sb.toString();
  }
}
