package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

public class AbfsApacheHttpClientHttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsHttpOperation.class);

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


  public AbfsApacheHttpClientHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders) {
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
  }


  public static AbfsApacheHttpClientHttpOperation getAbfsApacheHttpClientHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsApacheHttpClientHttpOperation abfsApacheHttpClientHttpOperation = new AbfsApacheHttpClientHttpOperation(url, method, new ArrayList<>());
    abfsApacheHttpClientHttpOperation.statusCode = httpStatus;
    return abfsApacheHttpClientHttpOperation;
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
      processStorageErrorResponse(httpResponse);
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

  private String getHeaderValue(final String headerName) {
    Header header = httpResponse.getFirstHeader(headerName);
    if(header != null) {
      return header.getValue();
    }
    return null;
  }

  /**
   * Parse the list file response
   *
   * @param stream InputStream contains the list results.
   * @throws IOException
   */
  private void parseListFilesResponse(final InputStream stream) throws IOException {
    if (stream == null) {
      return;
    }

    if (listResultSchema != null) {
      // already parse the response
      return;
    }

    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      this.listResultSchema = objectMapper.readValue(stream, ListResultSchema.class);
    } catch (IOException ex) {
      LOG.error("Unable to deserialize list results", ex);
      throw ex;
    }
  }

  private void processStorageErrorResponse(HttpResponse httpResponse) {
    try (InputStream stream = getContentInputStream(httpResponse)) {
      if (stream == null) {
        return;
      }
      JsonFactory jf = new JsonFactory();
      try (JsonParser jp = jf.createParser(stream)) {
        String fieldName, fieldValue;
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();  // FIELD_NAME - "error":
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();
        while (jp.hasCurrentToken()) {
          if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
            fieldName = jp.getCurrentName();
            jp.nextToken();
            fieldValue = jp.getText();
            switch (fieldName) {
            case "code":
              storageErrorCode = fieldValue;
              break;
            case "message":
              storageErrorMessage = fieldValue;
              break;
            case "ExpectedAppendPos":
              expectedAppendPos = fieldValue;
              break;
            default:
              break;
            }
          }
          jp.nextToken();
        }
      }
    } catch (IOException ex) {
      // Ignore errors that occur while attempting to parse the storage
      // error, since the response may have been handled by the HTTP driver
      // or for other reasons have an unexpected
      LOG.debug("ExpectedError: ", ex);
    }
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
      HttpEntityEnclosingRequestBase httpRequestBase = null;
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
        translateHeaders(httpRequestBase, requestHeaders);
        HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length, TEXT_PLAIN);
      }
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
}
