package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

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

  private final URL url;
  private final String method;
  private final List<AbfsHttpHeader> requestHeaders;

  private int httpStatus = -1;

  private byte[] sendBuffer;

  private static AbfsApacheHttpClient abfsApacheHttpClient;

  private HttpRequestBase httpRequestBase;

  private HttpResponse httpResponse;

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
    abfsApacheHttpClientHttpOperation.httpStatus = httpStatus;
    return abfsApacheHttpClientHttpOperation;
  }

  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    httpResponse = abfsApacheHttpClient.execute(httpRequestBase);

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
