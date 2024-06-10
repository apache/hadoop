package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import java.io.IOException;
import java.util.Objects;

import org.apache.http.HttpResponse;

/**
 * Encapsulates an exception thrown from ApacheHttpClient response parsing.
 */
public class HttpResponseException extends IOException {
  protected final HttpResponse httpResponse;
  public HttpResponseException(final String s, final HttpResponse httpResponse) {
    super(s);
    Objects.requireNonNull(httpResponse, "httpResponse should be non-null");
    this.httpResponse = httpResponse;
  }

  public HttpResponse getHttpResponse() {
    return httpResponse;
  }
}
