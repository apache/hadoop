package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import java.io.IOException;

import org.apache.http.HttpResponse;

public class AbfsApacheHttpExpect100Exception extends IOException {
  private final HttpResponse httpResponse;

  public AbfsApacheHttpExpect100Exception(final String s, final HttpResponse httpResponse) {
    super(s);
    this.httpResponse = httpResponse;
  }

  public HttpResponse getHttpResponse() {
    return httpResponse;
  }
}
