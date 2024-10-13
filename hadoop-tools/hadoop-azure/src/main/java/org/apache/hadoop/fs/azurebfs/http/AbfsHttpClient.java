package org.apache.hadoop.fs.azurebfs.http;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;

import java.io.IOException;
import java.net.URL;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse.BodyHandler;
import java.util.List;

public abstract class AbfsHttpClient {

    public AbfsHttpClient(AbfsConfiguration abfsConfiguration) {
    }

    public abstract AbfsHttpRequestBuilder createRequestBuilder(
            URL url, String method, List<AbfsHttpHeader> requestHeaders,
            BodyPublisher bodyPublisher);

    public abstract <T> AbfsHttpResponse sendRequest(
            AbfsHttpRequestBuilder httpRequestBuilder,
            BodyHandler<T> bodyHandler) throws IOException, InterruptedException;

}
