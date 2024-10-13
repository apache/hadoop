package org.apache.hadoop.fs.azurebfs.http.builtin;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpClient;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpRequestBuilder;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpResponse;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;

import java.io.IOException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Builder;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.List;

public class BuiltinAbfsHttpClient extends AbfsHttpClient {

    private final HttpClient httpClient;

    public BuiltinAbfsHttpClient(
            AbfsConfiguration abfsConfiguration) {
        super(abfsConfiguration);
        Builder httpClientBuilder = HttpClient.newBuilder();
        // TODO
        // httpClientBuilder.
        this.httpClient = httpClientBuilder.build();
    }

    @Override
    public AbfsHttpRequestBuilder createRequestBuilder(URL url, String method, List<AbfsHttpHeader> requestHeaders, BodyPublisher bodyPublisher) {
        return new BuiltinAbfsHttpRequestBuilder(url, method, requestHeaders, bodyPublisher);
    }

    @Override
    public <T> AbfsHttpResponse sendRequest(AbfsHttpRequestBuilder httpRequestBuilder, BodyHandler<T> bodyHandler) throws IOException, InterruptedException {
        BuiltinAbfsHttpRequestBuilder httpRequestBuilder2 = (BuiltinAbfsHttpRequestBuilder) httpRequestBuilder;
        HttpRequest httpRequest = httpRequestBuilder2.httpRequestBuilder.build();

        HttpResponse<T> httpResponse = httpClient.send(httpRequest, bodyHandler);

        return new BuiltinAbfsHttpResponse<T>(httpResponse);
    }

}
