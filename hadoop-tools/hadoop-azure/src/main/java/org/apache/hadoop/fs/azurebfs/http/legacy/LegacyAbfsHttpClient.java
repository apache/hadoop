package org.apache.hadoop.fs.azurebfs.http.legacy;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpClient;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpRequestBuilder;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;

import java.net.URL;
import java.net.http.HttpRequest.BodyPublisher;
import java.util.List;

public class LegacyAbfsHttpClient extends AbfsHttpClient {

    public LegacyAbfsHttpClient(AbfsConfiguration abfsConfiguration) {
        super(abfsConfiguration);
    }

    @Override
    public AbfsHttpRequestBuilder createRequestBuilder(URL url, String method, List<AbfsHttpHeader> requestHeaders, BodyPublisher bodyPublisher) {
        return null; // TODO ARNAUD
    }

}
