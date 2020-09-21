package org.apache.hadoop.fs.azurebfs.services;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Assert.*;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESOURCE;
import static org.mockito.Mockito.mock;
import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.*;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

import java.io.IOException;
import java.net.URL;
import java.util.UUID;

public class TestClientCorrelation {
    private AbfsHttpOperation op;
    private final AbfsClient client;

    public TestClientCorrelation() {
        client = mock(AbfsClient.class);
        //System.out.println(op.getUrl().getContent().toString());

        //System.out.println(op.getResult().getConnection().getRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID));
    }

    @Test
    public void verifyValidClientCorrelationId() throws IOException {

        String clientCorrelationId = "valid-corr-id-123";
        final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
        //requestHeaders.add(new AbfsHttpHeader(X_MS_VERSION, xMsVersion));
        requestHeaders.add(new AbfsHttpHeader(X_MS_CLIENT_REQUEST_ID, clientCorrelationId));
       // requestHeaders.add(new AbfsHttpHeader("Access-Control-Allow-Headers", "*"));
        requestHeaders.add(new AbfsHttpHeader("Access-Control-Expose-Headers",
                CONTENT_TYPE + "," + X_MS_CLIENT_REQUEST_ID));
        //requestHeaders.add(new AbfsHttpHeader("return-client-request-id", "True"));
        //op.getConnection().
        for (AbfsHttpHeader header : requestHeaders)
        System.out.println(header.getName());
        URL url = new URL("https://azure.com");
        op = new AbfsHttpOperation(url, "GET", requestHeaders);
        System.out.println("hi"+op.getConnection().getResponseCode());
        Assert.assertTrue("Requests should not fail",
                op.getConnection().getResponseCode() < 400
                || op.getConnection().getResponseCode() >=500
                || op.getConnection().getResponseCode() == 404);

        for (String header : op.getConnection().getHeaderFields().keySet() )
            System.out.println(header+op.getConnection().getHeaderField(header));
        //op.processResponse(responseBuffer, 0, responseBuffer.length);
        System.out.println(op.getResponseHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID));

        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID))
                .describedAs("Response header should contain client request header")
                .contains(clientCorrelationId);
    }

    @Test
    public void verifyInvalidClientCorrelationId() throws IOException {

        String clientCorrelationId = "thisIs!nval!d";
        final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();

        requestHeaders.add(new AbfsHttpHeader(X_MS_CLIENT_REQUEST_ID, clientCorrelationId));
        requestHeaders.add(new AbfsHttpHeader("Access-Control-Expose-Headers",
                CONTENT_TYPE + "," + X_MS_CLIENT_REQUEST_ID));

        URL url = new URL("https://azure.com");
        AbfsHttpOperation op = new AbfsHttpOperation(url, "GET", requestHeaders);
        System.out.println("hi"+op.getConnection().getResponseCode());
        Assert.assertTrue("Requests should not fail",
                op.getConnection().getResponseCode() < 400
                || op.getConnection().getResponseCode() >=500
                || op.getConnection().getResponseCode() == 404);

        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID))
                .describedAs("Invalid correlation id should be converted to empty string")
                .startsWith(":");
    }

    @Test
    public void verifyEmptyClientCorrelationId() throws IOException {

        String clientCorrelationId = "";

        final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();

        requestHeaders.add(new AbfsHttpHeader(X_MS_CLIENT_REQUEST_ID, clientCorrelationId));
        requestHeaders.add(new AbfsHttpHeader("Access-Control-Expose-Headers",
                CONTENT_TYPE + "," + X_MS_CLIENT_REQUEST_ID));

        URL url = new URL("https://azure.com");
        AbfsHttpOperation op = new AbfsHttpOperation(url, "GET", requestHeaders);
        System.out.println("hi"+op.getConnection().getResponseCode());
        Assert.assertTrue("Requests should not fail",
                op.getConnection().getResponseCode() < 400
                        || op.getConnection().getResponseCode() >=500
                        || op.getConnection().getResponseCode() == 404);

        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID))
                .describedAs("Should be empty").startsWith(":");
    }
}
