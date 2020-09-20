package org.apache.hadoop.fs.azurebfs.services;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Assert.*;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESOURCE;
import static org.mockito.Mockito.mock;
import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

import java.io.IOException;
import java.net.URL;
import java.util.UUID;

public class TestClientCorrelation {
    private AbfsHttpOperation op;
    //private AbfsRestOperation op;
    private final AbfsClient client;

    public TestClientCorrelation() {
        //op = mock(AbfsHttpOperation.class);
        /*URL url = mock(URL.class);
        final List<AbfsHttpHeader> requestHeaders = AbfsClient.createDefaultHeaders();
        op = new AbfsHttpOperation(url, "GET", requestHeaders);*/
        //op = mock(AbfsRestOperation.class);
        client = mock(AbfsClient.class);
        //System.out.println(op.getUrl().getContent().toString());

        //System.out.println(op.getResult().getConnection().getRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID));
    }

    @Test
    public void verifyValidClientCorrelationId() throws IOException {

        String clientCorrelationId = "valid-corr-id-123";
        //URL url = mock(URL.class);
        AbfsUriQueryBuilder abfsUriQueryBuilder = client.createDefaultUriQueryBuilder();
        System.out.println(abfsUriQueryBuilder);
        //abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

        final URL url = new URL("https://azure.com"); //client.createRequestUrl("",abfsUriQueryBuilder.toString());
        //final List<AbfsHttpHeader> requestHeaders = client.createDefaultHeaders();
        final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
        //requestHeaders.add(new AbfsHttpHeader(X_MS_VERSION, xMsVersion));
        requestHeaders.add(new AbfsHttpHeader(X_MS_CLIENT_REQUEST_ID, clientCorrelationId));
        op = new AbfsHttpOperation(url, "GET", requestHeaders);

        //client.setClientCorrelationId(clientCorrelationId);
        /*byte[] buffer = new byte[5];
        int offset = 0;
        int length = 5;
        byte[] requestBuffer = new byte[128];
        op.sendRequest(requestBuffer, 0, requestBuffer.length);

        byte[] responseBuffer = new byte[4 * 1024];
        op.processResponse(responseBuffer, 0, responseBuffer.length);
        System.out.println(responseBuffer);
        //op.sendRequest(buffer, offset, length);
        //final List<AbfsHttpHeader> requestHeaders = AbfsClient.createDefaultHeaders();
        op.getConnection().setRequestProperty(X_MS_CLIENT_REQUEST_ID,
          clientCorrelationId + ":" + UUID.randomUUID().toString());*/
        Assert.assertTrue("Requests should not fail",
                op.getStatusCode() >= 400 && op.getStatusCode() < 500
        && op.getStatusCode() != 404);
        //assertThat("",op.getStatusCode(), greaterThan(400)).describedAs("Requests should not fail");
        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID))
                .describedAs("Response header should contain client request header")
                .contains(clientCorrelationId);
    }
/*
    @Test
    public void verifyInvalidClientCorrelationId() {

        String clientCorrelationId = "thisIs!nval!d";
        op.getResult().getConnection().setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID,
                clientCorrelationId + ":" + UUID.randomUUID().toString());
        //assertTrue()
        Assert.assertTrue("Requests should not fail",op.getResult().getStatusCode() > 400);
        assertThat(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID))
                .describedAs("Invalid correlation id should be converted to empty string")
                .startsWith(":");
    }

    @Test
    public void verifyEmptyClientCorrelationId() {

        String clientCorrelationId = "";
        op.getResult().getConnection().setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID,
                clientCorrelationId + ":" + UUID.randomUUID().toString());
        Assert.assertTrue("Requests should not fail",op.getResult().getStatusCode() > 400);
        assertThat(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID))
                .describedAs("Should be empty").startsWith(":");
    }*/
}
