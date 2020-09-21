package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Assert.*;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.assertTrue;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.*;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

import java.io.IOException;
import java.net.URL;
import java.util.UUID;

public class TestClientCorrelation {

    //private final AbfsClient client;
    private static final String ACCOUNT_NAME = "bogusAccountName.dfs.core.windows.net";
    private String validClientCorrelationId = "valid-corr-id-123";

    public TestClientCorrelation() throws IOException, IllegalAccessException {


        //client = mock(AbfsClient.class);
        //System.out.println(op.getUrl().getContent().toString());

        //System.out.println(op.getResult().getConnection().getRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID));
    }

    @Test
    public void verifyValidClientCorrelationId() throws IOException, IllegalAccessException {

        String clientCorrelationId = "valid-corr-id-123";

        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        configuration.set(ConfigurationKeys.FS_AZURE_CLIENT_CORRELATIONID, validClientCorrelationId);
        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                ACCOUNT_NAME);
        AzureBlobFileSystem abfs = new AzureBlobFileSystem();
//        URI uri = new U
//        abfs.initialize(uri, abfsConfiguration);


        AbfsClientContext abfsClientContext = new AbfsClientContextBuilder().build();
//        AbfsClient client = new AbfsClient(new URL("https://azure.com"), null,
//                abfsConfiguration, (AccessTokenProvider) null, abfsClientContext);

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
        AbfsHttpOperation op = new AbfsHttpOperation(url, "GET", requestHeaders);
        //op.getConnection().setRequestProperty("Request-Context", clientCorrelationId);
        //op.getConnection().setRequestProperty();
        System.out.println("hi"+op.getConnection().getResponseCode());
        Assert.assertTrue("Requests should not fail",
                op.getConnection().getResponseCode() < 400
                || op.getConnection().getResponseCode() >=500
                || op.getConnection().getResponseCode() == 404);

        for (String header : op.getConnection().getHeaderFields().keySet() )
            System.out.println(header+op.getConnection().getHeaderField(header));
        //op.processResponse(responseBuffer, 0, responseBuffer.length);
        System.out.println(op.getResponseHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID));
        System.out.println(op.getResponseHeader("Request-Context"));
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
        //System.out.println(op.getResponseHeader("C"))
        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID))
                .describedAs("Should be empty").startsWith(":");
    }
}
