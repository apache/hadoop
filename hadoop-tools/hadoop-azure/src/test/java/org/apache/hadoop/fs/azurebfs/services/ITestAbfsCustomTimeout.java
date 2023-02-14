package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.services.*;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ITestAbfsCustomTimeout extends AbstractAbfsIntegrationTest {
    public ITestAbfsCustomTimeout() throws Exception {
        super();
    }

    private AbfsRestOperation getMockRestOp() {
        AbfsRestOperation op = mock(AbfsRestOperation.class);
        AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
        return op;
    }

    private AbfsClient getMockAbfsClient() {
        AbfsClient client = mock(AbfsClient.class);
        AbfsPerfTracker tracker = new AbfsPerfTracker(
                "test",
                this.getAccountName(),
                this.getConfiguration());
        when(client.getAbfsPerfTracker()).thenReturn(tracker);
        return client;
    }

    @Test
    public void testCustomFirstTry() throws IOException {
        // trying operation with get file system properties
        AbfsConfiguration currentConfig = getConfiguration();
        currentConfig.set("fs.azure.getfs.request.timeout", "100");
        AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(currentConfig.getRawConfiguration());
        AbfsClient testClient = fs.getAbfsStore().getClient();
        AbfsRestOperation op = testClient.getFilesystemProperties(getTestTracingContext(fs, true));
        assertEquals("100", Integer.toString(op.getReqTimeout()));
    }

    @Test
    public void testRetry() throws Exception {
        AbfsConfiguration currentConfig = getConfiguration();
        currentConfig.set("fs.azure.getfs.request.timeout", "10");
        int startingReqTimeout = 50;
        currentConfig.set(ConfigurationKeys.AZURE_MAX_REQUEST_TIMEOUT, "90");
        int maxTimeout = 90;
        currentConfig.set(ConfigurationKeys.AZURE_REQUEST_TIMEOUT_INCREASE_RATE, "2");
        int incRate = 2;
        AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(currentConfig.getRawConfiguration());
        // need a mock of AbfsClient
        // need a mock of AbfsRestOperation
        // need a mock of AbfsHttpOperation -> hard code to give 1 retry and then okay
        // doAnswer on AbfsRestOperation
        // spy on the exponential retry class
        AbfsClient client = fs.getAbfsClient();
        AbfsClient mockClient = spy(client);
        AbfsRestOperation op = spy(AbfsRestOperation.class);
        doReturn(op).when(mockClient).getFilesystemProperties(any(TracingContext.class));
        AbfsHttpOperation mockHttpOp = spy(AbfsHttpOperation.class);
        doReturn(-1).when(mockHttpOp).getStatusCode();
        doReturn(mockHttpOp).when(op).getResult();
        doAnswer(invocationOnMock -> {
            assertEquals(op.getReqTimeout(), startingReqTimeout);
            return null;
        }).when(op).setCustomRetryRequestTimeout();
        mockClient.getFilesystemProperties(getTestTracingContext(fs, true));
        verify(op, times(2)).setCustomRetryRequestTimeout();

    }
}
