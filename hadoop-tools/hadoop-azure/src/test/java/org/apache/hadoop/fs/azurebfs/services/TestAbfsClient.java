package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsCountersImpl;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.net.URL;
import java.util.Map;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;

public class TestAbfsClient {
    private static final String ACCOUNT_NAME = "bogusAccountName.dfs.core.windows.net";
    private static final String ACCOUNT_KEY = "testKey";

    @Test
    public void testTimerNotInitialize() throws Exception {
        final Configuration configuration = new Configuration();
        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration, ACCOUNT_NAME);

        AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
        AbfsClientContext abfsClientContext = new AbfsClientContextBuilder().withAbfsCounters(abfsCounters).build();

        // Get an instance of AbfsClient.
        AbfsClient client = new AbfsDfsClient(new URL("https://azure.com"),
                null,
                abfsConfiguration,
                (AccessTokenProvider) null,
                null,
                abfsClientContext);

        Assertions.assertThat(client.getTimer())
                .describedAs("Timer should not be initialized")
                .isNull();

        // Check if a thread with the name "abfs-timer-client" exists
        Assertions.assertThat(isThreadRunning("abfs-timer-client"))
                .describedAs("Expected thread 'abfs-timer-client' not found")
                .isEqualTo(false);
        client.close();
    }

    @Test
    public void testTimerInitialize() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(FS_AZURE_METRIC_FORMAT, String.valueOf(MetricFormat.INTERNAL_BACKOFF_METRIC_FORMAT));
        configuration.set(FS_AZURE_METRIC_ACCOUNT_NAME, ACCOUNT_NAME);
        configuration.set(FS_AZURE_METRIC_ACCOUNT_KEY, Base64.encode(ACCOUNT_KEY.getBytes()));
        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration, ACCOUNT_NAME);

        AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
        AbfsClientContext abfsClientContext = new AbfsClientContextBuilder().withAbfsCounters(abfsCounters).build();

        // Get an instance of AbfsClient.
        AbfsClient client = new AbfsDfsClient(new URL("https://azure.com"),
                null,
                abfsConfiguration,
                (AccessTokenProvider) null,
                null,
                abfsClientContext);

        Assertions.assertThat(client.getTimer())
                .describedAs("Timer should be initialized")
                .isNotNull();

        // Check if a thread with the name "abfs-timer-client" exists
        Assertions.assertThat(isThreadRunning("abfs-timer-client"))
                .describedAs("Expected thread 'abfs-timer-client' not found")
                .isEqualTo(true);
        client.close();

        // Check if the thread is removed after closing the client
        Assertions.assertThat(isThreadRunning("abfs-timer-client"))
                .describedAs("Unexpected thread 'abfs-timer-client' found")
                .isEqualTo(false);
    }

    private boolean isThreadRunning(String threadName) {
        // Get all threads and their stack traces
        Map<Thread, StackTraceElement[]> allThreads = Thread.getAllStackTraces();

        // Check if any thread has the specified name
        for (Thread thread : allThreads.keySet()) {
            if (thread.getName().equals(threadName)) {
                return true;
            }
        }
        return false;
    }
}
