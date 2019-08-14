package org.apache.hadoop.fs.s3a;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.Signer;
import org.apache.hadoop.conf.Configuration;

import org.junit.Test;

import java.util.Objects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestS3ASigningAlgorithmOverride extends AbstractS3ATestBase {

    @Override
    protected Configuration createConfiguration() {
        Configuration conf = super.createConfiguration();
        conf.set(Constants.SIGNING_ALGORITHM, S3ATestConstants.S3A_SIGNING_ALGORITHM);
        LOG.debug("Inside createConfiguration...");
        return conf;
    }

    @Test
    public void testCustomSignerOverride() throws AssertionError {
        LOG.debug("Inside createConfiguration...");
        assertTrue(assertIsCustomSignerLoaded());
    }

    private boolean assertIsCustomSignerLoaded() {
        final ClientConfiguration awsConf = new ClientConfiguration();
        LOG.debug("Inside assertIsCustomSignerLoaded...");
        return assertEquals(awsConf.getSignerOverride(), S3ATestConstants.S3A_SIGNING_ALGORITHM);
    }

    private boolean assertEquals(String str1, String str2) {
        return Objects.equals(str1,str2);
    }
}
