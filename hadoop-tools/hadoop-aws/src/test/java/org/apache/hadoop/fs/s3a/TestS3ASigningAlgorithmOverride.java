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

    public void testCustomSignerOverride() throws AssertionError {
        LOG.debug("Inside createConfiguration...");
    }
}
