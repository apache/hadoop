package org.apache.hadoop.fs.s3a;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.Signer;

public class TestCustomSignerName implements Signer {
    @Override
    public void sign(SignableRequest<?> request, AWSCredentials credentials) {

    }
}
