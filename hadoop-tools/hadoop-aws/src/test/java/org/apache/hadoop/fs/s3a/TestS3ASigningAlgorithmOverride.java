package org.apache.hadoop.fs.s3a;

import com.amazonaws.ClientConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Objects;

import static org.apache.hadoop.fs.s3a.S3AUtils.initConnectionSettings;

public class TestS3ASigningAlgorithmOverride extends AbstractS3ATestBase {

    @Override
    protected Configuration createConfiguration() {
      Configuration conf = super.createConfiguration();
      S3ATestUtils.disableFilesystemCaching(conf);
      conf.set(Constants.SIGNING_ALGORITHM, S3ATestConstants.S3A_SIGNING_ALGORITHM);
      return conf;
    }

    @Test
    public void testCustomSignerOverride() throws AssertionError {
        assertTrue(assertIsCustomSignerLoaded(getConfiguration()));
    }

    private boolean assertIsCustomSignerLoaded(Configuration configuration) {
      final ClientConfiguration awsConf = new ClientConfiguration();
      initConnectionSettings(configuration, awsConf);
      return assertEquals(awsConf.getSignerOverride(), S3ATestConstants.S3A_SIGNING_ALGORITHM);
    }

    private boolean assertEquals(String str1, String str2) {
      return Objects.equals(str1,str2);
    }
}
