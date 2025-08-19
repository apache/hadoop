package org.apache.hadoop.fs.cosn.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.cosn.CosNConfigKeys;
import org.apache.hadoop.fs.cosn.auth.DynamicTemporaryCosnCredentialsProvider;

import static org.apache.hadoop.fs.cosn.auth.DynamicTemporaryCosnCredentialsProvider.STS_SECRET_ID_KEY;
import static org.apache.hadoop.fs.cosn.auth.DynamicTemporaryCosnCredentialsProvider.STS_SECRET_KEY_KEY;

/**
 * Contract tests for CosN using a dynamic temporary token provider (STS).
 * This test requires long-term credentials with STS access to be configured.
 */
public class TestCosNContractDynamicToken extends AbstractContractCreateTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new CosNContract(conf);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = super.createConfiguration();

    newConf.set(CosNConfigKeys.COSN_CREDENTIALS_PROVIDER,
        DynamicTemporaryCosnCredentialsProvider.class.getName());
    newConf.setBoolean("fs.cosn.impl.disable.cache", true);

    String stsSecretId = System.getProperty(STS_SECRET_ID_KEY);
    String stsSecretKey = System.getProperty(STS_SECRET_KEY_KEY);

    if (stsSecretId == null || stsSecretKey == null) {
      // Fallback to configuration for convenience, but log a warning.
      System.err.println("WARN: Reading long-term STS credentials from configuration file. "
          + "It is recommended to use system properties for security.");
      stsSecretId = newConf.get(STS_SECRET_ID_KEY);
      stsSecretKey = newConf.get(STS_SECRET_KEY_KEY);
    }

    if (stsSecretId == null || stsSecretKey == null) {
      throw new RuntimeException("STS credentials for tests are not provided. "
          + "Please set them via system properties (-Dfs.cosn.auth.sts.secret.id=... and "
          + "-Dfs.cosn.auth.sts.secret.key=...)");
    }

    newConf.set(STS_SECRET_ID_KEY, stsSecretId);
    newConf.set(STS_SECRET_KEY_KEY, stsSecretKey);

    return newConf;
  }
}