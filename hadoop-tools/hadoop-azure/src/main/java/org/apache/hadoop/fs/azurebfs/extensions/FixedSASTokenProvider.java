package org.apache.hadoop.fs.azurebfs.extensions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;

public class FixedSASTokenProvider implements SASTokenProvider{
    Configuration configuration;
    String accountName;

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
        this.configuration = configuration;
        this.accountName = accountName;
    }

    @Override
    public String getSASToken(String account, String fileSystem, String path, String operation) throws IOException {
        String fixedToken = configuration.get(ConfigurationKeys.FS_AZURE_SAS_FIXED_TOKEN, null);
        if (fixedToken == null)
            throw new InvalidConfigurationValueException("The fixed SAS Token configuration value is invalid.");
        else
            return fixedToken;
    }
}
