package org.apache.hadoop.fs.azurebfs.rules;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

public interface AuthTypesTestable {

  void setAuthType(AuthType authType);

  AbfsConfiguration getConfiguration();

  void initFSEndpointForNewFS() throws Exception;
}
