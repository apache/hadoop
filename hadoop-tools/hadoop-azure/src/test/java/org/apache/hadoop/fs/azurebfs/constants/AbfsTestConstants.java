package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.fs.azurebfs.services.AuthType;

public class AbfsTestConstants {

  public static final AccountType[] ACCOUNT_TYPES_TO_TEST = {
      AccountType.HNS,
      AccountType.NonHNS
  };

  public static final AuthType[] AUTH_TYPES_TO_TEST = {
      AuthType.OAuth,
      AuthType.SharedKey
      //AuthType.SAS
  };
}
