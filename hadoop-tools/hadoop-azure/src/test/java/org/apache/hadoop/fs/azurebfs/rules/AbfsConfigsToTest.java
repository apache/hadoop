package org.apache.hadoop.fs.azurebfs.rules;

import org.apache.hadoop.fs.azurebfs.constants.AccountType;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AbfsConfigsToTest {

  AuthType[] authTypes() default {AuthType.OAuth, AuthType.SharedKey
      //AuthType.SAS
  };

  AccountType[] accountTypes() default {AccountType.HNS, AccountType.NonHNS};
}
