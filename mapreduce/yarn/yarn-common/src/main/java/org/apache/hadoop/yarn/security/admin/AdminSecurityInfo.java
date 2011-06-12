package org.apache.hadoop.yarn.security.admin;

import java.lang.annotation.Annotation;

import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class AdminSecurityInfo implements SecurityInfo {

  @Override
  public KerberosInfo getKerborosInfo(Class<?> protocol) {
    return new KerberosInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public String serverPrincipal() {
        return YarnConfiguration.RM_SERVER_PRINCIPAL_KEY;
      }

      @Override
      public String clientPrincipal() {
        return null;
      }
    };
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol) {
    return null;
  }

}
