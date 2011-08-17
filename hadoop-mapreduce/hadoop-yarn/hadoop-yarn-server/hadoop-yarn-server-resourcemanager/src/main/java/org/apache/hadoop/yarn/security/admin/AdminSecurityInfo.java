package org.apache.hadoop.yarn.security.admin;

import java.lang.annotation.Annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.RMAdminProtocol;

public class AdminSecurityInfo extends SecurityInfo {

  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    if (!protocol.equals(RMAdminProtocol.RMAdminProtocolService.BlockingInterface.class)) {
      return null;
    }
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
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    return null;
  }

}
