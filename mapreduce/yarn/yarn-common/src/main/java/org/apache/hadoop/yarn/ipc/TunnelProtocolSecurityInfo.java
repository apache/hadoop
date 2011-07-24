package org.apache.hadoop.yarn.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AnnotatedSecurityInfo;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine.TunnelProtocol;

public class TunnelProtocolSecurityInfo implements SecurityInfo {
  public static final Log LOG = LogFactory.getLog(TunnelProtocolSecurityInfo.class);
  
  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    LOG.info("Get kerberos info being called, Tunnelprotocolinfo " + protocol);
    if (TunnelProtocol.class.equals(protocol)) {
      try {
        LOG.info("The Tunnel Security info class " + conf.get(YarnConfiguration.YARN_SECURITY_INFO));
        Class<SecurityInfo> secInfoClass = (Class<SecurityInfo>)  conf.getClass(
            YarnConfiguration.YARN_SECURITY_INFO, SecurityInfo.class);
        SecurityInfo secInfo = secInfoClass.newInstance();
        return secInfo.getKerberosInfo(protocol, conf);
      } catch (Exception e) {
        throw new RuntimeException("Unable to load class", e);
      }
    }
    return null;
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    if (TunnelProtocol.class.equals(protocol)) {
      try {
        Class<SecurityInfo> secInfoClass = (Class<SecurityInfo>)  conf.getClass(
            YarnConfiguration.YARN_SECURITY_INFO, AnnotatedSecurityInfo.class);
        SecurityInfo secInfo = secInfoClass.newInstance();
        return secInfo.getTokenInfo(protocol, conf);
      } catch (Exception e) {
        throw new RuntimeException("Unable to load Yarn Security Info class", e);
      }
    }
    return null;
  }
}