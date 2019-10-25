package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

@ConfigGroup(prefix = "hdds.scm")
public class ScmConfig {
  private String principal;
  private String keytab;

  @Config(key = "kerberos.principal",
    type = ConfigType.STRING,
    defaultValue = "",
    tags = { ConfigTag.SECURITY },
    description = "This Kerberos principal is used by the SCM service."
  )
  public void setKerberosPrincipal(String kerberosPrincipal) { this.principal = kerberosPrincipal; }

  @Config(key = "kerberos.keytab.file",
    type = ConfigType.STRING,
    defaultValue = "",
    tags = { ConfigTag.SECURITY },
    description = "The keytab file used by SCM daemon to login as its service principal."
  )
  public void setKerberosKeytab(String kerberosKeytab) { this.keytab = kerberosKeytab; }

  public String getKerberosPrincipal() { return this.principal; }

  public String getKerberosKeytab() { return this.keytab; }

  public static class ConfigStrings {
    /* required for SCMSecurityProtocol where the KerberosInfo references the old configuration with
     * the annotation shown below:-
     * @KerberosInfo(serverPrincipal = ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
     */
    public static final String HDDS_SCM_KERBEROS_PRINCIPAL_KEY = "hdds.scm.kerberos.principal";
    public static final String HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY = "hdds.scm.kerberos.keytab.file";
  }
}
