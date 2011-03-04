/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

import sun.security.jgss.krb5.Krb5Util;
import sun.security.krb5.Credentials;
import sun.security.krb5.PrincipalName;

public class SecurityUtil {
  private static final Log LOG = LogFactory.getLog(SecurityUtil.class);

  /**
   * Find the original TGT within the current subject's credentials. Cross-realm
   * TGT's of the form "krbtgt/TWO.COM@ONE.COM" may be present.
   * 
   * @return The TGT from the current subject
   * @throws IOException
   *           if TGT can't be found
   */
  private static KerberosTicket getTgtFromSubject() throws IOException {
    Set<KerberosTicket> tickets = Subject.getSubject(
        AccessController.getContext()).getPrivateCredentials(
        KerberosTicket.class);
    for (KerberosTicket t : tickets) {
      if (isOriginalTGT(t.getServer().getName()))
        return t;
    }
    throw new IOException("Failed to find TGT from current Subject");
  }
  
  // Original TGT must be of form "krbtgt/FOO@FOO". Verify this
  protected static boolean isOriginalTGT(String name) {
    if(name == null) return false;
    
    String [] components = name.split("[/@]");

    return components.length == 3 &&
           "krbtgt".equals(components[0]) &&
           components[1].equals(components[2]);
  }

  /**
   * Explicitly pull the service ticket for the specified host.  This solves a
   * problem with Java's Kerberos SSL problem where the client cannot 
   * authenticate against a cross-realm service.  It is necessary for clients
   * making kerberized https requests to call this method on the target URL
   * to ensure that in a cross-realm environment the remote host will be 
   * successfully authenticated.  
   * 
   * This method will be removed when the Java behavior is changed.
   * 
   * @param remoteHost Target URL the krb-https client will access
   */
  public static void fetchServiceTicket(URL remoteHost) throws IOException {
    if(!UserGroupInformation.isSecurityEnabled())
      return;
    
    String serviceName = "host/" + remoteHost.getHost();
    LOG.debug("Fetching service ticket for host at: " + serviceName);
    Credentials serviceCred = null;
    try {
      PrincipalName principal = new PrincipalName(serviceName,
          PrincipalName.KRB_NT_SRV_HST);
      serviceCred = Credentials.acquireServiceCreds(principal
          .toString(), Krb5Util.ticketToCreds(getTgtFromSubject()));
    } catch (Exception e) {
      throw new IOException("Invalid service principal name: "
          + serviceName, e);
    }
    if (serviceCred == null) {
      throw new IOException("Can't get service ticket for " + serviceName);
    }
    Subject.getSubject(AccessController.getContext()).getPrivateCredentials()
        .add(Krb5Util.credsToTicket(serviceCred));
  }
}
