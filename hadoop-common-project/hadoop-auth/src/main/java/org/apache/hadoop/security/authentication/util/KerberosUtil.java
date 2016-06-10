/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.authentication.util;

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;

public class KerberosUtil {

  /* Return the Kerberos login module name */
  public static String getKrb5LoginModuleName() {
    return (IBM_JAVA)
      ? "com.ibm.security.auth.module.Krb5LoginModule"
      : "com.sun.security.auth.module.Krb5LoginModule";
  }
  
  public static Oid getOidInstance(String oidName) 
      throws ClassNotFoundException, GSSException, NoSuchFieldException,
      IllegalAccessException {
    Class<?> oidClass;
    if (IBM_JAVA) {
      if ("NT_GSS_KRB5_PRINCIPAL".equals(oidName)) {
        // IBM JDK GSSUtil class does not have field for krb5 principal oid
        return new Oid("1.2.840.113554.1.2.2.1");
      }
      oidClass = Class.forName("com.ibm.security.jgss.GSSUtil");
    } else {
      oidClass = Class.forName("sun.security.jgss.GSSUtil");
    }
    Field oidField = oidClass.getDeclaredField(oidName);
    return (Oid)oidField.get(oidClass);
  }

  public static String getDefaultRealm() 
      throws ClassNotFoundException, NoSuchMethodException, 
      IllegalArgumentException, IllegalAccessException, 
      InvocationTargetException {
    Object kerbConf;
    Class<?> classRef;
    Method getInstanceMethod;
    Method getDefaultRealmMethod;
    if (IBM_JAVA) {
      classRef = Class.forName("com.ibm.security.krb5.internal.Config");
    } else {
      classRef = Class.forName("sun.security.krb5.Config");
    }
    getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
    kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
    getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm",
         new Class[0]);
    return (String)getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
  }

  public static String getDefaultRealmProtected() {
    String realmString = null;
    try {
      realmString = getDefaultRealm();
    } catch (RuntimeException rte) {
      //silently catch everything
    } catch (Exception e) {
      //silently return null
    }
    return realmString;
  }

  /*
   * For a Service Host Principal specification, map the host's domain
   * to kerberos realm, as specified by krb5.conf [domain_realm] mappings.
   * Unfortunately the mapping routines are private to the security.krb5
   * package, so have to construct a PrincipalName instance to derive the realm.
   *
   * Many things can go wrong with Kerberos configuration, and this is not
   * the place to be throwing exceptions to help debug them.  Nor do we choose
   * to make potentially voluminous logs on every call to a communications API.
   * So we simply swallow all exceptions from the underlying libraries and
   * return null if we can't get a good value for the realmString.
   *
   * @param shortprinc A service principal name with host fqdn as instance, e.g.
   *     "HTTP/myhost.mydomain"
   * @return String value of Kerberos realm, mapped from host fqdn
   *     May be default realm, or may be null.
   */
  public static String getDomainRealm(String shortprinc) {
    Class<?> classRef;
    Object principalName; //of type sun.security.krb5.PrincipalName or IBM equiv
    String realmString = null;
    try {
      if (IBM_JAVA) {
        classRef = Class.forName("com.ibm.security.krb5.PrincipalName");
      } else {
        classRef = Class.forName("sun.security.krb5.PrincipalName");
      }
      int tKrbNtSrvHst = classRef.getField("KRB_NT_SRV_HST").getInt(null);
      principalName = classRef.getConstructor(String.class, int.class).
          newInstance(shortprinc, tKrbNtSrvHst);
      realmString = (String)classRef.getMethod("getRealmString", new Class[0]).
          invoke(principalName, new Object[0]);
    } catch (RuntimeException rte) {
      //silently catch everything
    } catch (Exception e) {
      //silently return default realm (which may itself be null)
    }
    if (null == realmString || realmString.equals("")) {
      return getDefaultRealmProtected();
    } else {
      return realmString;
    }
  }

  /* Return fqdn of the current host */
  static String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }
  
  /**
   * Create Kerberos principal for a given service and hostname,
   * inferring realm from the fqdn of the hostname. It converts
   * hostname to lower case. If hostname is null or "0.0.0.0", it uses
   * dynamically looked-up fqdn of the current host instead.
   * If domain_realm mappings are inadequately specified, it will
   * use default_realm, per usual Kerberos behavior.
   * If default_realm also gives a null value, then a principal
   * without realm will be returned, which by Kerberos definitions is
   * just another way to specify default realm.
   *
   * @param service
   *          Service for which you want to generate the principal.
   * @param hostname
   *          Fully-qualified domain name.
   * @return Converted Kerberos principal name.
   * @throws UnknownHostException
   *           If no IP address for the local host could be found.
   */
  public static final String getServicePrincipal(String service,
      String hostname)
      throws UnknownHostException {
    String fqdn = hostname;
    String shortprinc = null;
    String realmString = null;
    if (null == fqdn || fqdn.equals("") || fqdn.equals("0.0.0.0")) {
      fqdn = getLocalHostName();
    }
    // convert hostname to lowercase as kerberos does not work with hostnames
    // with uppercase characters.
    fqdn = fqdn.toLowerCase(Locale.US);
    shortprinc = service + "/" + fqdn;
    // Obtain the realm name inferred from the domain of the host
    realmString = getDomainRealm(shortprinc);
    if (null == realmString || realmString.equals("")) {
      return shortprinc;
    } else {
      return shortprinc + "@" + realmString;
    }
  }

  /**
   * Get all the unique principals present in the keytabfile.
   * 
   * @param keytabFileName 
   *          Name of the keytab file to be read.
   * @return list of unique principals in the keytab.
   * @throws IOException 
   *          If keytab entries cannot be read from the file.
   */
  static final String[] getPrincipalNames(String keytabFileName) throws IOException {
    Keytab keytab = Keytab.loadKeytab(new File(keytabFileName));
    Set<String> principals = new HashSet<String>();
    List<PrincipalName> entries = keytab.getPrincipals();
    for (PrincipalName entry : entries) {
      principals.add(entry.getName().replace("\\", "/"));
    }
    return principals.toArray(new String[0]);
  }

  /**
   * Get all the unique principals from keytabfile which matches a pattern.
   * 
   * @param keytab Name of the keytab file to be read.
   * @param pattern pattern to be matched.
   * @return list of unique principals which matches the pattern.
   * @throws IOException if cannot get the principal name
   */
  public static final String[] getPrincipalNames(String keytab,
      Pattern pattern) throws IOException {
    String[] principals = getPrincipalNames(keytab);
    if (principals.length != 0) {
      List<String> matchingPrincipals = new ArrayList<String>();
      for (String principal : principals) {
        if (pattern.matcher(principal).matches()) {
          matchingPrincipals.add(principal);
        }
      }
      principals = matchingPrincipals.toArray(new String[0]);
    }
    return principals;
  }

  /**
   * Check if the subject contains Kerberos keytab related objects.
   * The Kerberos keytab object attached in subject has been changed
   * from KerberosKey (JDK 7) to KeyTab (JDK 8)
   *
   *
   * @param subject subject to be checked
   * @return true if the subject contains Kerberos keytab
   */
  public static boolean hasKerberosKeyTab(Subject subject) {
    return !subject.getPrivateCredentials(KeyTab.class).isEmpty();
  }

  /**
   * Check if the subject contains Kerberos ticket.
   *
   *
   * @param subject subject to be checked
   * @return true if the subject contains Kerberos ticket
   */
  public static boolean hasKerberosTicket(Subject subject) {
    return !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
  }
}
