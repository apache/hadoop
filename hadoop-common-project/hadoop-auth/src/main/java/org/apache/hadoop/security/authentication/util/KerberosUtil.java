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

import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

public class KerberosUtil {

  /* Return the Kerberos login module name */
  public static String getKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
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
    if (System.getProperty("java.vendor").contains("IBM")) {
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
  
  /* Return fqdn of the current host */
  static String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }
  
  /**
   * Create Kerberos principal for a given service and hostname. It converts
   * hostname to lower case. If hostname is null or "0.0.0.0", it uses
   * dynamically looked-up fqdn of the current host instead.
   * 
   * @param service
   *          Service for which you want to generate the principal.
   * @param hostname
   *          Fully-qualified domain name.
   * @return Converted Kerberos principal name.
   * @throws UnknownHostException
   *           If no IP address for the local host could be found.
   */
  public static final String getServicePrincipal(String service, String hostname)
      throws UnknownHostException {
    String fqdn = hostname;
    if (null == fqdn || fqdn.equals("") || fqdn.equals("0.0.0.0")) {
      fqdn = getLocalHostName();
    }
    // convert hostname to lowercase as kerberos does not work with hostnames
    // with uppercase characters.
    return service + "/" + fqdn.toLowerCase(Locale.US);
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
      Keytab keytab = Keytab.read(new File(keytabFileName));
      Set<String> principals = new HashSet<String>();
      List<KeytabEntry> entries = keytab.getEntries();
      for (KeytabEntry entry: entries){
        principals.add(entry.getPrincipalName().replace("\\", "/"));
      }
      return principals.toArray(new String[0]);
    }

  /**
   * Get all the unique principals from keytabfile which matches a pattern.
   * 
   * @param keytab 
   *          Name of the keytab file to be read.
   * @param pattern 
   *         pattern to be matched.
   * @return list of unique principals which matches the pattern.
   * @throws IOException 
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
}
