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
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.IllegalCharsetNameException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;

public class KerberosUtil {

  /* Return the Kerberos login module name */
  public static String getKrb5LoginModuleName() {
    return (IBM_JAVA)
      ? "com.ibm.security.auth.module.Krb5LoginModule"
      : "com.sun.security.auth.module.Krb5LoginModule";
  }

  public static final Oid GSS_SPNEGO_MECH_OID =
      getNumericOidInstance("1.3.6.1.5.5.2");
  public static final Oid GSS_KRB5_MECH_OID =
      getNumericOidInstance("1.2.840.113554.1.2.2");
  public static final Oid NT_GSS_KRB5_PRINCIPAL_OID =
      getNumericOidInstance("1.2.840.113554.1.2.2.1");

  // numeric oids will never generate a GSSException for a malformed oid.
  // use to initialize statics.
  private static Oid getNumericOidInstance(String oidName) {
    try {
      return new Oid(oidName);
    } catch (GSSException ex) {
      throw new IllegalArgumentException(ex);
    }
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

  /**
   * Return the default realm for this JVM.
   *
   * @return The default realm
   * @throws IllegalArgumentException If the default realm does not exist.
   * @throws ClassNotFoundException Not thrown. Exists for compatibility.
   * @throws NoSuchMethodException Not thrown. Exists for compatibility.
   * @throws IllegalAccessException Not thrown. Exists for compatibility.
   * @throws InvocationTargetException Not thrown. Exists for compatibility.
   */
  public static String getDefaultRealm()
      throws ClassNotFoundException, NoSuchMethodException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    // Any name is okay.
    return new KerberosPrincipal("tmp", 1).getRealm();
  }

  /**
   * Return the default realm for this JVM.
   * If the default realm does not exist, this method returns null.
   *
   * @return The default realm
   */
  public static String getDefaultRealmProtected() {
    try {
      return getDefaultRealm();
    } catch (Exception e) {
      //silently catch everything
      return null;
    }
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
  public static String getLocalHostName() throws UnknownHostException {
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

  /**
   * Extract the TGS server principal from the given gssapi kerberos or spnego
   * wrapped token.
   * @param rawToken bytes of the gss token
   * @return String of server principal
   * @throws IllegalArgumentException if token is undecodable
   */
  public static String getTokenServerName(byte[] rawToken) {
    // subsequent comments include only relevant portions of the kerberos
    // DER encoding that will be extracted.
    DER token = new DER(rawToken);
    // InitialContextToken ::= [APPLICATION 0] IMPLICIT SEQUENCE {
    //     mech   OID
    //     mech-token  (NegotiationToken or InnerContextToken)
    // }
    DER oid = token.next();
    if (oid.equals(DER.SPNEGO_MECH_OID)) {
      // NegotiationToken ::= CHOICE {
      //     neg-token-init[0] NegTokenInit
      // }
      // NegTokenInit ::= SEQUENCE {
      //     mech-token[2]     InitialContextToken
      // }
      token = token.next().get(0xa0, 0x30, 0xa2, 0x04).next();
      oid = token.next();
    }
    if (!oid.equals(DER.KRB5_MECH_OID)) {
      throw new IllegalArgumentException("Malformed gss token");
    }
    // InnerContextToken ::= {
    //     token-id[1]
    //     AP-REQ
    // }
    if (token.next().getTag() != 1) {
      throw new IllegalArgumentException("Not an AP-REQ token");
    }
    // AP-REQ ::= [APPLICATION 14] SEQUENCE {
    //     ticket[3]      Ticket
    // }
    DER ticket = token.next().get(0x6e, 0x30, 0xa3, 0x61, 0x30);
    // Ticket ::= [APPLICATION 1] SEQUENCE {
    //     realm[1]       String
    //     sname[2]       PrincipalName
    // }
    // PrincipalName ::= SEQUENCE {
    //     name-string[1] SEQUENCE OF String
    // }
    String realm = ticket.get(0xa1, 0x1b).getAsString();
    DER names = ticket.get(0xa2, 0x30, 0xa1, 0x30);
    StringBuilder sb = new StringBuilder();
    while (names.hasNext()) {
      if (sb.length() > 0) {
        sb.append('/');
      }
      sb.append(names.next().getAsString());
    }
    return sb.append('@').append(realm).toString();
  }

  // basic ASN.1 DER decoder to traverse encoded byte arrays.
  private static class DER implements Iterator<DER> {
    static final DER SPNEGO_MECH_OID = getDER(GSS_SPNEGO_MECH_OID);
    static final DER KRB5_MECH_OID = getDER(GSS_KRB5_MECH_OID);

    private static DER getDER(Oid oid) {
      try {
        return new DER(oid.getDER());
      } catch (GSSException ex) {
        // won't happen.  a proper OID is encodable.
        throw new IllegalArgumentException(ex);
      }
    }

    private final int tag;
    private final ByteBuffer bb;

    DER(byte[] buf) {
      this(ByteBuffer.wrap(buf));
    }

    DER(ByteBuffer srcbb) {
      tag = srcbb.get() & 0xff;
      int length = readLength(srcbb);
      bb = srcbb.slice();
      bb.limit(length);
      srcbb.position(srcbb.position() + length);
    }

    int getTag() {
      return tag;
    }

    // standard ASN.1 encoding.
    private static int readLength(ByteBuffer bb) {
      int length = bb.get();
      if ((length & (byte)0x80) != 0) {
        int varlength = length & 0x7f;
        length = 0;
        for (int i=0; i < varlength; i++) {
          length = (length << 8) | (bb.get() & 0xff);
        }
      }
      return length;
    }

    DER choose(int subtag) {
      while (hasNext()) {
        DER der = next();
        if (der.getTag() == subtag) {
          return der;
        }
      }
      return null;
    }

    DER get(int... tags) {
      DER der = this;
      for (int i=0; i < tags.length; i++) {
        int expectedTag = tags[i];
        // lookup for exact match, else scan if it's sequenced.
        if (der.getTag() != expectedTag) {
          der = der.hasNext() ? der.choose(expectedTag) : null;
        }
        if (der == null) {
          StringBuilder sb = new StringBuilder("Tag not found:");
          for (int ii=0; ii <= i; ii++) {
            sb.append(" 0x").append(Integer.toHexString(tags[ii]));
          }
          throw new IllegalStateException(sb.toString());
        }
      }
      return der;
    }

    String getAsString() {
      try {
        return new String(bb.array(), bb.arrayOffset() + bb.position(),
            bb.remaining(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new IllegalCharsetNameException("UTF-8"); // won't happen.
      }
    }

    @Override
    public int hashCode() {
      return 31 * tag + bb.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof DER) &&
          tag == ((DER)o).tag && bb.equals(((DER)o).bb);
    }

    @Override
    public boolean hasNext() {
      // it's a sequence or an embedded octet.
      return ((tag & 0x30) != 0 || tag == 0x04) && bb.hasRemaining();
    }

    @Override
    public DER next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return new DER(bb);
    }

    @Override
    public String toString() {
      return "[tag=0x"+Integer.toHexString(tag)+" bb="+bb+"]";
    }
  }
}
