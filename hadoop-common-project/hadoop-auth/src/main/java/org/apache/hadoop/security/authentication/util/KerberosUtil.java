package org.apache.hadoop.security.authentication.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

public class KerberosUtil {

  /* Return the Kerberos login module name */
  public static String getKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
      ? "com.ibm.security.auth.module.Krb5LoginModule"
      : "com.sun.security.auth.module.Krb5LoginModule";
  }
  
  public static Oid getOidClassInstance(String servicePrincipal,
      GSSManager gssManager) 
      throws ClassNotFoundException, GSSException, NoSuchFieldException,
      IllegalAccessException {
    Class<?> oidClass;
    if (System.getProperty("java.vendor").contains("IBM")) {
      oidClass = Class.forName("com.ibm.security.jgss.GSSUtil");
    } else {
      oidClass = Class.forName("sun.security.jgss.GSSUtil");
    }
    Field oidField = oidClass.getDeclaredField("GSS_KRB5_MECH_OID");
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
}
