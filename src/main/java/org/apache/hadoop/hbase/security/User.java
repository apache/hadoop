/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.security;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import org.apache.commons.logging.Log;

/**
 * Wrapper to abstract out usage of user and group information in HBase.
 *
 * <p>
 * This class provides a common interface for interacting with user and group
 * information across changing APIs in different versions of Hadoop.  It only
 * provides access to the common set of functionality in
 * {@link org.apache.hadoop.security.UserGroupInformation} currently needed by
 * HBase, but can be extended as needs change.
 * </p>
 *
 * <p>
 * Note: this class does not attempt to support any of the Kerberos
 * authentication methods exposed in security-enabled Hadoop (for the moment
 * at least), as they're not yet needed.  Properly supporting
 * authentication is left up to implementation in secure HBase.
 * </p>
 */
public abstract class User {
  private static boolean IS_SECURE_HADOOP = true;
  static {
    try {
      UserGroupInformation.class.getMethod("isSecurityEnabled");
    } catch (NoSuchMethodException nsme) {
      IS_SECURE_HADOOP = false;
    }
  }
  private static Log LOG = LogFactory.getLog(User.class);
  protected UserGroupInformation ugi;

  /**
   * Returns the full user name.  For Kerberos principals this will include
   * the host and realm portions of the principal name.
   * @return User full name.
   */
  public String getName() {
    return ugi.getUserName();
  }

  /**
   * Returns the shortened version of the user name -- the portion that maps
   * to an operating system user name.
   * @return Short name
   */
  public abstract String getShortName();

  /**
   * Executes the given action within the context of this user.
   */
  public abstract <T> T runAs(PrivilegedAction<T> action);

  /**
   * Executes the given action within the context of this user.
   */
  public abstract <T> T runAs(PrivilegedExceptionAction<T> action)
      throws IOException, InterruptedException;

  public String toString() {
    return ugi.toString();
  }

  /**
   * Returns the {@code User} instance within current execution context.
   */
  public static User getCurrent() throws IOException {
    if (IS_SECURE_HADOOP) {
      return new SecureHadoopUser();
    } else {
      return new HadoopUser();
    }
  }

  /**
   * Generates a new {@code User} instance specifically for use in test code.
   * @param name the full username
   * @param groups the group names to which the test user will belong
   * @return a new <code>User</code> instance
   */
  public static User createUserForTesting(Configuration conf,
      String name, String[] groups) {
    if (IS_SECURE_HADOOP) {
      return SecureHadoopUser.createUserForTesting(conf, name, groups);
    }
    return HadoopUser.createUserForTesting(conf, name, groups);
  }

  /**
   * Log in the current process using the given configuration keys for the
   * credential file and login principal.
   *
   * <p><strong>This is only applicable when
   * running on secure Hadoop</strong> -- see
   * {@link org.apache.hadoop.security.SecurityUtil#login(Configuration,String,String,String)}.
   * On regular Hadoop (without security features), this will safely be ignored.
   * </p>
   *
   * @param conf The configuration data to use
   * @param fileConfKey Property key used to configure path to the credential file
   * @param principalConfKey Property key used to configure login principal
   * @param localhost Current hostname to use in any credentials
   * @throws IOException underlying exception from SecurityUtil.login() call
   */
  public static void login(Configuration conf, String fileConfKey,
      String principalConfKey, String localhost) throws IOException {
    if (IS_SECURE_HADOOP) {
      SecureHadoopUser.login(conf, fileConfKey, principalConfKey, localhost);
    } else {
      HadoopUser.login(conf, fileConfKey, principalConfKey, localhost);
    }
  }

  /* Concrete implementations */

  /**
   * Bridges {@link User} calls to invocations of the appropriate methods
   * in {@link org.apache.hadoop.security.UserGroupInformation} in regular
   * Hadoop 0.20 (ASF Hadoop and other versions without the backported security
   * features).
   */
  private static class HadoopUser extends User {

    private HadoopUser() {
      try {
        ugi = (UserGroupInformation) callStatic("getCurrentUGI");
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected exception HadoopUser<init>");
      }
    }

    private HadoopUser(UserGroupInformation ugi) {
      this.ugi = ugi;
    }

    @Override
    public String getShortName() {
      return ugi.getUserName();
    }

    @Override
    public <T> T runAs(PrivilegedAction<T> action) {
      T result = null;
      UserGroupInformation previous = null;
      try {
        previous = (UserGroupInformation) callStatic("getCurrentUGI");
        try {
          if (ugi != null) {
            callStatic("setCurrentUser", new Class[]{UserGroupInformation.class},
                new Object[]{ugi});
          }
          result = action.run();
        } finally {
          callStatic("setCurrentUser", new Class[]{UserGroupInformation.class},
              new Object[]{previous});
        }
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected exception in runAs()");
      }
      return result;
    }

    @Override
    public <T> T runAs(PrivilegedExceptionAction<T> action)
        throws IOException, InterruptedException {
      T result = null;
      try {
        UserGroupInformation previous =
            (UserGroupInformation) callStatic("getCurrentUGI");
        try {
          if (ugi != null) {
            callStatic("setCurrentUGI", new Class[]{UserGroupInformation.class},
                new Object[]{ugi});
          }
          result = action.run();
        } finally {
          callStatic("setCurrentUGI", new Class[]{UserGroupInformation.class},
              new Object[]{previous});
        }
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException)e;
        } else if (e instanceof InterruptedException) {
          throw (InterruptedException)e;
        } else if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else {
          throw new UndeclaredThrowableException(e, "Unknown exception in runAs()");
        }
      }
      return result;
    }

    public static User createUserForTesting(Configuration conf,
        String name, String[] groups) {
      try {
        Class c = Class.forName("org.apache.hadoop.security.UnixUserGroupInformation");
        Constructor constructor = c.getConstructor(String.class, String[].class);
        if (constructor == null) {
          throw new NullPointerException(
             );
        }
        UserGroupInformation newUser =
            (UserGroupInformation)constructor.newInstance(name, groups);
        // set user in configuration -- hack for regular hadoop
        conf.set("hadoop.job.ugi", newUser.toString());
        return new HadoopUser(newUser);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(
            "UnixUserGroupInformation not found, is this secure Hadoop?", cnfe);
      } catch (NoSuchMethodException nsme) {
        throw new RuntimeException(
            "No valid constructor found for UnixUserGroupInformation!", nsme);
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected exception instantiating new UnixUserGroupInformation");
      }
    }

    public static void login(Configuration conf, String fileConfKey,
        String principalConfKey, String localhost) throws IOException {
      LOG.info("Skipping login, not running on secure Hadoop");
    }
  }

  /**
   * Bridges {@code User} invocations to underlying calls to
   * {@link org.apache.hadoop.security.UserGroupInformation} for secure Hadoop
   * 0.20 and versions 0.21 and above.
   */
  private static class SecureHadoopUser extends User {
    private SecureHadoopUser() throws IOException {
      try {
        ugi = (UserGroupInformation) callStatic("getCurrentUser");
      } catch (IOException ioe) {
        throw ioe;
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected exception getting current secure user");
      }
    }

    private SecureHadoopUser(UserGroupInformation ugi) {
      this.ugi = ugi;
    }

    @Override
    public String getShortName() {
      try {
        return (String)call(ugi, "getShortUserName", null, null);
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected error getting user short name");
      }
    }

    @Override
    public <T> T runAs(PrivilegedAction<T> action) {
      try {
        return (T) call(ugi, "doAs", new Class[]{PrivilegedAction.class},
            new Object[]{action});
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected exception in runAs()");
      }
    }

    @Override
    public <T> T runAs(PrivilegedExceptionAction<T> action)
        throws IOException, InterruptedException {
      try {
        return (T) call(ugi, "doAs",
            new Class[]{PrivilegedExceptionAction.class},
            new Object[]{action});
      } catch (IOException ioe) {
        throw ioe;
      } catch (InterruptedException ie) {
        throw ie;
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected exception in runAs(PrivilegedExceptionAction)");
      }
    }

    public static User createUserForTesting(Configuration conf,
        String name, String[] groups) {
      try {
        return new SecureHadoopUser(
            (UserGroupInformation)callStatic("createUserForTesting",
                new Class[]{String.class, String[].class},
                new Object[]{name, groups})
        );
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Error creating secure test user");
      }
    }

    public static void login(Configuration conf, String fileConfKey,
        String principalConfKey, String localhost) throws IOException {
      // check for SecurityUtil class
      try {
        Class c = Class.forName("org.apache.hadoop.security.SecurityUtil");
        Class[] types = new Class[]{
            Configuration.class, String.class, String.class, String.class };
        Object[] args = new Object[]{
            conf, fileConfKey, principalConfKey, localhost };
        call(c, null, "login", types, args);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Unable to login using " +
            "org.apache.hadoop.security.Security.login(). SecurityUtil class " +
            "was not found!  Is this a version of secure Hadoop?", cnfe);
      } catch (IOException ioe) {
        throw ioe;
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unhandled exception in User.login()");
      }
    }
  }

  /* Reflection helper methods */
  private static Object callStatic(String methodName) throws Exception {
    return call(null, methodName, null, null);
  }

  private static Object callStatic(String methodName, Class[] types,
      Object[] args) throws Exception {
    return call(null, methodName, types, args);
  }

  private static Object call(UserGroupInformation instance, String methodName,
      Class[] types, Object[] args) throws Exception {
    return call(UserGroupInformation.class, instance, methodName, types, args);
  }

  private static <T> Object call(Class<T> clazz, T instance, String methodName,
      Class[] types, Object[] args) throws Exception {
    try {
      Method m = clazz.getMethod(methodName, types);
      return m.invoke(instance, args);
    } catch (IllegalArgumentException arge) {
      LOG.fatal("Constructed invalid call. class="+clazz.getName()+
          " method=" + methodName + " types=" + stringify(types), arge);
      throw arge;
    } catch (NoSuchMethodException nsme) {
      throw new IllegalArgumentException(
          "Can't find method "+methodName+" in "+clazz.getName()+"!", nsme);
    } catch (InvocationTargetException ite) {
      // unwrap the underlying exception and rethrow
      if (ite.getTargetException() != null) {
        if (ite.getTargetException() instanceof Exception) {
          throw (Exception)ite.getTargetException();
        } else if (ite.getTargetException() instanceof Error) {
          throw (Error)ite.getTargetException();
        }
      }
      throw new UndeclaredThrowableException(ite,
          "Unknown exception invoking "+clazz.getName()+"."+methodName+"()");
    } catch (IllegalAccessException iae) {
      throw new IllegalArgumentException(
          "Denied access calling "+clazz.getName()+"."+methodName+"()", iae);
    } catch (SecurityException se) {
      LOG.fatal("SecurityException calling method. class="+clazz.getName()+
          " method=" + methodName + " types=" + stringify(types), se);
      throw se;
    }
  }

  private static String stringify(Class[] classes) {
    StringBuilder buf = new StringBuilder();
    if (classes != null) {
      for (Class c : classes) {
        if (buf.length() > 0) {
          buf.append(",");
        }
        buf.append(c.getName());
      }
    } else {
      buf.append("NULL");
    }
    return buf.toString();
  }
}
