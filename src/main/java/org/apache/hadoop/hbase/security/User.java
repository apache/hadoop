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
   * @return
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
  public static User getCurrent() {
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

  /* Concrete implementations */

  /**
   * Bridges {@link User} calls to invocations of the appropriate methods
   * in {@link org.apache.hadoop.security.UserGroupInformation} in regular
   * Hadoop 0.20 (ASF Hadoop and other versions without the backported security
   * features).
   */
  private static class HadoopUser extends User {

    private HadoopUser() {
      ugi = (UserGroupInformation) callStatic("getCurrentUGI");
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
      UserGroupInformation previous =
          (UserGroupInformation) callStatic("getCurrentUGI");
      if (ugi != null) {
        callStatic("setCurrentUser", new Class[]{UserGroupInformation.class},
            new Object[]{ugi});
      }
      T result = action.run();
      callStatic("setCurrentUser", new Class[]{UserGroupInformation.class},
          new Object[]{previous});
      return result;
    }

    @Override
    public <T> T runAs(PrivilegedExceptionAction<T> action)
        throws IOException, InterruptedException {
      UserGroupInformation previous =
          (UserGroupInformation) callStatic("getCurrentUGI");
      if (ugi != null) {
        callStatic("setCurrentUGI", new Class[]{UserGroupInformation.class},
            new Object[]{ugi});
      }
      T result = null;
      try {
        result = action.run();
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
      } finally {
        callStatic("setCurrentUGI", new Class[]{UserGroupInformation.class},
            new Object[]{previous});
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
        LOG.error("UnixUserGroupInformation not found, is this secure Hadoop?", cnfe);
      } catch (NoSuchMethodException nsme) {
        LOG.error("No valid constructor found for UnixUserGroupInformation!", nsme);
      } catch (Exception e) {
        LOG.error("Error instantiating new UnixUserGroupInformation", e);
      }

      return null;
    }
  }

  /**
   * Bridges {@code User} invocations to underlying calls to
   * {@link org.apache.hadoop.security.UserGroupInformation} for secure Hadoop
   * 0.20 and versions 0.21 and above.
   */
  private static class SecureHadoopUser extends User {
    private SecureHadoopUser() {
      ugi = (UserGroupInformation) callStatic("getCurrentUser");
    }

    private SecureHadoopUser(UserGroupInformation ugi) {
      this.ugi = ugi;
    }

    @Override
    public String getShortName() {
      return (String)call(ugi, "getShortUserName", null, null);
    }

    @Override
    public <T> T runAs(PrivilegedAction<T> action) {
      return (T) call(ugi, "doAs", new Class[]{PrivilegedAction.class},
          new Object[]{action});
    }

    @Override
    public <T> T runAs(PrivilegedExceptionAction<T> action)
        throws IOException, InterruptedException {
      return (T) call(ugi, "doAs",
          new Class[]{PrivilegedExceptionAction.class},
          new Object[]{action});
    }

    public static User createUserForTesting(Configuration conf,
        String name, String[] groups) {
      return new SecureHadoopUser(
          (UserGroupInformation)callStatic("createUserForTesting",
              new Class[]{String.class, String[].class},
              new Object[]{name, groups})
      );
    }
  }

  /* Reflection helper methods */
  private static Object callStatic(String methodName) {
    return call(null, methodName, null, null);
  }

  private static Object callStatic(String methodName, Class[] types,
      Object[] args) {
    return call(null, methodName, types, args);
  }

  private static Object call(UserGroupInformation instance, String methodName,
      Class[] types, Object[] args) {
    try {
      Method m = UserGroupInformation.class.getMethod(methodName, types);
      return m.invoke(instance, args);
    } catch (NoSuchMethodException nsme) {
      LOG.fatal("Can't find method "+methodName+" in UserGroupInformation!",
          nsme);
    } catch (Exception e) {
      LOG.fatal("Error calling method "+methodName, e);
    }
    return null;
  }
}
