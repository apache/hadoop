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

package org.apache.hadoop.util;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A helper class for getting build-info of the java-vm.
 *
 */
@InterfaceAudience.LimitedPrivate({"HBase"})
@InterfaceStability.Unstable
public class PlatformName {
  /**
   * The complete platform 'name' to identify the platform as
   * per the java-vm.
   */
  public static final String PLATFORM_NAME =
      (System.getProperty("os.name").startsWith("Windows") ?
      System.getenv("os") : System.getProperty("os.name"))
      + "-" + System.getProperty("os.arch") + "-"
      + System.getProperty("sun.arch.data.model");

  /**
   * The java vendor name used in this platform.
   */
  public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

  /**
   * Define a system class accessor that is open to changes in underlying implementations
   * of the system class loader modules.
   */
  private static final class SystemClassAccessor extends ClassLoader {
    public Class<?> getSystemClass(String className) throws ClassNotFoundException {
      return findSystemClass(className);
    }
  }

  /**
   * A public static variable to indicate the current java vendor is
   * IBM and the type is Java Technology Edition which provides its
   * own implementations of many security packages and Cipher suites.
   * Note that these are not provided in Semeru runtimes:
   * See https://developer.ibm.com/languages/java/semeru-runtimes for details.
   */
  public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM") &&
      hasIbmTechnologyEditionModules();

  private static boolean hasIbmTechnologyEditionModules() {
    return Arrays.asList(
        "com.ibm.security.auth.module.JAASLoginModule",
        "com.ibm.security.auth.module.Win64LoginModule",
        "com.ibm.security.auth.module.NTLoginModule",
        "com.ibm.security.auth.module.AIX64LoginModule",
        "com.ibm.security.auth.module.LinuxLoginModule",
        "com.ibm.security.auth.module.Krb5LoginModule"
    ).stream().anyMatch((module) -> isSystemClassAvailable(module));
  }

  /**
   * In rare cases where different behaviour is performed based on the JVM vendor
   * this method should be used to test for a unique JVM class provided by the
   * vendor rather than using the vendor method. For example if on JVM provides a
   * different Kerberos login module testing for that login module being loadable
   * before configuring to use it is preferable to using the vendor data.
   *
   * @param className the name of a class in the JVM to test for
   * @return true if the class is available, false otherwise.
   */
  private static boolean isSystemClassAvailable(String className) {
    return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
      try {
        // Using ClassLoader.findSystemClass() instead of
        // Class.forName(className, false, null) because Class.forName with a null
        // ClassLoader only looks at the boot ClassLoader with Java 9 and above
        // which doesn't look at all the modules available to the findSystemClass.
        new SystemClassAccessor().getSystemClass(className);
        return true;
      } catch (Exception ignored) {
        return false;
      }
    });
  }

  public static void main(String[] args) {
    System.out.println(PLATFORM_NAME);
  }
}
