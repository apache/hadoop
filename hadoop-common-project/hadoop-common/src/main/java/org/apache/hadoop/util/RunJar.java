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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

import org.apache.commons.io.input.TeeInputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IOUtils.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Run a Hadoop job jar. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RunJar {

  private static final Logger LOG = LoggerFactory.getLogger(RunJar.class);

  /** Pattern that matches any string. */
  public static final Pattern MATCH_ANY = Pattern.compile(".*");

  /**
   * Priority of the RunJar shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 10;

  /**
   * Environment key for using the client classloader.
   */
  public static final String HADOOP_USE_CLIENT_CLASSLOADER =
      "HADOOP_USE_CLIENT_CLASSLOADER";
  /**
   * Environment key for the (user-provided) hadoop classpath.
   */
  public static final String HADOOP_CLASSPATH = "HADOOP_CLASSPATH";
  /**
   * Environment key for the system classes.
   */
  public static final String HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES =
      "HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES";
  /**
   * Environment key for disabling unjar in client code.
   */
  public static final String HADOOP_CLIENT_SKIP_UNJAR =
      "HADOOP_CLIENT_SKIP_UNJAR";
  /**
   * Buffer size for copy the content of compressed file to new file.
   */
  private static final int BUFFER_SIZE = 8_192;

  /**
   * Unpack a jar file into a directory.
   *
   * This version unpacks all files inside the jar regardless of filename.
   *
   * @param jarFile the .jar file to unpack
   * @param toDir the destination directory into which to unpack the jar
   *
   * @throws IOException if an I/O error has occurred or toDir
   * cannot be created and does not already exist
   */
  public void unJar(File jarFile, File toDir) throws IOException {
    unJar(jarFile, toDir, MATCH_ANY);
  }

  /**
   * Unpack matching files from a jar. Entries inside the jar that do
   * not match the given pattern will be skipped.
   *
   * @param inputStream the jar stream to unpack
   * @param toDir the destination directory into which to unpack the jar
   * @param unpackRegex the pattern to match jar entries against
   *
   * @throws IOException if an I/O error has occurred or toDir
   * cannot be created and does not already exist
   */
  public static void unJar(InputStream inputStream, File toDir,
                           Pattern unpackRegex)
      throws IOException {
    try (JarInputStream jar = new JarInputStream(inputStream)) {
      int numOfFailedLastModifiedSet = 0;
      String targetDirPath = toDir.getCanonicalPath() + File.separator;
      for (JarEntry entry = jar.getNextJarEntry();
           entry != null;
           entry = jar.getNextJarEntry()) {
        if (!entry.isDirectory() &&
            unpackRegex.matcher(entry.getName()).matches()) {
          File file = new File(toDir, entry.getName());
          if (!file.getCanonicalPath().startsWith(targetDirPath)) {
            throw new IOException("expanding " + entry.getName()
                + " would create file outside of " + toDir);
          }
          ensureDirectory(file.getParentFile());
          try (OutputStream out = new FileOutputStream(file)) {
            IOUtils.copyBytes(jar, out, BUFFER_SIZE);
          }
          if (!file.setLastModified(entry.getTime())) {
            numOfFailedLastModifiedSet++;
          }
        }
      }
      if (numOfFailedLastModifiedSet > 0) {
        LOG.warn("Could not set last modfied time for {} file(s)",
            numOfFailedLastModifiedSet);
      }
      // ZipInputStream does not need the end of the file. Let's read it out.
      // This helps with an additional TeeInputStream on the input.
      IOUtils.copyBytes(inputStream, new NullOutputStream(), BUFFER_SIZE);
    }
  }

  /**
   * Unpack matching files from a jar. Entries inside the jar that do
   * not match the given pattern will be skipped. Keep also a copy
   * of the entire jar in the same directory for backward compatibility.
   * TODO remove this feature in a new release and do only unJar
   *
   * @param inputStream the jar stream to unpack
   * @param toDir the destination directory into which to unpack the jar
   * @param unpackRegex the pattern to match jar entries against
   *
   * @throws IOException if an I/O error has occurred or toDir
   * cannot be created and does not already exist
   */
  @Deprecated
  public static void unJarAndSave(InputStream inputStream, File toDir,
                           String name, Pattern unpackRegex)
      throws IOException{
    File file = new File(toDir, name);
    ensureDirectory(toDir);
    try (OutputStream jar = new FileOutputStream(file);
         TeeInputStream teeInputStream = new TeeInputStream(inputStream, jar)) {
      unJar(teeInputStream, toDir, unpackRegex);
    }
  }

  /**
   * Unpack matching files from a jar. Entries inside the jar that do
   * not match the given pattern will be skipped.
   *
   * @param jarFile the .jar file to unpack
   * @param toDir the destination directory into which to unpack the jar
   * @param unpackRegex the pattern to match jar entries against
   *
   * @throws IOException if an I/O error has occurred or toDir
   * cannot be created and does not already exist
   */
  public static void unJar(File jarFile, File toDir, Pattern unpackRegex)
      throws IOException {
    try (JarFile jar = new JarFile(jarFile)) {
      int numOfFailedLastModifiedSet = 0;
      String targetDirPath = toDir.getCanonicalPath() + File.separator;
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        final JarEntry entry = entries.nextElement();
        if (!entry.isDirectory() &&
            unpackRegex.matcher(entry.getName()).matches()) {
          try (InputStream in = jar.getInputStream(entry)) {
            File file = new File(toDir, entry.getName());
            if (!file.getCanonicalPath().startsWith(targetDirPath)) {
              throw new IOException("expanding " + entry.getName()
                  + " would create file outside of " + toDir);
            }
            ensureDirectory(file.getParentFile());
            try (OutputStream out = new FileOutputStream(file)) {
              IOUtils.copyBytes(in, out, BUFFER_SIZE);
            }
            if (!file.setLastModified(entry.getTime())) {
              numOfFailedLastModifiedSet++;
            }
          }
        }
      }
      if (numOfFailedLastModifiedSet > 0) {
        LOG.warn("Could not set last modfied time for {} file(s)",
            numOfFailedLastModifiedSet);
      }
    }
  }

  /**
   * Ensure the existence of a given directory.
   *
   * @param dir Directory to check
   *
   * @throws IOException if it cannot be created and does not already exist
   */
  private static void ensureDirectory(File dir) throws IOException {
    if (!dir.mkdirs() && !dir.isDirectory()) {
      throw new IOException("Mkdirs failed to create " +
                            dir.toString());
    }
  }

  /** Run a Hadoop job jar.  If the main class is not in the jar's manifest,
   * then it must be provided on the command line. */
  public static void main(String[] args) throws Throwable {
    new RunJar().run(args);
  }

  public void run(String[] args) throws Throwable {
    String usage = "RunJar jarFile [mainClass] args...";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    int firstArg = 0;
    String fileName = args[firstArg++];
    File file = new File(fileName);
    if (!file.exists() || !file.isFile()) {
      System.err.println("JAR does not exist or is not a normal file: " +
          file.getCanonicalPath());
      System.exit(-1);
    }
    String mainClassName = null;

    JarFile jarFile;
    try {
      jarFile = new JarFile(fileName);
    } catch (IOException io) {
      throw new IOException("Error opening job jar: " + fileName)
        .initCause(io);
    }

    Manifest manifest = jarFile.getManifest();
    if (manifest != null) {
      mainClassName = manifest.getMainAttributes().getValue("Main-Class");
    }
    jarFile.close();

    if (mainClassName == null) {
      if (args.length < 2) {
        System.err.println(usage);
        System.exit(-1);
      }
      mainClassName = args[firstArg++];
    }
    mainClassName = mainClassName.replaceAll("/", ".");

    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    ensureDirectory(tmpDir);

    final File workDir;
    try {
      workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
    } catch (IOException ioe) {
      // If user has insufficient perms to write to tmpDir, default
      // "Permission denied" message doesn't specify a filename.
      System.err.println("Error creating temp dir in java.io.tmpdir "
                         + tmpDir + " due to " + ioe.getMessage());
      System.exit(-1);
      return;
    }

    if (!workDir.delete()) {
      System.err.println("Delete failed for " + workDir);
      System.exit(-1);
    }
    ensureDirectory(workDir);

    ShutdownHookManager.get().addShutdownHook(
        new Runnable() {
          @Override
          public void run() {
            FileUtil.fullyDelete(workDir);
          }
        }, SHUTDOWN_HOOK_PRIORITY);

    if (!skipUnjar()) {
      unJar(file, workDir);
    }

    ClassLoader loader = createClassLoader(file, workDir);

    Thread.currentThread().setContextClassLoader(loader);
    Class<?> mainClass = Class.forName(mainClassName, true, loader);
    Method main = mainClass.getMethod("main", String[].class);
    List<String> newArgsSubList = Arrays.asList(args)
        .subList(firstArg, args.length);
    String[] newArgs = newArgsSubList
        .toArray(new String[newArgsSubList.size()]);
    try {
      main.invoke(null, new Object[] {newArgs});
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }

  /**
   * Creates a classloader based on the environment that was specified by the
   * user. If HADOOP_USE_CLIENT_CLASSLOADER is specified, it creates an
   * application classloader that provides the isolation of the user class space
   * from the hadoop classes and their dependencies. It forms a class space for
   * the user jar as well as the HADOOP_CLASSPATH. Otherwise, it creates a
   * classloader that simply adds the user jar to the classpath.
   */
  private ClassLoader createClassLoader(File file, final File workDir)
      throws MalformedURLException {
    ClassLoader loader;
    // see if the client classloader is enabled
    if (useClientClassLoader()) {
      StringBuilder sb = new StringBuilder();
      sb.append(workDir).append("/").
          append(File.pathSeparator).append(file).
          append(File.pathSeparator).append(workDir).append("/classes/").
          append(File.pathSeparator).append(workDir).append("/lib/*");
      // HADOOP_CLASSPATH is added to the client classpath
      String hadoopClasspath = getHadoopClasspath();
      if (hadoopClasspath != null && !hadoopClasspath.isEmpty()) {
        sb.append(File.pathSeparator).append(hadoopClasspath);
      }
      String clientClasspath = sb.toString();
      // get the system classes
      String systemClasses = getSystemClasses();
      List<String> systemClassesList = systemClasses == null ?
          null :
          Arrays.asList(StringUtils.getTrimmedStrings(systemClasses));
      // create an application classloader that isolates the user classes
      loader = new ApplicationClassLoader(clientClasspath,
          getClass().getClassLoader(), systemClassesList);
    } else {
      List<URL> classPath = new ArrayList<>();
      classPath.add(new File(workDir + "/").toURI().toURL());
      classPath.add(file.toURI().toURL());
      classPath.add(new File(workDir, "classes/").toURI().toURL());
      File[] libs = new File(workDir, "lib").listFiles();
      if (libs != null) {
        for (File lib : libs) {
          classPath.add(lib.toURI().toURL());
        }
      }
      // create a normal parent-delegating classloader
      loader = new URLClassLoader(classPath.toArray(new URL[classPath.size()]));
    }
    return loader;
  }

  boolean useClientClassLoader() {
    return Boolean.parseBoolean(System.getenv(HADOOP_USE_CLIENT_CLASSLOADER));
  }

  boolean skipUnjar() {
    return Boolean.parseBoolean(System.getenv(HADOOP_CLIENT_SKIP_UNJAR));
  }

  String getHadoopClasspath() {
    return System.getenv(HADOOP_CLASSPATH);
  }

  String getSystemClasses() {
    return System.getenv(HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES);
  }
}
