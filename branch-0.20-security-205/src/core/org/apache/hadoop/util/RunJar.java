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

import java.util.jar.*;
import java.lang.reflect.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

/** Run a Hadoop job jar. */
public class RunJar {

  /** Unpack a jar file into a directory. */
  public static void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry)entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create " + 
                                      file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }

  /** Run a Hadoop job jar.  If the main class is not in the jar's manifest,
   * then it must be provided on the command line. */
  public static void main(String[] args) throws Throwable {
    String usage = "RunJar jarFile [mainClass] args...";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    int firstArg = 0;
    String fileName = args[firstArg++];
    File file = new File(fileName);
    String mainClassName = null;

    JarFile jarFile;
    try {
      jarFile = new JarFile(fileName);
    } catch(IOException io) {
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

    File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
    tmpDir.mkdirs();
    if (!tmpDir.isDirectory()) { 
      System.err.println("Mkdirs failed to create " + tmpDir);
      System.exit(-1);
    }
    final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
    workDir.delete();
    workDir.mkdirs();
    if (!workDir.isDirectory()) {
      System.err.println("Mkdirs failed to create " + workDir);
      System.exit(-1);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            FileUtil.fullyDelete(workDir);
          } catch (IOException e) {
          }
        }
      });

    unJar(file, workDir);
    
    ArrayList<URL> classPath = new ArrayList<URL>();
    classPath.add(new File(workDir+"/").toURL());
    classPath.add(file.toURL());
    classPath.add(new File(workDir, "classes/").toURL());
    File[] libs = new File(workDir, "lib").listFiles();
    if (libs != null) {
      for (int i = 0; i < libs.length; i++) {
        classPath.add(libs[i].toURL());
      }
    }
    
    ClassLoader loader =
      new URLClassLoader(classPath.toArray(new URL[0]));

    Thread.currentThread().setContextClassLoader(loader);
    Class<?> mainClass = Class.forName(mainClassName, true, loader);
    Method main = mainClass.getMethod("main", new Class[] {
      Array.newInstance(String.class, 0).getClass()
    });
    String[] newArgs = Arrays.asList(args)
      .subList(firstArg, args.length).toArray(new String[0]);
    try {
      main.invoke(null, new Object[] { newArgs });
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }
  
}
