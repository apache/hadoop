/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.hql;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.RunJar;

/**
 * Run hadoop jar commands.
 */
public class JarCommand extends BasicCommand {
  private List<String> query;

  public JarCommand(Writer o) {
    super(o);
  }

  @SuppressWarnings("deprecation")
  public ReturnMsg execute(@SuppressWarnings("unused")
  HBaseConfiguration conf) {

    try {
      String[] args = getQuery();
      String usage = "JAR jarFile [mainClass] args...;\n";

      if (args.length < 1) {
        return new ReturnMsg(0, usage);
      }

      int firstArg = 0;
      String fileName = args[firstArg++];
      File file = new File(fileName);
      String mainClassName = null;

      JarFile jarFile;
      try {
        jarFile = new JarFile(fileName);
      } catch (IOException io) {
        throw new IOException("Error opening job jar: " + fileName + "\n")
            .initCause(io);
      }

      Manifest manifest = jarFile.getManifest();
      if (manifest != null) {
        mainClassName = manifest.getMainAttributes().getValue("Main-Class");
      }
      jarFile.close();

      if (mainClassName == null) {
        if (args.length < 2) {
          return new ReturnMsg(0, usage);
        }
        mainClassName = args[firstArg++];
      }
      mainClassName = mainClassName.replaceAll("/", ".");

      File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
      tmpDir.mkdirs();
      if (!tmpDir.isDirectory()) {
        return new ReturnMsg(0, "Mkdirs failed to create " + tmpDir + "\n");
      }
      final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
      workDir.delete();
      workDir.mkdirs();
      if (!workDir.isDirectory()) {
        return new ReturnMsg(0, "Mkdirs failed to create " + workDir + "\n");
      }

      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            FileUtil.fullyDelete(workDir);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });

      RunJar.unJar(file, workDir);

      ArrayList<URL> classPath = new ArrayList<URL>();
      classPath.add(new File(workDir + "/").toURL());
      classPath.add(file.toURL());
      classPath.add(new File(workDir, "classes/").toURL());
      File[] libs = new File(workDir, "lib").listFiles();
      if (libs != null) {
        for (int i = 0; i < libs.length; i++) {
          classPath.add(libs[i].toURL());
        }
      }
      ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));

      Thread.currentThread().setContextClassLoader(loader);
      Class<?> mainClass = Class.forName(mainClassName, true, loader);
      Method main = mainClass.getMethod("main", new Class[] { Array.newInstance(
          String.class, 0).getClass() });
      String[] newArgs = Arrays.asList(args).subList(firstArg, args.length)
          .toArray(new String[0]);
      try {
        main.invoke(null, new Object[] { newArgs });
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }

    return null;
  }

  public void setQuery(List<String> query) {
    this.query = query;
  }

  private String[] getQuery() {
    return query.toArray(new String[] {});
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SHELL;
  }
}
