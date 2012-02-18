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

package org.apache.hadoop.streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;

/** 
 * Utilities used in streaming
 */
@InterfaceAudience.Private
public class StreamUtil {

  /** It may seem strange to silently switch behaviour when a String
   * is not a classname; the reason is simplified Usage:<pre>
   * -mapper [classname | program ]
   * instead of the explicit Usage:
   * [-mapper program | -javamapper classname], -mapper and -javamapper are mutually exclusive.
   * (repeat for -reducer, -combiner) </pre>
   */
  public static Class goodClassOrNull(Configuration conf, String className, String defaultPackage) {
    Class clazz = null;
    try {
      clazz = conf.getClassByName(className);
    } catch (ClassNotFoundException cnf) {
    }
    if (clazz == null) {
      if (className.indexOf('.') == -1 && defaultPackage != null) {
        className = defaultPackage + "." + className;
        try {
          clazz = conf.getClassByName(className);
        } catch (ClassNotFoundException cnf) {
        }
      }
    }
    return clazz;
  }

  public static String findInClasspath(String className) {
    return findInClasspath(className, StreamUtil.class.getClassLoader());
  }

  /** @return a jar file path or a base directory or null if not found.
   */
  public static String findInClasspath(String className, ClassLoader loader) {

    String relPath = className;
    relPath = relPath.replace('.', '/');
    relPath += ".class";
    java.net.URL classUrl = loader.getResource(relPath);

    String codePath;
    if (classUrl != null) {
      boolean inJar = classUrl.getProtocol().equals("jar");
      codePath = classUrl.toString();
      if (codePath.startsWith("jar:")) {
        codePath = codePath.substring("jar:".length());
      }
      if (codePath.startsWith("file:")) { // can have both
        codePath = codePath.substring("file:".length());
      }
      if (inJar) {
        // A jar spec: remove class suffix in /path/my.jar!/package/Class
        int bang = codePath.lastIndexOf('!');
        codePath = codePath.substring(0, bang);
      } else {
        // A class spec: remove the /my/package/Class.class portion
        int pos = codePath.lastIndexOf(relPath);
        if (pos == -1) {
          throw new IllegalArgumentException("invalid codePath: className=" + className
                                             + " codePath=" + codePath);
        }
        codePath = codePath.substring(0, pos);
      }
    } else {
      codePath = null;
    }
    return codePath;
  }

  static String qualifyHost(String url) {
    try {
      return qualifyHost(new URL(url)).toString();
    } catch (IOException io) {
      return url;
    }
  }

  static URL qualifyHost(URL url) {
    try {
      InetAddress a = InetAddress.getByName(url.getHost());
      String qualHost = a.getCanonicalHostName();
      URL q = new URL(url.getProtocol(), qualHost, url.getPort(), url.getFile());
      return q;
    } catch (IOException io) {
      return url;
    }
  }

  static final String regexpSpecials = "[]()?*+|.!^-\\~@";

  public static String regexpEscape(String plain) {
    StringBuffer buf = new StringBuffer();
    char[] ch = plain.toCharArray();
    int csup = ch.length;
    for (int c = 0; c < csup; c++) {
      if (regexpSpecials.indexOf(ch[c]) != -1) {
        buf.append("\\");
      }
      buf.append(ch[c]);
    }
    return buf.toString();
  }

  static String slurp(File f) throws IOException {
    int len = (int) f.length();
    byte[] buf = new byte[len];
    FileInputStream in = new FileInputStream(f);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  static String slurpHadoop(Path p, FileSystem fs) throws IOException {
    int len = (int) fs.getFileStatus(p).getLen();
    byte[] buf = new byte[len];
    FSDataInputStream in = fs.open(p);
    String contents = null;
    try {
      in.readFully(in.getPos(), buf);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  static private Environment env;
  static String HOST;

  static {
    try {
      env = new Environment();
      HOST = env.getHost();
    } catch (IOException io) {
      io.printStackTrace();
    }
  }

  static Environment env() {
    if (env != null) {
      return env;
    }
    try {
      env = new Environment();
    } catch (IOException io) {
      io.printStackTrace();
    }
    return env;
  }

  public static boolean isLocalJobTracker(JobConf job) {
    String framework = 
        job.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME); 
    return framework.equals(MRConfig.LOCAL_FRAMEWORK_NAME);
  }
}
