/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.text.DecimalFormat;
import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.*;

/** Utilities not available elsewhere in Hadoop.
 *  
 */
public class StreamUtil
{

  public static Class goodClassOrNull(String className, String defaultPackage)
  {
    if(className.indexOf('.') == -1 && defaultPackage != null) {
        className = defaultPackage + "." + className;
    }
    Class clazz = null;
    try {
        clazz = Class.forName(className);
    } catch(ClassNotFoundException cnf) {
    } catch(LinkageError cnf) {
    }
    return clazz;
  }
  
   /** @return a jar file path or a base directory or null if not found.
   */
   public static String findInClasspath(String className) 
   {

    String relPath = className;
    if (!relPath.startsWith("/")) {
      relPath = "/" + relPath;
    }
    relPath = relPath.replace('.', '/');
    relPath += ".class";

    java.net.URL classUrl = StreamUtil.class.getResource(relPath);

    String codePath;
    if (classUrl != null) {
        boolean inJar = classUrl.getProtocol().equals("jar");
        codePath = classUrl.toString();
        if(codePath.startsWith("jar:")) {
            codePath = codePath.substring("jar:".length());
        }
        if(codePath.startsWith("file:")) { // can have both
            codePath = codePath.substring("file:".length());
        }
        if(inJar) {          
          // A jar spec: remove class suffix in /path/my.jar!/package/Class
          int bang = codePath.lastIndexOf('!');
          codePath = codePath.substring(0, bang);
        } else {
          // A class spec: remove the /my/package/Class.class portion
          int pos = codePath.lastIndexOf(relPath);
          if(pos == -1) {
            throw new IllegalArgumentException(
              "invalid codePath: className=" + className + " codePath=" + codePath);
          }
          codePath = codePath.substring(0, pos);
        }
    } else {
        codePath = null;
    }
    return codePath;
  }

  // copied from TaskRunner  
  static void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry)entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            file.getParentFile().mkdirs();
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
  

  
  final static long KB = 1024L * 1;
  final static long MB = 1024L * KB;
  final static long GB = 1024L * MB;
  final static long TB = 1024L * GB;
  final static long PB = 1024L * TB;

  static DecimalFormat dfm = new DecimalFormat("####.000");
  static DecimalFormat ifm = new DecimalFormat("###,###,###,###,###");
  
  public static String dfmt(double d)
  {
    return dfm.format(d);
  }
  public static String ifmt(double d)
  {
    return ifm.format(d);
  }
  
  public static String formatBytes(long numBytes)
  {
    StringBuffer buf = new StringBuffer();
    boolean bDetails = true;    
    double num = numBytes;
    
    if(numBytes < KB) {
      buf.append(numBytes + " B");
      bDetails = false;
    } else if(numBytes < MB) {
      buf.append(dfmt(num/KB) + " KB");
    } else if(numBytes < GB) {
      buf.append(dfmt(num/MB) + " MB");
    } else if(numBytes < TB) {
      buf.append(dfmt(num/GB) + " GB");
    } else if(numBytes < PB) {
      buf.append(dfmt(num/TB) + " TB");
    } else {
      buf.append(dfmt(num/PB) + " PB");
    }
    if(bDetails) {
      buf.append(" (" + ifmt(numBytes) + " bytes)");
    }
    return buf.toString();
  }

  public static String formatBytes2(long numBytes)
  {
    StringBuffer buf = new StringBuffer();
    long u = 0;
    if(numBytes >= TB) {
      u = numBytes/TB;
      numBytes -= u*TB;
      buf.append(u + " TB ");
    }
    if(numBytes >= GB) {
      u = numBytes/GB;
      numBytes -= u*GB;
      buf.append(u + " GB ");
    }
    if(numBytes >= MB) {
      u = numBytes/MB;
      numBytes -= u*MB;
      buf.append(u + " MB ");
    }
    if(numBytes >= KB) {
      u = numBytes/KB;
      numBytes -= u*KB;
      buf.append(u + " KB ");
    }
    buf.append(u + " B"); //even if zero
    return buf.toString();
  }

  static Environment env;
  static String HOST;
  
  static {
    try {
      env = new Environment();
      HOST = env.getHost();
    } catch(IOException io) {
      io.printStackTrace();
    }
  }

  static class StreamConsumer extends Thread
  {
    StreamConsumer(InputStream in, OutputStream out)
    {
      this.bin = new LineNumberReader(
        new BufferedReader(new InputStreamReader(in)));
      if(out != null) {
        this.bout = new DataOutputStream(out);
      }
    }
    public void run()
    {
      try {
        String line;
        while((line=bin.readLine()) != null) {
          if(bout != null) {
            bout.writeUTF(line); //writeChars
            bout.writeChar('\n');
          }
        }
        bout.flush();
      } catch(IOException io) {        
      }
    }
    LineNumberReader bin;
    DataOutputStream bout;
  }

  static void exec(String arg, PrintStream log)
  {
    exec( new String[] {arg}, log );
  }
  
  static void exec(String[] args, PrintStream log)
  {
      try {
        log.println("Exec: start: " + Arrays.asList(args));
        Process proc = Runtime.getRuntime().exec(args);
        new StreamConsumer(proc.getErrorStream(), log).start();
        new StreamConsumer(proc.getInputStream(), log).start();
        int status = proc.waitFor();
        //if status != 0
        log.println("Exec: status=" + status + ": " + Arrays.asList(args));
      } catch(InterruptedException in) {
        in.printStackTrace();
      } catch(IOException io) {
        io.printStackTrace();
      }
  }
  
  static String qualifyHost(String url)
  {
    try {
        return qualifyHost(new URL(url)).toString();
    } catch(IOException io) {
        return url;
    }
  }
  
  static URL qualifyHost(URL url)
  {    
    try {
      InetAddress a = InetAddress.getByName(url.getHost());
      String qualHost = a.getCanonicalHostName();
      URL q = new URL(url.getProtocol(), qualHost, url.getPort(), url.getFile());
      return q;
    } catch(IOException io) {
      return url;
    }
  }
  
  static final String regexpSpecials = "[]()?*+|.!^-\\~@";
  
  public static String regexpEscape(String plain)
  {
    StringBuffer buf = new StringBuffer();
    char[] ch = plain.toCharArray();
    int csup = ch.length;
    for(int c=0; c<csup; c++) {
      if(regexpSpecials.indexOf(ch[c]) != -1) {
        buf.append("\\");    
      }
      buf.append(ch[c]);
    }
    return buf.toString();
  }
  
  static String slurp(File f) throws IOException
  {
    FileInputStream in = new FileInputStream(f);
    int len = (int)f.length();
    byte[] buf = new byte[len];
    in.read(buf, 0, len);
    return new String(buf);
  }
  
  static private Environment env_;
  
  static Environment env()
  {
    if(env_ != null) {
      return env_;
    }
    try {
      env_ = new Environment();
    } catch(IOException io) {      
      io.printStackTrace();
    }
    return env_;
  }
  
}
