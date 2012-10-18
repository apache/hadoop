package org.apache.hadoop.cmake.maven.ng;

/*
 * Copyright 2012 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.maven.plugin.MojoExecutionException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

/**
 * Utilities.
 */
public class Utils {
  static void validatePlatform() throws MojoExecutionException {
    if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
      throw new MojoExecutionException("CMake-NG does not (yet) support " +
          "the Windows platform.");
    }
  }

  /**
   * Validate that the parameters look sane.
   */
  static void validateParams(File output, File source)
      throws MojoExecutionException {
    String cOutput = null, cSource = null;
    try {
      cOutput = output.getCanonicalPath();
    } catch (IOException e) {
      throw new MojoExecutionException("error getting canonical path " +
          "for output");
    }
    try {
      cSource = source.getCanonicalPath();
    } catch (IOException e) {
      throw new MojoExecutionException("error getting canonical path " +
          "for source");
    }
    
    // This doesn't catch all the bad cases-- we could be following symlinks or
    // hardlinks, etc.  However, this will usually catch a common mistake.
    if (cSource.startsWith(cOutput)) {
      throw new MojoExecutionException("The source directory must not be " +
          "inside the output directory (it would be destroyed by " +
          "'mvn clean')");
    }
  }

  /**
   * Add environment variables to a ProcessBuilder.
   */
  static void addEnvironment(ProcessBuilder pb, Map<String, String> env) {
    if (env == null) {
      return;
    }
    Map<String, String> processEnv = pb.environment();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      String val = entry.getValue();
      if (val == null) {
        val = "";
      }
      processEnv.put(entry.getKey(), val);
    }
  }

  /**
   * Pretty-print the environment.
   */
  static void envronmentToString(StringBuilder bld, Map<String, String> env) {
    if ((env == null) || (env.isEmpty())) {
      return;
    }
    bld.append("ENV: ");
    for (Map.Entry<String, String> entry : env.entrySet()) {
      String val = entry.getValue();
      if (val == null) {
        val = "";
      }
      bld.append(entry.getKey()).
            append(" = ").append(val).append("\n");
    }
    bld.append("=======================================" +
        "========================================\n");
  }

  /**
   * This thread reads the output of the a subprocess and buffers it.
   *
   * Note that because of the way the Java Process APIs are designed, even
   * if we didn't intend to ever display this output, we still would
   * have to read it.  We are connected to the subprocess via a blocking pipe,
   * and if we stop draining our end of the pipe, the subprocess will
   * eventually be blocked if it writes enough to stdout/stderr.
   */
  public static class OutputBufferThread extends Thread {
    private InputStreamReader reader;
    private ArrayList<char[]> bufs;
    
    public OutputBufferThread(InputStream is) 
        throws UnsupportedEncodingException {
      this.reader = new InputStreamReader(is, "UTF8");
      this.bufs = new ArrayList<char[]>();
    }

    public void run() {
      try {
        char[] arr = new char[8192];
        while (true) {
          int amt = reader.read(arr);
          if (amt < 0) return;
          char[] arr2 = new char[amt];
          for (int i = 0; i < amt; i++) {
            arr2[i] = arr[i];
          }
          bufs.add(arr2);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    public void printBufs() {
      for (char[] b : bufs) {
        System.out.print(b);
      }
    }
  }

  /**
   * This thread reads the output of the a subprocess and writes it to a
   * thread.  There is an easier way to do this in Java 7, but we want to stay
   * compatible with old JDK versions.
   */
  public static class OutputToFileThread extends Thread {
    private InputStream is;
    private FileOutputStream out;

    private static void writePrefix(File outFile, String prefix) 
        throws IOException {
      if ((prefix == null) || (prefix.equals(""))) {
        return;
      }
      FileOutputStream fos = new FileOutputStream(outFile, false);
      BufferedWriter wr = null;
      try {
        wr = new BufferedWriter(new OutputStreamWriter(fos, "UTF8"));
        wr.write(prefix);
      } finally {
        if (wr != null) {
          wr.close();
        } else {
          fos.close();
        }
      }
    }

    public OutputToFileThread(InputStream is, File outFile, String prefix) 
        throws IOException {
      this.is = is;
      writePrefix(outFile, prefix);
      this.out = new FileOutputStream(outFile, true);
    }

    public void run() {
      byte[] arr = new byte[8192];
      try {
        while (true) {
          int amt = is.read(arr);
          if (amt < 0) return;
          out.write(arr, 0, amt);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        close();
      }
    }

    public void close() {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          is = null;
        }
      }
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          out = null;
        }
      }
    }
  }
}
