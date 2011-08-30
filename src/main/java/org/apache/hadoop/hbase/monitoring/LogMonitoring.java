/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.monitoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.Enumeration;
import java.util.Set;

import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

/**
 * Utility functions for reading the log4j logs that are
 * being written by HBase.
 */
public abstract class LogMonitoring {
  public static Set<File> getActiveLogFiles() throws IOException {
    Set<File> ret = Sets.newHashSet();
    Appender a;
    @SuppressWarnings("unchecked")
    Enumeration<Appender> e = Logger.getRootLogger().getAllAppenders();
    while (e.hasMoreElements()) {
      a = e.nextElement();
      if (a instanceof FileAppender) {
        FileAppender fa = (FileAppender) a;
        String filename = fa.getFile();
        ret.add(new File(filename));
      }
    }
    return ret;
  }
  

  public static void dumpTailOfLogs(
      PrintWriter out, long tailKb) throws IOException {
    Set<File> logs = LogMonitoring.getActiveLogFiles();
    for (File f : logs) {
      out.println("+++++++++++++++++++++++++++++++");
      out.println(f.getAbsolutePath());
      out.println("+++++++++++++++++++++++++++++++");
      try {
        dumpTailOfLog(f, out, tailKb);
      } catch (IOException ioe) {
        out.println("Unable to dump log at " + f);
        ioe.printStackTrace(out);
      }
      out.println("\n\n");
    }
  }

  private static void dumpTailOfLog(File f, PrintWriter out, long tailKb)
      throws IOException {
    FileInputStream fis = new FileInputStream(f);
    try {
      FileChannel channel = fis.getChannel();
      channel.position(Math.max(0, channel.size() - tailKb*1024));
      BufferedReader r = new BufferedReader(
          new InputStreamReader(fis));
      r.readLine(); // skip the first partial line
      String line;
      while ((line = r.readLine()) != null) {
        out.println(line);
      }
    } finally {
      IOUtils.closeStream(fis);
    }
  }
}
