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
package org.apache.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants;
import org.apache.hadoop.util.Shell;

/** Filesystem disk space usage statistics.  Uses the unix 'du' program*/
public class DU extends Shell {
  private String  dirPath;

  private long used;
  
  public DU(File path, long interval) throws IOException {
    super(interval);
    this.dirPath = path.getCanonicalPath();
  }
  
  public DU(File path, Configuration conf) throws IOException {
    this(path, conf.getLong("dfs.blockreport.intervalMsec",
        FSConstants.BLOCKREPORT_INTERVAL));
  }
  
  synchronized public void decDfsUsed(long value) {
    used -= value;
  }

  synchronized public void incDfsUsed(long value) {
    used += value;
  }
  
  synchronized public long getUsed() throws IOException { 
    run();
    return used;
  }

  public String getDirPath() {
    return dirPath;
  }
  
  
  public String toString() {
    return
      "du -s " + dirPath +"\n" +
      used + "\t" + dirPath;
  }

  protected String[] getExecString() {
    return new String[] {"du","-s", dirPath};
  }
  
  protected void parseExecResult(BufferedReader lines) throws IOException {
    String line = lines.readLine();
    if (line == null) {
      throw new IOException( "Expecting a line not the end of stream" );
    }
    String[] tokens = line.split("\t");
    if(tokens.length == 0) {
      throw new IOException("Illegal du output");
    }
    this.used = Long.parseLong(tokens[0])*1024;
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0)
      path = args[0];

    System.out.println(new DU(new File(path), new Configuration()).toString());
  }
}
