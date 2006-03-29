/**
 * Copyright 2005 The Apache Software Foundation
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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

/** Filesystem disk space usage statistics.  Uses the unix 'df' program.
 * Tested on Linux, FreeBSD, Cygwin. */
public class DF {
  public static final long DF_INTERVAL_DEFAULT = 3 * 1000; // default DF refresh interval 
  
  private String  dirPath;
  private long    dfInterval;	// DF refresh interval in msec
  private long    lastDF;		// last time doDF() was performed
  
  private String filesystem;
  private long capacity;
  private long used;
  private long available;
  private int percentUsed;
  private String mount;
  
  public DF(String path, Configuration conf ) throws IOException {
    this( path, conf.getLong( "dfs.df.interval", DF.DF_INTERVAL_DEFAULT ));
  }

  public DF(String path, long dfInterval) throws IOException {
    this.dirPath = path;
    this.dfInterval = dfInterval;
    lastDF = ( dfInterval < 0 ) ? 0 : -dfInterval;
    this.doDF();
  }
  
  private void doDF() throws IOException { 
    if( lastDF + dfInterval > System.currentTimeMillis() )
      return;
    Process process;
    process = Runtime.getRuntime().exec(getExecString());

    try {
      if (process.waitFor() != 0) {
        throw new IOException
        (new BufferedReader(new InputStreamReader(process.getErrorStream()))
         .readLine());
      }
      parseExecResult(
        new BufferedReader(new InputStreamReader(process.getInputStream())));
    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      process.destroy();
    }
  }

  /// ACCESSORS

  public String getDirPath() {
    return dirPath;
  }
  
  public String getFilesystem() throws IOException { 
    doDF(); 
    return filesystem; 
  }
  
  public long getCapacity() throws IOException { 
    doDF(); 
    return capacity; 
  }
  
  public long getUsed() throws IOException { 
    doDF(); 
    return used;
  }
  
  public long getAvailable() throws IOException { 
    doDF(); 
    return available;
  }
  
  public int getPercentUsed() throws IOException {
    doDF();
    return percentUsed;
  }

  public String getMount() throws IOException {
    doDF();
    return mount;
  }
  
  public String toString() {
    return
      "df -k " + mount +"\n" +
      filesystem + "\t" +
      capacity / 1024 + "\t" +
      used / 1024 + "\t" +
      available / 1024 + "\t" +
      percentUsed + "%\t" +
      mount;
  }

  private String[] getExecString() {
    return new String[] {"df","-k",dirPath};
  }
  
  private void parseExecResult( BufferedReader lines ) throws IOException {
    lines.readLine();                         // skip headings
  
    StringTokenizer tokens =
      new StringTokenizer(lines.readLine(), " \t\n\r\f%");
    
    this.filesystem = tokens.nextToken();
    if (!tokens.hasMoreTokens()) {            // for long filesystem name
      tokens = new StringTokenizer(lines.readLine(), " \t\n\r\f%");
    }
    this.capacity = Long.parseLong(tokens.nextToken()) * 1024;
    this.used = Long.parseLong(tokens.nextToken()) * 1024;
    this.available = Long.parseLong(tokens.nextToken()) * 1024;
    this.percentUsed = Integer.parseInt(tokens.nextToken());
    this.mount = tokens.nextToken();
    this.lastDF = System.currentTimeMillis();
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if( args.length > 0 )
      path = args[0];

    System.out.println(new DF(path, DF_INTERVAL_DEFAULT).toString());
  }
}
