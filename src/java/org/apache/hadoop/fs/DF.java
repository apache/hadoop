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

import java.util.EnumSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;

/** Filesystem disk space usage statistics.  Uses the unix 'df' program.
 * Tested on Linux, FreeBSD, Cygwin. */
public class DF extends Shell {
  public static final long DF_INTERVAL_DEFAULT = 3 * 1000; // default DF refresh interval 
  
  private String dirPath;
  private String filesystem;
  private long capacity;
  private long used;
  private long available;
  private int percentUsed;
  private String mount;

  enum OSType {
    OS_TYPE_UNIX("UNIX"),
    OS_TYPE_WIN("Windows"),
    OS_TYPE_SOLARIS("SunOS"),
    OS_TYPE_MAC("Mac"),
    OS_TYPE_AIX("AIX");

    private String id;
    OSType(String id) {
      this.id = id;
    }
    public boolean match(String osStr) {
      return osStr != null && osStr.indexOf(id) >= 0;
    }
    String getId() {
      return id;
    }
  }

  private static final String OS_NAME = System.getProperty("os.name");
  private static final OSType OS_TYPE = getOSType(OS_NAME);

  protected static OSType getOSType(String osName) {
    for (OSType ost : EnumSet.allOf(OSType.class)) {
      if (ost.match(osName)) {
        return ost;
      }
    }
    return OSType.OS_TYPE_UNIX;
  }

  public DF(File path, Configuration conf) throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, DF.DF_INTERVAL_DEFAULT));
  }

  public DF(File path, long dfInterval) throws IOException {
    super(dfInterval);
    this.dirPath = path.getCanonicalPath();
  }

  protected OSType getOSType() {
    return OS_TYPE;
  }
  
  /// ACCESSORS

  public String getDirPath() {
    return dirPath;
  }
  
  public String getFilesystem() throws IOException { 
    run(); 
    return filesystem; 
  }
  
  public long getCapacity() throws IOException { 
    run(); 
    return capacity; 
  }
  
  public long getUsed() throws IOException { 
    run(); 
    return used;
  }
  
  public long getAvailable() throws IOException { 
    run(); 
    return available;
  }
  
  public int getPercentUsed() throws IOException {
    run();
    return percentUsed;
  }

  public String getMount() throws IOException {
    run();
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

  @Override
  protected String[] getExecString() {
    // ignoring the error since the exit code it enough
    return new String[] {"bash","-c","exec 'df' '-k' '" + dirPath 
                         + "' 2>/dev/null"};
  }

  @Override
  protected void parseExecResult(BufferedReader lines) throws IOException {
    lines.readLine();                         // skip headings
  
    String line = lines.readLine();
    if (line == null) {
      throw new IOException( "Expecting a line not the end of stream" );
    }
    StringTokenizer tokens =
      new StringTokenizer(line, " \t\n\r\f%");
    
    this.filesystem = tokens.nextToken();
    if (!tokens.hasMoreTokens()) {            // for long filesystem name
      line = lines.readLine();
      if (line == null) {
        throw new IOException( "Expecting a line not the end of stream" );
      }
      tokens = new StringTokenizer(line, " \t\n\r\f%");
    }

    switch(getOSType()) {
      case OS_TYPE_AIX:
        this.capacity = Long.parseLong(tokens.nextToken()) * 1024;
        this.available = Long.parseLong(tokens.nextToken()) * 1024;
        this.percentUsed = Integer.parseInt(tokens.nextToken());
        tokens.nextToken();
        tokens.nextToken();
        this.mount = tokens.nextToken();
        this.used = this.capacity - this.available;
        break;

      case OS_TYPE_WIN:
      case OS_TYPE_SOLARIS:
      case OS_TYPE_MAC:
      case OS_TYPE_UNIX:
      default:
        this.capacity = Long.parseLong(tokens.nextToken()) * 1024;
        this.used = Long.parseLong(tokens.nextToken()) * 1024;
        this.available = Long.parseLong(tokens.nextToken()) * 1024;
        this.percentUsed = Integer.parseInt(tokens.nextToken());
        this.mount = tokens.nextToken();
        break;
   }
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0)
      path = args[0];

    System.out.println(new DF(new File(path), DF_INTERVAL_DEFAULT).toString());
  }
}
