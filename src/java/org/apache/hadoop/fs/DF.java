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

/** Filesystem disk space usage statistics.  Uses the unix 'df' program.
 * Tested on Linux, FreeBSD and Cygwin. */
public class DF {
  private String filesystem;
  private long capacity;
  private long used;
  private long available;
  private int percentUsed;
  private String mount;
  
  public DF(String path) throws IOException {

    Process process = Runtime.getRuntime().exec(new String[] {"df","-k",path});

    try {
      if (process.waitFor() == 0) {
        BufferedReader lines =
          new BufferedReader(new InputStreamReader(process.getInputStream()));

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

      } else {
        throw new IOException
          (new BufferedReader(new InputStreamReader(process.getErrorStream()))
           .readLine());
      }
    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      process.destroy();
    }
  }

  /// ACCESSORS

  public String getFilesystem() { return filesystem; }
  public long getCapacity() { return capacity; }
  public long getUsed() { return used; }
  public long getAvailable() { return available; }
  public int getPercentUsed() { return percentUsed; }
  public String getMount() { return mount; }
  
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

  public static void main(String[] args) throws Exception {
    System.out.println(new DF(args[0]));
  }
}
