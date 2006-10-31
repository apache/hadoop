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
package org.apache.hadoop.dfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolBase;

/**
 * This class provides rudimentary checking of DFS volumes for errors and
 * sub-optimal conditions.
 * <p>The tool scans all files and directories, starting from an indicated
 *  root path. The following abnormal conditions are detected and handled:</p>
 * <ul>
 * <li>files with blocks that are completely missing from all datanodes.<br/>
 * In this case the tool can perform one of the following actions:
 *  <ul>
 *      <li>none ({@link NamenodeFsck#FIXING_NONE})</li>
 *      <li>move corrupted files to /lost+found directory on DFS
 *      ({@link NamenodeFsck#FIXING_MOVE}). Remaining data blocks are saved as a
 *      block chains, representing longest consecutive series of valid blocks.</li>
 *      <li>delete corrupted files ({@link NamenodeFsck#FIXING_DELETE})</li>
 *  </ul>
 *  </li>
 *  <li>detect files with under-replicated or over-replicated blocks</li>
 *  </ul>
 *  Additionally, the tool collects a detailed overall DFS statistics, and
 *  optionally can print detailed statistics on block locations and replication
 *  factors of each file.
 *  
 * @author Andrzej Bialecki
 */
public class DFSck extends ToolBase {
  private static final Log LOG = LogFactory.getLog(DFSck.class.getName());

  DFSck() {}
  
  /**
   * Filesystem checker.
   * @param conf current Configuration
   * @throws Exception
   */
  public DFSck(Configuration conf) throws Exception {
    setConf(conf);
  }
  
  private String getInfoServer() throws IOException {
    String fsName = conf.get("fs.default.name", "local");
    if (fsName.equals("local")) {
      throw new IOException("This tool only checks DFS, but your config uses 'local' FS.");
    }
    String[] splits = fsName.split(":", 2);
    int infoPort = conf.getInt("dfs.info.port", 50070);
    return splits[0]+":"+infoPort;
  }
  
  /**
   * @param args
   */
  public int run(String[] args) throws Exception {
    String fsName = getInfoServer();
    if (args.length == 0) {
      System.err.println("Usage: DFSck <path> [-move | -delete] [-files] [-blocks [-locations]]");
      System.err.println("\t<path>\tstart checking from this path");
      System.err.println("\t-move\tmove corrupted files to /lost+found");
      System.err.println("\t-delete\tdelete corrupted files");
      System.err.println("\t-files\tprint out files being checked");
      System.err.println("\t-blocks\tprint out block report");
      System.err.println("\t-locations\tprint out locations for every block");
      return -1;
    }
    StringBuffer url = new StringBuffer("http://"+fsName+"/fsck?path=");
    String dir = "/";
    // find top-level dir first
    for (int idx = 0; idx < args.length; idx++) {
      if (!args[idx].startsWith("-")) { dir = args[idx]; break; }
    }
    url.append(URLEncoder.encode(dir, "UTF-8"));
    for (int idx = 0; idx < args.length; idx++) {
      if (args[idx].equals("-move")) { url.append("&move=1"); }
      else if (args[idx].equals("-delete")) { url.append("&delete=1"); }
      else if (args[idx].equals("-files")) { url.append("&files=1"); }
      else if (args[idx].equals("-blocks")) { url.append("&blocks=1"); }
      else if (args[idx].equals("-locations")) { url.append("&locations=1"); }
    }
    URL path = new URL(url.toString());
    URLConnection connection = path.openConnection();
    InputStream stream = connection.getInputStream();
    InputStreamReader input =
        new InputStreamReader(stream, "UTF-8");
    try {
      int c = input.read();
      while (c != -1) {
        System.out.print((char) c);
        c = input.read();
      }
    } finally {
      input.close();
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
      int res = new DFSck().doMain(new Configuration(), args);
      System.exit(res);
  }
}
