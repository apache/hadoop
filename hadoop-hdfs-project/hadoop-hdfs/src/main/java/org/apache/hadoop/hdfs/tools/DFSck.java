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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class provides rudimentary checking of DFS volumes for errors and
 * sub-optimal conditions.
 * <p>The tool scans all files and directories, starting from an indicated
 *  root path. The following abnormal conditions are detected and handled:</p>
 * <ul>
 * <li>files with blocks that are completely missing from all datanodes.<br/>
 * In this case the tool can perform one of the following actions:
 *  <ul>
 *      <li>none ({@link org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_NONE})</li>
 *      <li>move corrupted files to /lost+found directory on DFS
 *      ({@link org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_MOVE}). Remaining data blocks are saved as a
 *      block chains, representing longest consecutive series of valid blocks.</li>
 *      <li>delete corrupted files ({@link org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_DELETE})</li>
 *  </ul>
 *  </li>
 *  <li>detect files with under-replicated or over-replicated blocks</li>
 *  </ul>
 *  Additionally, the tool collects a detailed overall DFS statistics, and
 *  optionally can print detailed statistics on block locations and replication
 *  factors of each file.
 *  The tool also provides and option to filter open files during the scan.
 *  
 */
@InterfaceAudience.Private
public class DFSck extends Configured implements Tool {
  static{
    HdfsConfiguration.init();
  }

  private static final String USAGE = "Usage: DFSck <path> "
      + "[-list-corruptfileblocks | "
      + "[-move | -delete | -openforwrite] "
      + "[-files [-blocks [-locations | -racks]]]] [-showprogress]\n"
      + "\t<path>\tstart checking from this path\n"
      + "\t-move\tmove corrupted files to /lost+found\n"
      + "\t-delete\tdelete corrupted files\n"
      + "\t-files\tprint out files being checked\n"
      + "\t-openforwrite\tprint out files opened for write\n"
      + "\t-includeSnapshots\tinclude snapshot data if the given path"
      + " indicates a snapshottable directory or there are "
      + "snapshottable directories under it\n"
      + "\t-list-corruptfileblocks\tprint out list of missing "
      + "blocks and files they belong to\n"
      + "\t-blocks\tprint out block report\n"
      + "\t-locations\tprint out locations for every block\n"
      + "\t-racks\tprint out network topology for data-node locations\n"
      + "\t-showprogress\tshow progress in output. Default is OFF (no progress)\n\n"
      + "Please Note:\n"
      + "\t1. By default fsck ignores files opened for write, "
      + "use -openforwrite to report such files. They are usually "
      + " tagged CORRUPT or HEALTHY depending on their block "
      + "allocation status\n"
      + "\t2. Option -includeSnapshots should not be used for comparing stats,"
      + " should be used only for HEALTH check, as this may contain duplicates"
      + " if the same file present in both original fs tree "
      + "and inside snapshots.";
  
  private final UserGroupInformation ugi;
  private final PrintStream out;
  private final URLConnectionFactory connectionFactory;
  private final boolean isSpnegoEnabled;

  /**
   * Filesystem checker.
   * @param conf current Configuration
   */
  public DFSck(Configuration conf) throws IOException {
    this(conf, System.out);
  }

  public DFSck(Configuration conf, PrintStream out) throws IOException {
    super(conf);
    this.ugi = UserGroupInformation.getCurrentUser();
    this.out = out;
    this.connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);
    this.isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
  }

  /**
   * Print fsck usage information
   */
  static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
    ToolRunner.printGenericCommandUsage(out);
  }
  @Override
  public int run(final String[] args) throws IOException {
    if (args.length == 0) {
      printUsage(System.err);
      return -1;
    }

    try {
      return UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Integer>() {
            @Override
            public Integer run() throws Exception {
              return doWork(args);
            }
          });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  /*
   * To get the list, we need to call iteratively until the server says
   * there is no more left.
   */
  private Integer listCorruptFileBlocks(String dir, String baseUrl)
      throws IOException {
    int errCode = -1;
    int numCorrupt = 0;
    int cookie = 0;
    final String noCorruptLine = "has no CORRUPT files";
    final String noMoreCorruptLine = "has no more CORRUPT files";
    final String cookiePrefix = "Cookie:";
    boolean allDone = false;
    while (!allDone) {
      final StringBuffer url = new StringBuffer(baseUrl);
      if (cookie > 0) {
        url.append("&startblockafter=").append(String.valueOf(cookie));
      }
      URL path = new URL(url.toString());
      URLConnection connection;
      try {
        connection = connectionFactory.openConnection(path, isSpnegoEnabled);
      } catch (AuthenticationException e) {
        throw new IOException(e);
      }
      InputStream stream = connection.getInputStream();
      BufferedReader input = new BufferedReader(new InputStreamReader(
          stream, "UTF-8"));
      try {
        String line = null;
        while ((line = input.readLine()) != null) {
          if (line.startsWith(cookiePrefix)){
            try{
              cookie = Integer.parseInt(line.split("\t")[1]);
            } catch (Exception e){
              allDone = true;
              break;
            }
            continue;
          }
          if ((line.endsWith(noCorruptLine)) ||
              (line.endsWith(noMoreCorruptLine)) ||
              (line.endsWith(NamenodeFsck.NONEXISTENT_STATUS))) {
            allDone = true;
            break;
          }
          if ((line.isEmpty())
              || (line.startsWith("FSCK started by"))
              || (line.startsWith("The filesystem under path")))
            continue;
          numCorrupt++;
          if (numCorrupt == 1) {
            out.println("The list of corrupt files under path '"
                + dir + "' are:");
          }
          out.println(line);
        }
      } finally {
        input.close();
      }
    }
    out.println("The filesystem under path '" + dir + "' has " 
        + numCorrupt + " CORRUPT files");
    if (numCorrupt == 0)
      errCode = 0;
    return errCode;
  }
  
  /**
   * Derive the namenode http address from the current file system,
   * either default or as set by "-fs" in the generic options.
   * @return Returns http address or null if failure.
   * @throws IOException if we can't determine the active NN address
   */
  private URI getCurrentNamenodeAddress() throws IOException {
    //String nnAddress = null;
    Configuration conf = getConf();

    //get the filesystem object to verify it is an HDFS system
    FileSystem fs;
    try {
      fs = FileSystem.get(conf);
    } catch (IOException ioe) {
      System.err.println("FileSystem is inaccessible due to:\n"
          + StringUtils.stringifyException(ioe));
      return null;
    }
    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("FileSystem is " + fs.getUri());
      return null;
    }
    
    return DFSUtil.getInfoServer(HAUtil.getAddressOfActive(fs), conf,
        DFSUtil.getHttpClientScheme(conf));
  }

  private int doWork(final String[] args) throws IOException {
    final StringBuilder url = new StringBuilder();
    
    URI namenodeAddress = getCurrentNamenodeAddress();
    if (namenodeAddress == null) {
      //Error message already output in {@link #getCurrentNamenodeAddress()}
      System.err.println("DFSck exiting.");
      return 0;
    }

    url.append(namenodeAddress.toString());
    System.err.println("Connecting to namenode via " + url.toString());
    
    url.append("/fsck?ugi=").append(ugi.getShortUserName());
    String dir = null;
    boolean doListCorruptFileBlocks = false;
    for (int idx = 0; idx < args.length; idx++) {
      if (args[idx].equals("-move")) { url.append("&move=1"); }
      else if (args[idx].equals("-delete")) { url.append("&delete=1"); }
      else if (args[idx].equals("-files")) { url.append("&files=1"); }
      else if (args[idx].equals("-openforwrite")) { url.append("&openforwrite=1"); }
      else if (args[idx].equals("-blocks")) { url.append("&blocks=1"); }
      else if (args[idx].equals("-locations")) { url.append("&locations=1"); }
      else if (args[idx].equals("-racks")) { url.append("&racks=1"); }
      else if (args[idx].equals("-showprogress")) { url.append("&showprogress=1"); }
      else if (args[idx].equals("-list-corruptfileblocks")) {
        url.append("&listcorruptfileblocks=1");
        doListCorruptFileBlocks = true;
      } else if (args[idx].equals("-includeSnapshots")) {
        url.append("&includeSnapshots=1");
      } else if (!args[idx].startsWith("-")) {
        if (null == dir) {
          dir = args[idx];
        } else {
          System.err.println("fsck: can only operate on one path at a time '"
              + args[idx] + "'");
          printUsage(System.err);
          return -1;
        }
      } else {
        System.err.println("fsck: Illegal option '" + args[idx] + "'");
        printUsage(System.err);
        return -1;
      }
    }
    if (null == dir) {
      dir = "/";
    }
    url.append("&path=").append(URLEncoder.encode(dir, "UTF-8"));
    if (doListCorruptFileBlocks) {
      return listCorruptFileBlocks(dir, url.toString());
    }
    URL path = new URL(url.toString());
    URLConnection connection;
    try {
      connection = connectionFactory.openConnection(path, isSpnegoEnabled);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }
    InputStream stream = connection.getInputStream();
    BufferedReader input = new BufferedReader(new InputStreamReader(
                                              stream, "UTF-8"));
    String line = null;
    String lastLine = null;
    int errCode = -1;
    try {
      while ((line = input.readLine()) != null) {
        out.println(line);
        lastLine = line;
      }
    } finally {
      input.close();
    }
    if (lastLine.endsWith(NamenodeFsck.HEALTHY_STATUS)) {
      errCode = 0;
    } else if (lastLine.endsWith(NamenodeFsck.CORRUPT_STATUS)) {
      errCode = 1;
    } else if (lastLine.endsWith(NamenodeFsck.NONEXISTENT_STATUS)) {
      errCode = 0;
    }
    return errCode;
  }

  public static void main(String[] args) throws Exception {
    // -files option is also used by GenericOptionsParser
    // Make sure that is not the first argument for fsck
    int res = -1;
    if ((args.length == 0) || ("-files".equals(args[0]))) {
      printUsage(System.err);
      ToolRunner.printGenericCommandUsage(System.err);
    } else if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      res = 0;
    } else {
      res = ToolRunner.run(new DFSck(new HdfsConfiguration()), args);
    }
    System.exit(res);
  }
}
