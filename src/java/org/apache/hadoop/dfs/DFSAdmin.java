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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RPC;

/**
 * This class provides some DFS administrative access.
 *
 * @author Dhruba Borthakur
 */
public class DFSAdmin extends DFSShell {

    /**
     * Construct a DFSAdmin object.
     */
    public DFSAdmin() {
        super();
    }

    /**
     * Gives a report on how the FileSystem is doing.
     * @exception IOException if the filesystem does not exist.
     */
    public void report() throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        long raw = dfs.getRawCapacity();
        long rawUsed = dfs.getRawUsed();
        long used = dfs.getUsed();
        boolean mode = dfs.setSafeMode(
                           FSConstants.SafeModeAction.SAFEMODE_GET);

        if (mode) {
          System.out.println("Safe mode is ON");
        }
        System.out.println("Total raw bytes: " + raw
                           + " (" + byteDesc(raw) + ")");
        System.out.println("Used raw bytes: " + rawUsed
                           + " (" + byteDesc(rawUsed) + ")");
        System.out.println("% used: "
                           + limitDecimal(((1.0 * rawUsed) / raw) * 100, 2)
                           + "%");
        System.out.println();
        System.out.println("Total effective bytes: " + used
                           + " (" + byteDesc(used) + ")");
        System.out.println("Effective replication multiplier: "
                           + (1.0 * rawUsed / used));

        System.out.println("-------------------------------------------------");
        DatanodeInfo[] info = dfs.getDataNodeStats();
        System.out.println("Datanodes available: " + info.length);
        System.out.println();
        for (int i = 0; i < info.length; i++) {
          System.out.println(info[i].getDatanodeReport());
          System.out.println();
        }
      }
    }

    /**
     * Safe mode maintenance command.
     * Usage: java DFSAdmin -safemode [enter | leave | get]
     * @param argv List of of command line parameters.
     * @param idx The index of the command that is being processed.
     * @exception IOException if the filesystem does not exist.
     */
    public void setSafeMode(String[] argv, int idx) throws IOException {
      final String safeModeUsage = "Usage: java DFSAdmin -safemode "
                                   + "[enter | leave | get]";
      if (!(fs instanceof DistributedFileSystem)) {
        System.out.println("FileSystem is " + fs.getName());
        return;
      }
      if (idx != argv.length - 1) {
        printUsage("-safemode");
        return;
      }
      FSConstants.SafeModeAction action;

      if ("leave".equalsIgnoreCase(argv[idx])) {
        action = FSConstants.SafeModeAction.SAFEMODE_LEAVE;
      } else if ("enter".equalsIgnoreCase(argv[idx])) {
        action = FSConstants.SafeModeAction.SAFEMODE_ENTER;
      } else if ("get".equalsIgnoreCase(argv[idx])) {
        action = FSConstants.SafeModeAction.SAFEMODE_GET;
      } else {
        printUsage("-safemode");
        return;
      }
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      boolean mode = dfs.setSafeMode(action);
      System.out.println("Safe mode is " + (mode ? "ON" : "OFF"));
    }

    /**
     * Displays format of commands.
     * @param cmd The command that is being executed.
     */
    public void printUsage(String cmd) {
          if ("-report".equals(cmd)) {
            System.err.println("Usage: java DFSAdmin"
                + " [report]");
          } else if ("-safemode".equals(cmd)) {
            System.err.println("Usage: java DFSAdmin"
                + " [-safemode enter | leave | get]");
          } else {
            System.err.println("Usage: java DFSAdmin");
            System.err.println("           [-report]");
            System.err.println("           [-safemode enter | leave | get]");
          }
    }

    /**
     * @param argv The parameters passed to this program.
     * @exception Exception if the filesystem does not exist.
     * @return 0 on success, non zero on error.
     */
    public int run(String[] argv) throws Exception {

        if (argv.length < 1) {
            printUsage("");
            return -1;
        }

        int exitCode = -1;
        int i = 0;
        String cmd = argv[i++];

        //
        // verify that we have enough command line parameters
        //
        if ("-safemode".equals(cmd)) {
                if (argv.length != 2) {
                  printUsage(cmd);
                  return exitCode;
                }
        } else if ("-report".equals(cmd)) {
                if (argv.length != 1) {
                  printUsage(cmd);
                  return exitCode;
                }
        }

        // initialize DFSAdmin
        try {
            init();
        } catch (RPC.VersionMismatch v) {
            System.err.println("Version Mismatch between client and server"
                               + "... command aborted.");
            return exitCode;
        } catch (IOException e) {
            System.err.println("Bad connection to DFS... command aborted.");
            return exitCode;
        }

        exitCode = 0;
        try {
            if ("-report".equals(cmd)) {
                report();
            } else if ("-safemode".equals(cmd)) {
                setSafeMode(argv, i);
            } else {
                exitCode = -1;
                System.err.println(cmd.substring(1) + ": Unknown command");
                printUsage("");
            }
        } catch (RemoteException e) {
          //
          // This is a error returned by hadoop server. Print
          // out the first line of the error mesage, ignore the stack trace.
          exitCode = -1;
          try {
            String[] content;
            content = e.getLocalizedMessage().split("\n");
            System.err.println(cmd.substring(1) + ": "
                               + content[0]);
          } catch (Exception ex) {
            System.err.println(cmd.substring(1) + ": "
                               + ex.getLocalizedMessage());
          }
        } catch (IOException e) {
          //
          // IO exception encountered locally.
          //
          exitCode = -1;
          System.err.println(cmd.substring(1) + ": "
                             + e.getLocalizedMessage());
        } finally {
            fs.close();
        }
        return exitCode;
    }

    /**
     * main() has some simple utility methods.
     * @param argv Command line parameters.
     * @exception Exception if the filesystem does not exist.
     */
    public static void main(String[] argv) throws Exception {
        int res = new DFSAdmin().doMain(new Configuration(), argv);
        System.exit(res);
    }
}
