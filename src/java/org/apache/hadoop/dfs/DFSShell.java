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
package org.apache.hadoop.dfs;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

/**************************************************
 * This class provides some DFS administrative access.
 *
 * @author Mike Cafarella
 **************************************************/
public class DFSShell {
    FileSystem fs;

    /**
     */
    public DFSShell(FileSystem fs) {
        this.fs = fs;
    }

    /**
     * Add a local file to the indicated name in DFS. src is kept.
     */
    void copyFromLocal(File src, String dstf) throws IOException {
        fs.copyFromLocalFile(src, new File(dstf));
    }

    /**
     * Add a local file to the indicated name in DFS. src is removed.
     */
    void moveFromLocal(File src, String dstf) throws IOException {
        fs.moveFromLocalFile(src, new File(dstf));
    }

    /**
     * Obtain the indicated DFS file and copy to the local name.
     * srcf is kept.
     */
    void copyToLocal(String srcf, File dst) throws IOException {
        fs.copyToLocalFile(new File(srcf), dst);
    }

    /**
     * Obtain the indicated DFS file and copy to the local name.
     * srcf is removed.
     */
    void moveToLocal(String srcf, File dst) throws IOException {
        System.err.println("Option '-moveToLocal' is not implemented yet.");
    }

    void cat(String srcf) throws IOException {
      FSDataInputStream in = fs.open(new File(srcf));
      try {
        DataInputStream din = new DataInputStream(new BufferedInputStream(in));
        String line;
        while((line = din.readLine()) != null) {
          System.out.println(line);      
        }
      } finally {
        in.close();
      }
    }

    /**
     * Get a listing of all files in DFS at the indicated name
     */
    public void ls(String src, boolean recursive) throws IOException {
        File items[] = fs.listFiles(new File(src));
        if (items == null) {
            System.out.println("Could not get listing for " + src);
        } else {
            if(!recursive) {
            	System.out.println("Found " + items.length + " items");
            }
            for (int i = 0; i < items.length; i++) {
                File cur = items[i];
                System.out.println(cur.getPath() + "\t" + (cur.isDirectory() ? "<dir>" : ("" + cur.length())));
                if(recursive && cur.isDirectory()) {
									 ls(cur.getPath(), recursive);
                }
            }
        }
    }

    /**
     */
    public void du(String src) throws IOException {
        File items[] = fs.listFiles(new File(src));
        if (items == null) {
            System.out.println("Could not get listing for " + src);
        } else {
            System.out.println("Found " + items.length + " items");
            for (int i = 0; i < items.length; i++) {
                DFSFile cur = (DFSFile) items[i];
                System.out.println(cur.getPath() + "\t" + cur.getContentsLength());
            }
        }
    }

    /**
     * Create the given dir
     */
    public void mkdir(String src) throws IOException {
        File f = new File(src);
        fs.mkdirs(f);
    }
    
    /**
     * Rename an DFS file
     */
    public void rename(String srcf, String dstf) throws IOException {
        if (fs.rename(new File(srcf), new File(dstf))) {
            System.out.println("Renamed " + srcf + " to " + dstf);
        } else {
            System.out.println("Rename failed");
        }
    }

    /**
     * Copy an DFS file
     */
    public void copy(String srcf, String dstf, Configuration conf) throws IOException {
        if (FileUtil.copyContents(fs, new File(srcf), new File(dstf), true, conf)) {
            System.out.println("Copied " + srcf + " to " + dstf);
        } else {
            System.out.println("Copy failed");
        }
    }

    /**
     * Delete an DFS file
     */
    public void delete(String srcf) throws IOException {
        if (fs.delete(new File(srcf))) {
            System.out.println("Deleted " + srcf);
        } else {
            System.out.println("Delete failed");
        }
    }

    /**
     * Return an abbreviated English-language desc of the byte length
     */
    static String byteDesc(long len) {
        double val = 0.0;
        String ending = "";
        if (len < 1024 * 1024) {
            val = (1.0 * len) / 1024;
            ending = " k";
        } else if (len < 1024 * 1024 * 1024) {
            val = (1.0 * len) / (1024 * 1024);
            ending = " Mb";
        } else {
            val = (1.0 * len) / (1024 * 1024 * 1024);
            ending = " Gb";
        }
        return limitDecimal(val, 2) + ending;
    }

    static String limitDecimal(double d, int placesAfterDecimal) {
        String strVal = Double.toString(d);
        int decpt = strVal.indexOf(".");
        if (decpt >= 0) {
            strVal = strVal.substring(0, Math.min(strVal.length(), decpt + 1 + placesAfterDecimal));
        }
        return strVal;
    }

    /**
     * Gives a report on how the FileSystem is doing
     */
    public void report() throws IOException {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem)fs;
        long raw = dfs.getRawCapacity();
        long rawUsed = dfs.getRawUsed();
        long used = dfs.getUsed();

        System.out.println("Total raw bytes: " + raw + " (" + byteDesc(raw) + ")");
        System.out.println("Used raw bytes: " + rawUsed + " (" + byteDesc(rawUsed) + ")");
        System.out.println("% used: " + limitDecimal(((1.0 * rawUsed) / raw) * 100, 2) + "%");
        System.out.println();
        System.out.println("Total effective bytes: " + used + " (" + byteDesc(used) + ")");
        System.out.println("Effective replication multiplier: " + (1.0 * rawUsed / used));

        System.out.println("-------------------------------------------------");
        DataNodeReport info[] = dfs.getDataNodeStats();
        System.out.println("Datanodes available: " + info.length);
        System.out.println();
        for (int i = 0; i < info.length; i++) {
          System.out.println(info[i]);
          System.out.println();
        }
      }
    }

    /**
     * main() has some simple utility methods
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length < 1) {
            System.out.println("Usage: java DFSShell [-local | -dfs <namenode:port>]" +
                    " [-ls <path>] [-lsr <path>] [-du <path>] [-mv <src> <dst>] [-cp <src> <dst>] [-rm <src>]" +
                    " [-put <localsrc> <dst>] [-copyFromLocal <localsrc> <dst>] [-moveFromLocal <localsrc> <dst>]" + 
                    " [-get <src> <localdst>] [-cat <src>] [-copyToLocal <src> <localdst>] [-moveToLocal <src> <localdst>]" +
                    " [-mkdir <path>] [-report]");
            return;
        }

        Configuration conf = new Configuration();
        int i = 0;
        FileSystem fs = FileSystem.parseArgs(argv, i, conf);
        String cmd = argv[i++];
        try {
            DFSShell tc = new DFSShell(fs);

            if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
                tc.copyFromLocal(new File(argv[i++]), argv[i++]);
            } else if ("-moveFromLocal".equals(cmd)) {
                tc.moveFromLocal(new File(argv[i++]), argv[i++]);
            } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
                tc.copyToLocal(argv[i++], new File(argv[i++]));
            } else if ("-cat".equals(cmd)) {
                tc.cat(argv[i++]);
            } else if ("-moveToLocal".equals(cmd)) {
                tc.moveToLocal(argv[i++], new File(argv[i++]));
            } else if ("-ls".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                tc.ls(arg, false);
            } else if ("-lsr".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                tc.ls(arg, true);
            } else if ("-mv".equals(cmd)) {
                tc.rename(argv[i++], argv[i++]);
            } else if ("-cp".equals(cmd)) {
                tc.copy(argv[i++], argv[i++], conf);
            } else if ("-rm".equals(cmd)) {
                tc.delete(argv[i++]);
            } else if ("-du".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                tc.du(arg);
            } else if ("-mkdir".equals(cmd)) {
                tc.mkdir(argv[i++]);
            } else if ("-report".equals(cmd)) {
                tc.report();
            }
            System.exit(0);
        } catch (IOException e ) {
          System.err.println( cmd.substring(1) + ": " + e.getLocalizedMessage() );  
          System.exit(-1);
        } finally {
            fs.close();
        }
    }
}
