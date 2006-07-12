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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolBase;

/**************************************************
 * This class provides some DFS administrative access.
 *
 * @author Mike Cafarella
 **************************************************/
public class DFSShell extends ToolBase {

    FileSystem fs;

    /**
     */
    public DFSShell() {
    }

    public void init() throws IOException {
        this.fs = FileSystem.get(conf);
    }
    /**
     * Add a local file to the indicated name in DFS. src is kept.
     */
    void copyFromLocal(Path src, String dstf) throws IOException {
        fs.copyFromLocalFile(src, new Path(dstf));
    }

    /**
     * Add a local file to the indicated name in DFS. src is removed.
     */
    void moveFromLocal(Path src, String dstf) throws IOException {
        fs.moveFromLocalFile(src, new Path(dstf));
    }

    /**
     * Obtain the indicated DFS file and copy to the local name.
     * srcf is kept.
     */
    void copyToLocal(String srcf, Path dst) throws IOException {
        fs.copyToLocalFile(new Path(srcf), dst);
    }

    /**
     * Obtain the indicated DFS file and copy to the local name.
     * srcf is removed.
     */
    void moveToLocal(String srcf, Path dst) throws IOException {
        System.err.println("Option '-moveToLocal' is not implemented yet.");
    }

    void cat(String srcf) throws IOException {
      FSDataInputStream in = fs.open(new Path(srcf));
      try {
        BufferedReader din = new BufferedReader(new InputStreamReader(in));
        String line;
        while((line = din.readLine()) != null) {
          System.out.println(line);      
        }
      } finally {
        in.close();
      }
    }

    /**
     * Parse the incoming command string
     * @param cmd
     * @param pos ignore anything before this pos in cmd
     * @throws IOException 
     */
    private void setReplication(String[] cmd, int pos) throws IOException {
      if(cmd.length-pos<2 || (cmd.length-pos==2 && cmd[pos].equalsIgnoreCase("-R"))) {
        System.err.println("Usage: [-R] <repvalue> <path>");
        System.exit(-1);
      }
      
      boolean recursive = false;
      short rep = 3;
      
      if("-R".equalsIgnoreCase(cmd[pos])) {
        recursive=true;
        pos++;
        
      }
      
      try {
        rep = Short.parseShort(cmd[pos]);
        pos++;
      } catch (NumberFormatException e) {
        System.err.println("Cannot set replication to: " + cmd[pos]);
        System.exit(-1);
      }
      
      setReplication(rep, new Path(cmd[pos]), recursive);
    }
    
    /**
     * Set the replication for the path argument
     * if it's a directory and recursive is true,
     * set replication for all the subdirs and those files too
     */
    public void setReplication(short newRep, Path src, boolean recursive) throws IOException {
  	
    	if(!fs.isDirectory(src)) {
    		setFileReplication(src, newRep);
    		return;
    	}
    	
      Path items[] = fs.listPaths(src);
      if (items == null) {
      	System.out.println("Could not get listing for " + src);
      } else {

      	for (int i = 0; i < items.length; i++) {
      		Path cur = items[i];
       		if(!fs.isDirectory(cur)) {
       			setFileReplication(cur, newRep);
       		} else if(recursive) {
       			setReplication(newRep, cur, recursive);
       		}
       	}
       }
    }
    
    /**
     * Actually set the replication for this file
     * If it fails either throw IOException or print an error msg
     * @param file
     * @param newRep
     * @throws IOException
     */
    private void setFileReplication(Path file, short newRep) throws IOException {
    	
    	if(fs.setReplication(file, newRep)) {
    		System.out.println("Replication " + newRep + " set: " + file);
    	} else {
    		System.err.println("Could not set replication for: " + file);
    	}
    }
    
    
    /**
     * Get a listing of all files in DFS at the indicated name
     */
    public void ls(String src, boolean recursive) throws IOException {
        Path items[] = fs.listPaths(new Path(src));
        if (items == null) {
            System.out.println("Could not get listing for " + src);
        } else {
            if(!recursive) {
            	System.out.println("Found " + items.length + " items");
            }
            for (int i = 0; i < items.length; i++) {
                Path cur = items[i];
                System.out.println(cur + "\t" 
                                    + (fs.isDirectory(cur) ? 
                                        "<dir>" : 
                                        ("<r " + fs.getReplication(cur) 
                                            + ">\t" + fs.getLength(cur))));
                if(recursive && fs.isDirectory(cur)) {
                  ls(cur.toString(), recursive);
                }
            }
        }
    }

    /**
     */
    public void du(String src) throws IOException {
        Path items[] = fs.listPaths(new Path(src));
        if (items == null) {
            System.out.println("Could not get listing for " + src);
        } else {
            System.out.println("Found " + items.length + " items");
            for (int i = 0; i < items.length; i++) {
                DfsPath cur = (DfsPath) items[i];
                System.out.println(cur + "\t" + cur.getContentsLength());
            }
        }
    }

    /**
     * Create the given dir
     */
    public void mkdir(String src) throws IOException {
        Path f = new Path(src);
        fs.mkdirs(f);
    }
    
    /**
     * Rename an DFS file
     */
    public void rename(String srcf, String dstf) throws IOException {
        if (fs.rename(new Path(srcf), new Path(dstf))) {
            System.out.println("Renamed " + srcf + " to " + dstf);
        } else {
            System.out.println("Rename failed");
        }
    }

    /**
     * Copy an DFS file
     */
    public void copy(String srcf, String dstf, Configuration conf) throws IOException {
      FileUtil.copy(fs, new Path(srcf), fs, new Path(dstf), false, conf);
    }

    /**
     * Delete an DFS file
     */
    public void delete(String srcf) throws IOException {
        if (fs.delete(new Path(srcf))) {
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
     * run
     */
    public int run( String argv[] ) throws Exception {
        if (argv.length < 1) {
            System.out.println("Usage: java DFSShell [-fs <local | namenode:port>]"+
                    " [-conf <configuration file>] [-D <[property=value>]"+
                    " [-ls <path>] [-lsr <path>] [-du <path>] [-mv <src> <dst>] [-cp <src> <dst>] [-rm <src>]" +
                    " [-put <localsrc> <dst>] [-copyFromLocal <localsrc> <dst>] [-moveFromLocal <localsrc> <dst>]" + 
                    " [-get <src> <localdst>] [-cat <src>] [-copyToLocal <src> <localdst>] [-moveToLocal <src> <localdst>]" +
                    " [-mkdir <path>] [-report] [-setrep [-R] <rep> <path/file>]");
            return -1;
        }

        // initialize DFSShell
        init();
        
        int exitCode = -1;
        int i = 0;
        String cmd = argv[i++];
        try {
            if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
                copyFromLocal(new Path(argv[i++]), argv[i++]);
            } else if ("-moveFromLocal".equals(cmd)) {
                moveFromLocal(new Path(argv[i++]), argv[i++]);
            } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
                copyToLocal(argv[i++], new Path(argv[i++]));
            } else if ("-cat".equals(cmd)) {
                cat(argv[i++]);
            } else if ("-moveToLocal".equals(cmd)) {
                moveToLocal(argv[i++], new Path(argv[i++]));
            } else if ("-setrep".equals(cmd)) {
            	setReplication(argv, i);           
            } else if ("-ls".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                ls(arg, false);
            } else if ("-lsr".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                ls(arg, true);
            } else if ("-mv".equals(cmd)) {
                rename(argv[i++], argv[i++]);
            } else if ("-cp".equals(cmd)) {
                copy(argv[i++], argv[i++], conf);
            } else if ("-rm".equals(cmd)) {
                delete(argv[i++]);
            } else if ("-du".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                du(arg);
            } else if ("-mkdir".equals(cmd)) {
                mkdir(argv[i++]);
            } else if ("-report".equals(cmd)) {
                report();
            }
            exitCode = 0;;
        } catch (IOException e ) {
          System.err.println( cmd.substring(1) + ": " + e.getLocalizedMessage() );  
        } finally {
            fs.close();
        }
        return exitCode;
    }

    /**
     * main() has some simple utility methods
     */
    public static void main(String argv[]) throws Exception {
        new DFSShell().doMain(new Configuration(), argv);
    }
}
