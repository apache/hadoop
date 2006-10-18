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
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.*;
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
        conf.setQuietMode(true);
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
     * Obtain the indicated DFS files that match the file pattern <i>srcf</i>
     * and copy them to the local name. srcf is kept.
     * When copying mutiple files, the destination must be a directory. 
     * Otherwise, IOException is thrown.
     * @param srcf: a file pattern specifying source files
     * @param dstf: a destination local file/directory 
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    void copyToLocal(String srcf, String dstf) throws IOException {
      Path [] srcs = fs.globPaths( new Path(srcf) );
      if( srcs.length > 1 && !new File( dstf ).isDirectory()) {
        throw new IOException( "When copy multiple files, " 
            + "destination should be a directory." );
      }
      Path dst = new Path( dstf );
      for( int i=0; i<srcs.length; i++ ) {
        fs.copyToLocalFile( srcs[i], dst );
      }
    }
    
    /**
     * Get all the files in the directories that match the source file 
     * pattern and merge and sort them to only one file on local fs 
     * srcf is kept.
     * @param srcf: a file pattern specifying source files
     * @param dstf: a destination local file/directory 
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    void copyMergeToLocal(String srcf, Path dst) throws IOException {
        copyMergeToLocal(srcf, dst, false);
    }    
    

    /**
     * Get all the files in the directories that match the source file pattern
     * and merge and sort them to only one file on local fs 
     * srcf is kept.
     * 
     * Also adds a string between the files (useful for adding \n
     * to a text file)
     * @param srcf: a file pattern specifying source files
     * @param dstf: a destination local file/directory
     * @param endline: if an end of line character is added to a text file 
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    void copyMergeToLocal(String srcf, Path dst, boolean endline) throws IOException {
      Path [] srcs = fs.globPaths( new Path( srcf ) );
      for( int i=0; i<srcs.length; i++ ) {
        if(endline) {
            FileUtil.copyMerge(fs, srcs[i], 
                    FileSystem.getNamed("local", conf), dst, false, conf, "\n");
        } else {
            FileUtil.copyMerge(fs, srcs[i], 
                    FileSystem.getNamed("local", conf), dst, false, conf, null);
        }
      }
    }      

    /**
     * Obtain the indicated DFS file and copy to the local name.
     * srcf is removed.
     */
    void moveToLocal(String srcf, Path dst) throws IOException {
        System.err.println("Option '-moveToLocal' is not implemented yet.");
    }

    /**
     * Fetch all DFS files that match the file pattern <i>srcf</i> and display
     * their content on stdout. 
     * @param srcf: a file pattern specifying source files
     * @exception: IOException
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    void cat(String srcf) throws IOException {
      Path [] srcs = fs.globPaths( new Path( srcf ) );
      for( int i=0; i<srcs.length; i++ ) {
        cat(srcs[i]);
      }
    }
    

    /* print the content of src to screen */
    private void cat(Path src) throws IOException {
      FSDataInputStream in = fs.open(src);
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
      
      setReplication(rep, cmd[pos], recursive);
    }
    
    /**
     * Set the replication for files that match file pattern <i>srcf</i>
     * if it's a directory and recursive is true,
     * set replication for all the subdirs and those files too
     * @param newRep: new replication factor
     * @param srcf: a file pattern specifying source files
     * @param recursive: if need to set replication factor for files in subdirs
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    public void setReplication(short newRep, String srcf, boolean recursive)
        throws IOException {
      Path[] srcs = fs.globPaths( new Path(srcf) );
      for( int i=0; i<srcs.length; i++ ) {
        setReplication( newRep, srcs[i], recursive );
      }
    }
    
    private void setReplication(short newRep, Path src, boolean recursive)
      throws IOException {
  	
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
     * @param file: a dfs file/directory
     * @param newRep: new replication factor
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
     * Get a listing of all files in DFS that match the file pattern <i>srcf</i>
     * @param srcf: a file pattern specifying source files
     * @param recursive: if need to list files in subdirs
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    public void ls(String srcf, boolean recursive) throws IOException {
      Path[] srcs = fs.globPaths( new Path(srcf) );
      boolean printHeader = (srcs.length == 1) ? true: false;
      for(int i=0; i<srcs.length; i++) {
        ls(srcs[i], recursive, printHeader);
      }
    }

    /* list all files in dfs under the directory <i>src</i>*/
    private void ls(Path src, boolean recursive, boolean printHeader ) throws IOException {
        Path items[] = fs.listPaths(src);
        if (items == null) {
            System.out.println("Could not get listing for " + src);
        } else {
            if(!recursive && printHeader ) {
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
                  ls(cur, recursive, printHeader);
                }
            }
        }
    }

    /**
     * Show the size of all files in DFS that match the file pattern <i>srcf</i>
     * @param srcf: a file pattern specifying source files
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    public void du(String src) throws IOException {
        Path items[] = fs.listPaths( fs.globPaths( new Path(src) ) );
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
     * Move DFS files that match the file pattern <i>srcf</i>
     * to a destination dfs file.
     * When moving mutiple files, the destination must be a directory. 
     * Otherwise, IOException is thrown.
     * @param srcf: a file pattern specifying source files
     * @param dstf: a destination local file/directory 
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    public void rename(String srcf, String dstf) throws IOException {
      Path [] srcs = fs.globPaths( new Path(srcf) );
      Path dst = new Path(dstf);
      if( srcs.length > 1 && !fs.isDirectory(dst)) {
        throw new IOException( "When moving multiple files, " 
            + "destination should be a directory." );
      }
      for( int i=0; i<srcs.length; i++ ) {
        if (fs.rename(srcs[i], dst)) {
            System.out.println("Renamed " + srcs[i] + " to " + dstf);
        } else {
            System.out.println("Rename failed " + srcs[i]);
        }
      }
    }

    /**
     * Copy DFS files that match the file pattern <i>srcf</i>
     * to a destination dfs file.
     * When copying mutiple files, the destination must be a directory. 
     * Otherwise, IOException is thrown.
     * @param srcf: a file pattern specifying source files
     * @param dstf: a destination local file/directory 
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    public void copy(String srcf, String dstf, Configuration conf) throws IOException {
      Path [] srcs = fs.globPaths( new Path(srcf) );
      Path dst = new Path(dstf);
      if( srcs.length > 1 && !fs.isDirectory(dst)) {
        throw new IOException( "When copying multiple files, " 
            + "destination should be a directory." );
      }
      for( int i=0; i<srcs.length; i++ ) {
        FileUtil.copy(fs, srcs[i], fs, dst, false, conf);
      }
    }

    /**
     * Delete all files in DFS that match the file pattern <i>srcf</i>
     * @param srcf: a file pattern specifying source files
     * @param recursive: if need to delete subdirs
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    public void delete(String srcf, boolean recursive) throws IOException {
      Path [] srcs = fs.globPaths( new Path(srcf) );
      for( int i=0; i<srcs.length; i++ ) {
        delete(srcs[i], recursive);
      }
    }
    
    /* delete an DFS file */
    private void delete(Path src, boolean recursive ) throws IOException {
      if (fs.isDirectory(src) && !recursive) {
        System.out.println("Cannot remove directory \"" + src +
                           "\", use -rmr instead");
        return;
      }

      if (fs.delete(src)) {
        System.out.println("Deleted " + src);
      } else {
        System.out.println("Delete failed " + src);
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
        boolean mode = dfs.setSafeMode( FSConstants.SafeModeAction.SAFEMODE_GET );

        if( mode )
          System.out.println("Safe mode is ON" );
        System.out.println("Total raw bytes: " + raw + " (" + byteDesc(raw) + ")");
        System.out.println("Used raw bytes: " + rawUsed + " (" + byteDesc(rawUsed) + ")");
        System.out.println("% used: " + limitDecimal(((1.0 * rawUsed) / raw) * 100, 2) + "%");
        System.out.println();
        System.out.println("Total effective bytes: " + used + " (" + byteDesc(used) + ")");
        System.out.println("Effective replication multiplier: " + (1.0 * rawUsed / used));

        System.out.println("-------------------------------------------------");
        DatanodeInfo info[] = dfs.getDataNodeStats();
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
     * 
     * Usage: java DFSShell -safemode [enter | leave | get]
     */
    public void setSafeMode( String argv[], int idx ) throws IOException {
      final String safeModeUsage = "Usage: java DFSShell -safemode [enter | leave | get]";
      if( ! (fs instanceof DistributedFileSystem) ) {
        System.out.println( "FileSystem is " + fs.getName() );
        return;
      }
      if( idx != argv.length-1 ) {
        printUsage("-safemode");
        return;
      }
      FSConstants.SafeModeAction action;
      if( "leave".equalsIgnoreCase(argv[idx]) )
        action = FSConstants.SafeModeAction.SAFEMODE_LEAVE;
      else if( "enter".equalsIgnoreCase(argv[idx]) )
        action = FSConstants.SafeModeAction.SAFEMODE_ENTER;
      else if( "get".equalsIgnoreCase(argv[idx]) )
        action = FSConstants.SafeModeAction.SAFEMODE_GET;
      else {
        printUsage("-safemode");
        return;
      }
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      boolean mode = dfs.setSafeMode( action );
      System.out.println( "Safe mode is " + ( mode ? "ON" : "OFF" ));
    }

    /**
     * Displays format of commands.
     * 
     */
    public void printUsage(String cmd) {
          if ("-fs".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [-fs <local | namenode:port>]");
          } else if ("-conf".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [-conf <configuration file>]");
          } else if ("-D".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [-D <[property=value>]");
          } else if ("-ls".equals(cmd) || "-lsr".equals(cmd) ||
                   "-du".equals(cmd) || "-rm".equals(cmd) ||
                   "-rmr".equals(cmd) || "-mkdir".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [" + cmd + " <path>]");
          } else if ("-mv".equals(cmd) || "-cp".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [" + cmd + " <src> <dst>]");
          } else if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd) ||
                   "-moveFromLocal".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [" + cmd + " <localsrc> <dst>]");
          } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd) ||
                   "-moveToLocal".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [" + cmd + " <src> <localdst>]");
          } else if ("-cat".equals(cmd)) {
            System.out.println("Usage: java DFSShell" + 
                " [" + cmd + " <src>]");
          } else if ("-get".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [" + cmd + " <src> <localdst> [addnl]]");
          } else if ("-report".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [report]");
          } else if ("-setrep".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [-setrep [-R] <rep> <path/file>]");
          } else if ("-safemode".equals(cmd)) {
            System.err.println("Usage: java DFSShell" + 
                " [-safemode enter | leave | get]");
          } else {
            System.err.println("Usage: java DFSShell");
            System.err.println("           [-fs <local | namenode:port>]");
            System.err.println("           [-conf <configuration file>]");
            System.err.println("           [-D <[property=value>]");
            System.err.println("           [-ls <path>]" );
            System.err.println("           [-lsr <path>]");
            System.err.println("           [-du <path>]");
            System.err.println("           [-mv <src> <dst>]");
            System.err.println("           [-cp <src> <dst>]");
            System.err.println("           [-rm <path>]");
            System.err.println("           [-rmr <path>]");
            System.err.println("           [-put <localsrc> <dst>]");
            System.err.println("           [-copyFromLocal <localsrc> <dst>]");
            System.err.println("           [-moveFromLocal <localsrc> <dst>]");
            System.err.println("           [-get <src> <localdst>]");
            System.err.println("           [-getmerge <src> <localdst> [addnl]]");
            System.err.println("           [-cat <src>]");
            System.err.println("           [-copyToLocal <src> <localdst>]");
            System.err.println("           [-moveToLocal <src> <localdst>]");
            System.err.println("           [-mkdir <path>]");
            System.err.println("           [-report]");
            System.err.println("           [-setrep [-R] <rep> <path/file>]");
            System.err.println("           [-safemode enter | leave | get]");
          }
    }

    /**
     * run
     */
    public int run( String argv[] ) throws Exception {

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
        if ("-put".equals(cmd) || "-get".equals(cmd) || 
            "-copyFromLocal".equals(cmd) || "-moveFromLocal".equals(cmd) || 
            "-copyToLocal".equals(cmd) || "-moveToLocal".equals(cmd) || 
            "-mv".equals(cmd) || "-cp".equals(cmd)) {
                if (argv.length != 3) {
                  printUsage(cmd);
                  return exitCode;
                }
        } else if ("-rm".equals(cmd) || "-rmr".equals(cmd) ||
                 "-cat".equals(cmd) || "-mkdir".equals(cmd) ||
                 "-safemode".equals(cmd)) {
                if (argv.length != 2) {
                  printUsage(cmd);
                  return exitCode;
                }
        } else if ( "-report".equals(cmd)) {
                if (argv.length != 1) {
                  printUsage(cmd);
                  return exitCode;
                }
        }

        // initialize DFSShell
        try {
            init();
        } catch (IOException e) {
            System.err.println("Bad connection to DFS... command aborted.");
            return exitCode;
        }

        exitCode = 0;
        try {
            if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
                copyFromLocal(new Path(argv[i++]), argv[i++]);
            } else if ("-moveFromLocal".equals(cmd)) {
                moveFromLocal(new Path(argv[i++]), argv[i++]);
            } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
                copyToLocal(argv[i++], argv[i++]);
            } else if ("-getmerge".equals(cmd)) {
                if(argv.length>i+2)
                    copyMergeToLocal(argv[i++], new Path(argv[i++]), Boolean.parseBoolean(argv[i++]));
                else
                    copyMergeToLocal(argv[i++], new Path(argv[i++]));
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
                delete(argv[i++], false);
            } else if ("-rmr".equals(cmd)) {
                delete(argv[i++], true);
            } else if ("-du".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                du(arg);
            } else if ("-mkdir".equals(cmd)) {
                mkdir(argv[i++]);
            } else if ("-report".equals(cmd)) {
                report();
            } else if ("-safemode".equals(cmd)) {
                setSafeMode(argv,i);
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
            System.err.println(cmd.substring(1) + ": " + 
                               content[0]);
          } catch (Exception ex) {
            System.err.println(cmd.substring(1) + ": " + 
                               ex.getLocalizedMessage());  
          }
        } catch (IOException e ) {
          //
          // IO exception encountered locally.
          // 
          exitCode = -1;
          System.err.println(cmd.substring(1) + ": " + 
                             e.getLocalizedMessage());  
        } finally {
            fs.close();
        }
        return exitCode;
    }

    /**
     * main() has some simple utility methods
     */
    public static void main(String argv[]) throws Exception {
        int res = new DFSShell().doMain(new Configuration(), argv);
        System.exit(res);
    }
}
