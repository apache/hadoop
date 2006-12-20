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

import java.io.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.util.ToolBase;

/** Provide command line access to a FileSystem. */
public class FsShell extends ToolBase {

    protected FileSystem fs;

    /**
     */
    public FsShell() {
    }

    public void init() throws IOException {
        conf.setQuietMode(true);
        this.fs = FileSystem.get(conf);
    }

    /**
     * Copies from stdin to the indicated file.
     */
    private void copyFromStdin(Path dst) throws IOException {
      if (fs.isDirectory(dst)) {
        throw new IOException("When source is stdin, destination must be a file.");
      }
      if (fs.exists(dst)) {
        throw new IOException("Target " + dst.toString() + " already exists.");
      }
      FSDataOutputStream out = fs.create(dst); 
      byte buf[] = new byte[conf.getInt("io.file.buffer.size", 4096)];
      try {
        int bytesRead = System.in.read(buf);
        while (bytesRead >= 0) {
          out.write(buf, 0, bytesRead);
          bytesRead = System.in.read(buf);
        }
      } finally {
        out.close();
      }
    }

    /**
     * Add a local file to the indicated FileSystem name. src is kept.
     */
    void copyFromLocal(Path src, String dstf) throws IOException {
      if (src.toString().equals("-")) {
        copyFromStdin(new Path(dstf));
      } else {
        fs.copyFromLocalFile(src, new Path(dstf));
      }
    }

    /**
     * Add a local file to the indicated FileSystem name. src is removed.
     */
    void moveFromLocal(Path src, String dstf) throws IOException {
        fs.moveFromLocalFile(src, new Path(dstf));
    }

    /**
     * Obtain the indicated files that match the file pattern <i>srcf</i>
     * and copy them to the local name. srcf is kept.
     * When copying mutiple files, the destination must be a directory. 
     * Otherwise, IOException is thrown.
     * @param argv: arguments
     * @param pos: Ignore everything before argv[pos]  
     * @exception: IOException  
     * @see org.apache.hadoop.fs.FileSystem.globPaths 
     */
    void copyToLocal(String[]argv, int pos) throws IOException {
      if(argv.length-pos<2 || (argv.length-pos==2 && argv[pos].equalsIgnoreCase("-crc"))) {
        System.err.println("Usage: -get [-crc] <src> <dst>");
        System.exit(-1);
      }
      boolean copyCrc = false;
      if ("-crc".equalsIgnoreCase(argv[pos])) {
        pos++;
        copyCrc = true;
      }
      String srcf = argv[pos++];
      String dstf = argv[pos++];
      if( dstf.equals("-")) {
        if (copyCrc) {
          System.err.println("-crc option is not valid when destination is stdout.");
        }
        cat(srcf);
      } else {
        Path [] srcs = fs.globPaths( new Path(srcf) );
        if( srcs.length > 1 && !new File( dstf ).isDirectory()) {
          throw new IOException( "When copying multiple files, " 
                                 + "destination should be a directory." );
        }
        Path dst = new Path( dstf );
        for( int i=0; i<srcs.length; i++ ) {
          fs.copyToLocalFile( srcs[i], dst, copyCrc );
        }
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
     * Obtain the indicated file and copy to the local name.
     * srcf is removed.
     */
    void moveToLocal(String srcf, Path dst) throws IOException {
        System.err.println("Option '-moveToLocal' is not implemented yet.");
    }

    /**
     * Fetch all files that match the file pattern <i>srcf</i> and display
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
        int checkFactor = 0;
        while((line = din.readLine()) != null) {
          System.out.println(line);      

          //
          // Peridically check if the output encountered an error. This can
          // happen if the output stream has been disconnected
          //
          if (checkFactor == 0) {
            if (System.out.checkError()) {
              throw new IOException("Unable to write to output stream");
            }
            checkFactor = 10000;
          }
          checkFactor--;
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
     * set replication for all the subdirs and those files too.
     * @param newRep new replication factor
     * @param srcf a file pattern specifying source files
     * @param recursive if need to set replication factor for files in subdirs
     * @throws IOException  
     * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
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
      	throw new IOException("Could not get listing for " + src);
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
     * @param file: a file/directory
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
     * Get a listing of all files in that match the file pattern <i>srcf</i>.
     * @param srcf a file pattern specifying source files
     * @param recursive if need to list files in subdirs
     * @throws IOException  
     * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
     */
    public void ls(String srcf, boolean recursive) throws IOException {
      Path[] srcs = fs.globPaths( new Path(srcf) );
      boolean printHeader = (srcs.length == 1) ? true: false;
      for(int i=0; i<srcs.length; i++) {
        ls(srcs[i], recursive, printHeader);
      }
    }

    /* list all files under the directory <i>src</i>*/
    private void ls(Path src, boolean recursive, boolean printHeader ) throws IOException {
        Path items[] = fs.listPaths(src);
        if (items == null) {
            throw new IOException("Could not get listing for " + src);
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
     * Show the size of all files that match the file pattern <i>src</i>
     * @param src a file pattern specifying source files
     * @throws IOException  
     * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
     */
    public void du(String src) throws IOException {
        Path items[] = fs.listPaths( fs.globPaths( new Path(src) ) );
        if (items == null) {
            throw new IOException("Could not get listing for " + src);
        } else {
            System.out.println("Found " + items.length + " items");
            for (int i = 0; i < items.length; i++) {
              Path cur = items[i];
              System.out.println(cur + "\t" + fs.getLength(cur));
            }
        }
    }
    
    /**
     * Show the summary disk usage of each dir/file 
     * that matches the file pattern <i>src</i>
     * @param src a file pattern specifying source files
     * @throws IOException  
     * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
     */
    public void dus(String src) throws IOException {
      Path paths[] = fs.globPaths( new Path(src) );
      if( paths==null && paths.length==0 ) {
        throw new IOException( "dus: No match: " + src );
      }
      for(int i=0; i<paths.length; i++) {
        Path items[] = fs.listPaths( paths[i] );
        if (items != null) {
          int totalSize=0;
          for(int j=0; j<items.length; j++) {
            totalSize += fs.getLength(items[j]);
          }
          String pathStr = paths[i].toString();
          System.out.println(
              ("".equals(pathStr)?".":pathStr) + "\t" + totalSize);
        }
      }
    }

    /**
     * Create the given dir
     */
    public void mkdir(String src) throws IOException {
        Path f = new Path(src);
        if (!fs.mkdirs(f)) {
          throw new IOException("Mkdirs failed to create " + src);
        }
    }
    
    /**
     * Move files that match the file pattern <i>srcf</i>
     * to a destination file.
     * When moving mutiple files, the destination must be a directory. 
     * Otherwise, IOException is thrown.
     * @param srcf a file pattern specifying source files
     * @param dstf a destination local file/directory 
     * @throws IOException  
     * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
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
            throw new IOException("Rename failed " + srcs[i]);
        }
      }
    }

    /**
     * Move/rename file(s) to a destination file. Multiple source
     * files can be specified. The destination is the last element of
     * the argvp[] array.
     * If multiple source files are specified, then the destination 
     * must be a directory. Otherwise, IOException is thrown.
     * @exception: IOException  
     */
    private int rename(String argv[], Configuration conf) throws IOException {
      int i = 0;
      int exitCode = 0;
      String cmd = argv[i++];  
      String dest = argv[argv.length-1];
      //
      // If the user has specified multiple source files, then
      // the destination has to be a directory
      //
      if (argv.length > 3) {
        Path dst = new Path(dest);
        if (!fs.isDirectory(dst)) {
          throw new IOException( "When moving multiple files, " 
            + "destination " + dest + " should be a directory." );
        }
      }
      //
      // for each source file, issue the rename
      //
      for (; i < argv.length - 1; i++) {
        try {
          //
          // issue the rename to the fs
          //
          rename(argv[i], dest);
        } catch (RemoteException e) {
          //
          // This is a error returned by hadoop server. Print
          // out the first line of the error mesage.
          //
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
        } catch (IOException e) {
          //
          // IO exception encountered locally.
          //
          exitCode = -1;
          System.err.println(cmd.substring(1) + ": " +
                             e.getLocalizedMessage());
        }
      }
      return exitCode;
    }

    /**
     * Copy files that match the file pattern <i>srcf</i>
     * to a destination file.
     * When copying mutiple files, the destination must be a directory. 
     * Otherwise, IOException is thrown.
     * @param srcf a file pattern specifying source files
     * @param dstf a destination local file/directory 
     * @throws IOException  
     * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
     */
    public void copy(String srcf, String dstf, Configuration conf) throws IOException {
      Path [] srcs = fs.globPaths( new Path(srcf) );
      Path dst = new Path(dstf);
      if( srcs.length > 1 && !fs.isDirectory(dst)) {
        throw new IOException( "When copying multiple files, " 
            + "destination should be a directory." );
      }
      for( int i=0; i<srcs.length; i++ ) {
        FileUtil.copy(fs, srcs[i], fs, dst, false, true, conf);
      }
    }

    /**
     * Copy file(s) to a destination file. Multiple source
     * files can be specified. The destination is the last element of
     * the argvp[] array.
     * If multiple source files are specified, then the destination 
     * must be a directory. Otherwise, IOException is thrown.
     * @exception: IOException  
     */
    private int copy(String argv[], Configuration conf) throws IOException {
      int i = 0;
      int exitCode = 0;
      String cmd = argv[i++];  
      String dest = argv[argv.length-1];
      //
      // If the user has specified multiple source files, then
      // the destination has to be a directory
      //
      if (argv.length > 3) {
        Path dst = new Path(dest);
        if (!fs.isDirectory(dst)) {
          throw new IOException( "When copying multiple files, " 
            + "destination " + dest + " should be a directory." );
        }
      }
      //
      // for each source file, issue the copy
      //
      for (; i < argv.length - 1; i++) {
        try {
          //
          // issue the copy to the fs
          //
          copy(argv[i], dest, conf);
        } catch (RemoteException e) {
          //
          // This is a error returned by hadoop server. Print
          // out the first line of the error mesage.
          //
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
        } catch (IOException e) {
          //
          // IO exception encountered locally.
          //
          exitCode = -1;
          System.err.println(cmd.substring(1) + ": " +
                             e.getLocalizedMessage());
        }
      }
      return exitCode;
    }

    /**
     * Delete all files that match the file pattern <i>srcf</i>.
     * @param srcf a file pattern specifying source files
     * @param recursive if need to delete subdirs
     * @throws IOException  
     * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
     */
    public void delete(String srcf, boolean recursive) throws IOException {
      Path [] srcs = fs.globPaths( new Path(srcf) );
      for( int i=0; i<srcs.length; i++ ) {
        delete(srcs[i], recursive);
      }
    }
    
    /* delete a file */
    private void delete(Path src, boolean recursive ) throws IOException {
      if (fs.isDirectory(src) && !recursive) {
        throw new IOException("Cannot remove directory \"" + src +
                           "\", use -rmr instead");
      }

      if (fs.delete(src)) {
        System.out.println("Deleted " + src);
      } else {
        throw new IOException("Delete failed " + src);
      }
    }

    /**
     * Return an abbreviated English-language desc of the byte length
     */
    public static String byteDesc(long len) {
        double val = 0.0;
        String ending = "";
        if (len < 1024 * 1024) {
            val = (1.0 * len) / 1024;
            ending = " k";
        } else if (len < 1024 * 1024 * 1024) {
            val = (1.0 * len) / (1024 * 1024);
            ending = " MB";
        } else if (len < 128L * 1024 * 1024 * 1024 ) {
            val = (1.0 * len) / (1024 * 1024 * 1024);
            ending = " GB";
        } else if (len < 1024L * 1024 * 1024 * 1024 * 1024) {
            val = (1.0 * len) / (1024L * 1024 * 1024 * 1024);
            ending = " TB";
        } else {
            val = (1.0 * len) / (1024L * 1024 * 1024 * 1024 * 1024);
            ending = " PB";
        }
        return limitDecimal(val, 2) + ending;
    }

    public static String limitDecimal(double d, int placesAfterDecimal) {
        String strVal = Double.toString(d);
        int decpt = strVal.indexOf(".");
        if (decpt >= 0) {
            strVal = strVal.substring(0, Math.min(strVal.length(), decpt + 1 + placesAfterDecimal));
        }
        return strVal;
    }

    /**
     * Apply operation specified by 'cmd' on all parameters
     * starting from argv[startindex].
     */
    private int doall(String cmd, String argv[], Configuration conf, 
                      int startindex) {
      int exitCode = 0;
      int i = startindex;
      //
      // for each source file, issue the command
      //
      for (; i < argv.length; i++) {
        try {
          //
          // issue the command to the fs
          //
          if ("-cat".equals(cmd)) {
              cat(argv[i]);
          } else if ("-mkdir".equals(cmd)) {
              mkdir(argv[i]);
          } else if ("-rm".equals(cmd)) {
              delete(argv[i], false);
          } else if ("-rmr".equals(cmd)) {
              delete(argv[i], true);
          } else if ("-du".equals(cmd)) {
              du(argv[i]);
          } else if ("-dus".equals(cmd)) {
              dus(argv[i]);
          } else if ("-ls".equals(cmd)) {
              ls(argv[i], false);
          } else if ("-lsr".equals(cmd)) {
              ls(argv[i], true);
          }
        } catch (RemoteException e) {
          //
          // This is a error returned by hadoop server. Print
          // out the first line of the error mesage.
          //
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
        } catch (IOException e) {
          //
          // IO exception encountered locally.
          //
          exitCode = -1;
          System.err.println(cmd.substring(1) + ": " +
                             e.getLocalizedMessage());
        }
      }
      return exitCode;
    }

    /**
     * Displays format of commands.
     * 
     */
    public void printUsage(String cmd) {
          if ("-fs".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [-fs <local | namenode:port>]");
          } else if ("-conf".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [-conf <configuration file>]");
          } else if ("-D".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [-D <[property=value>]");
          } else if ("-ls".equals(cmd) || "-lsr".equals(cmd) ||
                   "-du".equals(cmd) || "-dus".equals(cmd) || 
                   "-rm".equals(cmd) || "-rmr".equals(cmd) || 
                   "-mkdir".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [" + cmd + " <path>]");
          } else if ("-mv".equals(cmd) || "-cp".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [" + cmd + " <src> <dst>]");
          } else if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd) ||
                   "-moveFromLocal".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [" + cmd + " <localsrc> <dst>]");
          } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd) ||
                   "-moveToLocal".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [" + cmd + " [-crc] <src> <localdst>]");
          } else if ("-cat".equals(cmd)) {
            System.out.println("Usage: java FsShell" + 
                " [" + cmd + " <src>]");
          } else if ("-get".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [" + cmd + " <src> <localdst> [addnl]]");
          } else if ("-setrep".equals(cmd)) {
            System.err.println("Usage: java FsShell" + 
                " [-setrep [-R] <rep> <path/file>]");
          } else {
            System.err.println("Usage: java FsShell");
            System.err.println("           [-fs <local | namenode:port>]");
            System.err.println("           [-conf <configuration file>]");
            System.err.println("           [-D <[property=value>]");
            System.err.println("           [-ls <path>]" );
            System.err.println("           [-lsr <path>]");
            System.err.println("           [-du <path>]");
            System.err.println("           [-dus <path>]");
            System.err.println("           [-mv <src> <dst>]");
            System.err.println("           [-cp <src> <dst>]");
            System.err.println("           [-rm <path>]");
            System.err.println("           [-rmr <path>]");
            System.err.println("           [-put <localsrc> <dst>]");
            System.err.println("           [-copyFromLocal <localsrc> <dst>]");
            System.err.println("           [-moveFromLocal <localsrc> <dst>]");
            System.err.println("           [-get [-crc] <src> <localdst>]");
            System.err.println("           [-getmerge <src> <localdst> [addnl]]");
            System.err.println("           [-cat <src>]");
            System.err.println("           [-copyToLocal [-crc] <src> <localdst>]");
            System.err.println("           [-moveToLocal [-crc] <src> <localdst>]");
            System.err.println("           [-mkdir <path>]");
            System.err.println("           [-setrep [-R] <rep> <path/file>]");
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
        if ("-put".equals(cmd) || 
            "-copyFromLocal".equals(cmd) || "-moveFromLocal".equals(cmd)) {
                if (argv.length != 3) {
                  printUsage(cmd);
                  return exitCode;
                }
        } else if ("-get".equals(cmd) || 
            "-copyToLocal".equals(cmd) || "-moveToLocal".equals(cmd)) {
                if (argv.length < 3) {
                  printUsage(cmd);
                  return exitCode;
                }
        } else if ("-mv".equals(cmd) || "-cp".equals(cmd)) {
                if (argv.length < 3) {
                  printUsage(cmd);
                  return exitCode;
                }
        } else if ("-rm".equals(cmd) || "-rmr".equals(cmd) ||
                   "-cat".equals(cmd) || "-mkdir".equals(cmd)) {
                if (argv.length < 2) {
                  printUsage(cmd);
                  return exitCode;
                }
        }

        // initialize FsShell
        try {
            init();
        } catch (RPC.VersionMismatch v) { 
            System.err.println("Version Mismatch between client and server" +
                               "... command aborted.");
            return exitCode;
        } catch (IOException e) {
            System.err.println("Bad connection to FS. command aborted.");
            return exitCode;
        }

        exitCode = 0;
        try {
            if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
                copyFromLocal(new Path(argv[i++]), argv[i++]);
            } else if ("-moveFromLocal".equals(cmd)) {
                moveFromLocal(new Path(argv[i++]), argv[i++]);
            } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
                copyToLocal(argv, i);
            } else if ("-getmerge".equals(cmd)) {
                if(argv.length>i+2)
                    copyMergeToLocal(argv[i++], new Path(argv[i++]), Boolean.parseBoolean(argv[i++]));
                else
                    copyMergeToLocal(argv[i++], new Path(argv[i++]));
            } else if ("-cat".equals(cmd)) {
                exitCode = doall(cmd, argv, conf, i);
            } else if ("-moveToLocal".equals(cmd)) {
                moveToLocal(argv[i++], new Path(argv[i++]));
            } else if ("-setrep".equals(cmd)) {
            	setReplication(argv, i);           
            } else if ("-ls".equals(cmd)) {
                if (i < argv.length) {
                    exitCode = doall(cmd, argv, conf, i);
                } else {
                    ls("", false);
                } 
            } else if ("-lsr".equals(cmd)) {
                if (i < argv.length) {
                    exitCode = doall(cmd, argv, conf, i);
                } else {
                    ls("", true);
                } 
            } else if ("-mv".equals(cmd)) {
                exitCode = rename(argv, conf);
            } else if ("-cp".equals(cmd)) {
                exitCode = copy(argv, conf);
            } else if ("-rm".equals(cmd)) {
                exitCode = doall(cmd, argv, conf, i);
            } else if ("-rmr".equals(cmd)) {
                exitCode = doall(cmd, argv, conf, i);
            } else if ("-du".equals(cmd)) {
                if (i < argv.length) {
                    exitCode = doall(cmd, argv, conf, i);
                } else {
                    du("");
                }
            } else if( "-dus".equals(cmd)) {
              if (i < argv.length) {
                  exitCode = doall(cmd, argv, conf, i);
              } else {
                  dus("");
              }         
            } else if ("-mkdir".equals(cmd)) {
                exitCode = doall(cmd, argv, conf, i);
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
        int res = new FsShell().doMain(new Configuration(), argv);
        System.exit(res);
    }
}
