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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.DFSShell;
import org.apache.hadoop.dfs.DataNode;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestDFSShellGenericOptions extends TestCase {

    public void testDFSCommand() throws IOException {
        String namenode = null;
        MiniDFSCluster cluster = null;
        try {
          Configuration conf = new Configuration();
          cluster = new MiniDFSCluster(65316, conf, true);
          namenode = conf.get("fs.default.name", "local");
          String [] args = new String[4];
          args[2] = "-mkdir";
          args[3] = "/data";
          testFsOption(args, namenode);
          testConfOption(args, namenode);
          testPropertyOption(args, namenode);
        } finally {
          if (cluster != null) { cluster.shutdown(); }
        }
      }

    private void testFsOption(String [] args, String namenode) {        
        // prepare arguments to create a directory /data
        args[0] = "-fs";
        args[1] = namenode;
        execute(args, namenode);
    }
    
    private void testConfOption(String[] args, String namenode) {
        // prepare configuration hadoop-site.xml
        File configDir = new File(new File("build", "test"), "minidfs");
        assertTrue(configDir.mkdirs());
        File siteFile = new File(configDir, "hadoop-site.xml");
        PrintWriter pw;
        try {
            pw = new PrintWriter(siteFile);
            pw.print("<?xml version=\"1.0\"?>\n"+
                    "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"+
                    "<configuration>\n"+
                    " <property>\n"+
                    "   <name>fs.default.name</name>\n"+
                    "   <value>"+namenode+"</value>\n"+
                    " </property>\n"+
                    "</configuration>\n");
            pw.close();
    
            // prepare arguments to create a directory /data
            args[0] = "-conf";
            args[1] = siteFile.getPath();
            execute(args, namenode); 
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
          siteFile.delete();
          configDir.delete();
        }
    }
    
    private void testPropertyOption(String[] args, String namenode) {
        // prepare arguments to create a directory /data
        args[0] = "-D";
        args[1] = "fs.default.name="+namenode;
        execute(args, namenode);        
    }
    
    private void execute( String [] args, String namenode ) {
        DFSShell shell=new DFSShell();
        FileSystem fs=null;
        try {
            shell.doMain(new Configuration(), args);
            fs = new DistributedFileSystem(
                    DataNode.createSocketAddr(namenode), 
                    shell.getConf());
            assertTrue( "Directory does not get created", 
                    fs.isDirectory(new Path("/data")) );
            fs.delete(new Path("/data"));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        } finally {
            if( fs!=null ) {
                try {
                  fs.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

}
