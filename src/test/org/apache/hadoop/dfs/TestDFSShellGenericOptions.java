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
        File configDir = new File("conf", "minidfs");
        configDir.mkdirs();
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
            args[1] = "conf/minidfs/hadoop-site.xml";
            execute(args, namenode); 
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        configDir.delete();
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
