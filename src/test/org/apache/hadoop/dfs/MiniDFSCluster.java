package org.apache.hadoop.dfs;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * One thread is created for each server.
 * The data directories for DFS are undering the testing directory.
 * @author Owen O'Malley
 */
public class MiniDFSCluster {

  private Configuration conf;
  private Thread nameNodeThread;
  private Thread dataNodeThread;
  private NameNodeRunner nameNode;
  private DataNodeRunner dataNode;

  /**
   * An inner class that runs a name node.
   */
  class NameNodeRunner implements Runnable {
    private NameNode node;
    
    public boolean isUp() {
      if (node == null) {
        return false;
      }
      try {
        long[] sizes = node.getStats();
        return sizes[0] != 0;
      } catch (IOException ie) {
        return false;
      }
    }
    
    /**
     * Create the name node and run it.
     */
    public void run() {
      try {
        node = new NameNode(conf);
      } catch (Throwable e) {
        node = null;
        System.err.println("Name node crashed:");
        e.printStackTrace();
      }
    }
    
    /**
     * Shutdown the name node and wait for it to finish.
     */
    public void shutdown() {
      if (node != null) {
        node.stop();
        node.join();
      }
    }
  }
  
  /**
   * An inner class to run the data node.
   */
  class DataNodeRunner implements Runnable {
    private DataNode node;
    
    /**
     * Create and run the data node.
     */
    public void run() {
      try {
        File dataDir = new File(conf.get("dfs.data.dir"));
        dataDir.mkdirs();
        node = new DataNode(conf, dataDir.getPath());
        node.run();
      } catch (Throwable e) {
        node = null;
        System.err.println("Data node crashed:");
        e.printStackTrace();
      }
    }

    /**    
     * Shut down the server and wait for it to finish.
     */
    public void shutdown() {
      if (node != null) {
        node.shutdown();
      }
    }
  }
  
  /**
   * Create the config and start up the servers.
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        boolean dataNodeFirst) throws IOException {
    this.conf = conf;
    conf.set("fs.default.name", 
             "localhost:"+ Integer.toString(namenodePort));
    File base_dir = new File(System.getProperty("test.build.data"),
                             "dfs/");
    conf.set("dfs.name.dir", new File(base_dir, "name").getPath());
    conf.set("dfs.data.dir", new File(base_dir, "data").getPath());
    conf.setInt("dfs.replication", 1);
    // this timeout seems to control the minimum time for the test, so
    // decrease it considerably.
    conf.setInt("ipc.client.timeout", 2000);
    NameNode.format(conf);
    nameNode = new NameNodeRunner();
    nameNodeThread = new Thread(nameNode);
    dataNode = new DataNodeRunner();
    dataNodeThread = new Thread(dataNode);
    if (dataNodeFirst) {
      dataNodeThread.start();      
      nameNodeThread.start();      
    } else {
      nameNodeThread.start();
      dataNodeThread.start();      
    }
    while (!nameNode.isUp()) {
      try {                                     // let daemons get started
        System.out.println("waiting for dfs minicluster to start");
        Thread.sleep(2000);
      } catch(InterruptedException e) {
      }
    }
  }
  
  /**
   * Shut down the servers.
   */
  public void shutdown() {
    nameNode.shutdown();
    dataNode.shutdown();
  }
  
  /**
   * Get a client handle to the DFS cluster.
   */
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }
}
