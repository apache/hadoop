package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.hdfsdb.*;
import org.apache.log4j.Level;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Created by hmai on 6/3/15.
 */
public class LevelDBProfile {
  private static final String DB_PATH = "/Users/hmai/work/test/partialnsdb";
  private static final int TIMES = 300000;
  public static void main(String[] args) throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean("dfs.partialns", true);
    conf.set("dfs.partialns.path", DB_PATH);
    conf.setInt("dfs.partialns.writebuffer", 8388608 * 16);
    conf.setLong("dfs.partialns.blockcache", 4294967296L);
    ExecutorService executor = Executors.newFixedThreadPool(8, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "Executor");
      }
    });
    ((Log4JLogger)FSNamesystem.auditLog).getLogger().setLevel(Level.WARN);

    try {
      FileUtils.deleteDirectory(new File(DB_PATH));
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      final DistributedFileSystem fs = cluster.getFileSystem();
      final org.apache.hadoop.hdfs.hdfsdb.DB db = cluster.getNamesystem().getFSDirectory().getLevelDb();
      final Path PATH = new Path("/foo");
      final byte[] p = new byte[20];
      try (OutputStream os = fs.create(PATH)) {
      }
      cluster.shutdownDataNodes();
      final FSNamesystem fsn = cluster.getNamesystem();
      final Runnable getFileStatus = new Runnable() {
        @Override
        public void run() {
          try {
            fsn.getFileInfo("/foo", true);
            //fs.getFileStatus(PATH);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };

      long start = monotonicNow();
      for (int i = 0; i < TIMES; ++i) {
        executor.submit(getFileStatus);
      }
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.HOURS);
      long end = monotonicNow();
      System.err.println("Time: " + (end - start) + " ms");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
