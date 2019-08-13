package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;

public class ListingBenchmark {

  public static void main(String[] args) throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .format(true)
        .build();
    NameNode nn = cluster.getNameNode();
  }
}
