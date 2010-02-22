package org.apache.hadoop.hbase.zookeeper;

import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

/**
 * Tool for reading ZooKeeper servers from HBase XML configuation and producing
 * a line-by-line list for use by bash scripts.
 */
public class ZKServerTool implements HConstants {
  /**
   * Run the tool.
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) {
    Configuration conf = HBaseConfiguration.create();
    // Note that we do not simply grab the property ZOOKEEPER_QUORUM from
    // the HBaseConfiguration because the user may be using a zoo.cfg file.
    Properties zkProps = HQuorumPeer.makeZKProps(conf);
    for (Entry<Object, Object> entry : zkProps.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        String[] parts = value.split(":");
        String host = parts[0];
        System.out.println(host);
      }
    }
  }
}
