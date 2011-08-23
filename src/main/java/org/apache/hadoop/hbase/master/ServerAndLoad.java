package org.apache.hadoop.hbase.master;


import org.apache.hadoop.hbase.ServerName;

/**
 * Data structure that holds servername and 'load'.
 */
class ServerAndLoad implements Comparable<ServerAndLoad> {
  private final ServerName sn;
  private final int load;

  ServerAndLoad(final ServerName sn, final int load) {
    this.sn = sn;
    this.load = load;
  }

  ServerName getServerName() {
    return this.sn;
  }

  int getLoad() {
    return this.load;
  }

  @Override
  public int compareTo(ServerAndLoad other) {
    int diff = this.load - other.load;
    return diff != 0 ? diff : this.sn.compareTo(other.getServerName());
  }
}