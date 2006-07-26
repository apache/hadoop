package org.apache.hadoop.dfs;

/**
 * DatanodeID is composed of the data node 
 * name (hostname:portNumber) and the data storage ID, 
 * which it currently represents.
 * 
 * @author Konstantin Shvachko
 */
public class DatanodeID implements Comparable {

  protected String name;      /// hostname:portNumber
  protected String storageID; /// unique per cluster storageID
  
  /**
   * Create DatanodeID
   * 
   * @param nodeName (hostname:portNumber) 
   * @param storageID data storage ID
   */
  public DatanodeID( String nodeName, String storageID ) {
    this.name = nodeName;
    this.storageID = storageID;
  }
  
  /**
   * @return hostname:portNumber.
   */
  public String getName() {
    return name;
  }
  
  /**
   * @return data storage ID.
   */
  public String getStorageID() {
    return this.storageID;
  }

  /**
   * @return hostname and no :portNumber.
   */
  public String getHost() {
    int colon = name.indexOf(":");
    if (colon < 0) {
      return name;
    } else {
      return name.substring(0, colon);
    }
  }
  
  public String toString() {
    return name;
  }
  
  /** Comparable.
   * Basis of compare is the String name (host:portNumber) only.
   * @param o
   * @return as specified by Comparable.
   */
  public int compareTo(Object o) {
    return name.compareTo(((DatanodeID)o).getName());
  }
}
