package org.apache.hadoop.dfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;

/**
 * DatanodeID is composed of the data node 
 * name (hostname:portNumber) and the data storage ID, 
 * which it currently represents.
 * 
 * @author Konstantin Shvachko
 */
public class DatanodeID implements WritableComparable {

  protected String name;      /// hostname:portNumber
  protected String storageID; /// unique per cluster storageID
  protected int infoPort;     /// the port where the infoserver is running

  /**
   * DatanodeID default constructor
   */
  public DatanodeID() {
    this( new String(), new String(), -1 );
  }

  /**
   * DatanodeID copy constructor
   * 
   * @param from
   */
  public DatanodeID( DatanodeID from ) {
    this( from.getName(), from.getStorageID(), from.getInfoPort() );
  }
  
  /**
   * Create DatanodeID
   * 
   * @param nodeName (hostname:portNumber) 
   * @param storageID data storage ID
   */
  public DatanodeID( String nodeName, String storageID, int infoPort ) {
    this.name = nodeName;
    this.storageID = storageID;
    this.infoPort = infoPort;
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
   * @return infoPort (the port at which the HTTP server bound to)
   */
  public int getInfoPort() {
    return infoPort;
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
  
  public boolean equals( Object to ) {
    return (name.equals(((DatanodeID)to).getName()) &&
        storageID.equals(((DatanodeID)to).getStorageID()));
  }
  
  public int hashCode() {
    return name.hashCode()^ storageID.hashCode();
  }
  
  public String toString() {
    return name;
  }
  
  /**
   * Update fields when a new registration request comes in.
   * Note that this does not update storageID.
   */
  void updateRegInfo( DatanodeID nodeReg ) {
      name = nodeReg.getName();
      infoPort = nodeReg.getInfoPort();
      // update any more fields added in future.
  }
    
  /** Comparable.
   * Basis of compare is the String name (host:portNumber) only.
   * @param o
   * @return as specified by Comparable.
   */
  public int compareTo(Object o) {
    return name.compareTo(((DatanodeID)o).getName());
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  /**
   */
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, name);
    UTF8.writeString(out, storageID);
    out.writeShort(infoPort);
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    name = UTF8.readString(in);
    storageID = UTF8.readString(in);
    // the infoPort read could be negative, if the port is a large number (more
    // than 15 bits in storage size (but less than 16 bits).
    // So chop off the first two bytes (and hence the signed bits) before 
    // setting the field.
    this.infoPort = in.readShort() & 0x0000ffff;
  }
}
