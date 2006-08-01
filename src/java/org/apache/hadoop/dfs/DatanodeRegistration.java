package org.apache.hadoop.dfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/** 
 * DatanodeRegistration class conatins all information the Namenode needs
 * to identify and verify a Datanode when it contacts the Namenode.
 * This information is sent by Datanode with each communication request.
 * 
 * @author Konstantin Shvachko
 */
class DatanodeRegistration extends DatanodeID implements Writable {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeRegistration.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeRegistration(); }
       });
  }

  int version;            /// current Datanode version
  String registrationID;  /// a unique per namenode id; indicates   
                          /// the namenode the datanode is registered with

  /**
   * Default constructor.
   */
  public DatanodeRegistration() {
    this( 0, null, null, -1, null );
  }
  
  /**
   * Create DatanodeRegistration
   */
  public DatanodeRegistration(int version, 
                              String nodeName, 
                              String storageID,
                              int infoPort,
                              String registrationID ) {
    super( nodeName, storageID, infoPort );
    this.version = version;
    this.registrationID = registrationID;
  }

  /**
   */
  public int getVersion() {
    return version;
  }
  
  /**
   */
  public String getRegistrationID() {
    return registrationID;
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  /**
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.version);
    super.write( out );
    UTF8.writeString(out, registrationID);
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    this.version = in.readInt();
    super.readFields(in);
    this.registrationID = UTF8.readString(in);   
  }
}
