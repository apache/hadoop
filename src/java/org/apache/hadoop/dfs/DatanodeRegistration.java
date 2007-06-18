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

  StorageInfo storageInfo;

  /**
   * Default constructor.
   */
  public DatanodeRegistration() {
    super(null, null, -1);
    this.storageInfo = new StorageInfo();
  }
  
  /**
   * Create DatanodeRegistration
   */
  public DatanodeRegistration(String nodeName) {
    super(nodeName, "", -1);
    this.storageInfo = new StorageInfo();
  }
  
  void setInfoPort(int infoPort) {
    this.infoPort = infoPort;
  }
  
  void setStorageInfo(DataStorage storage) {
    this.storageInfo = new StorageInfo(storage);
  }
  
  void setName(String name) {
    this.name = name;
  }

  /**
   */
  public int getVersion() {
    return storageInfo.getLayoutVersion();
  }
  
  /**
   */
  public String getRegistrationID() {
    return Storage.getRegistrationID(storageInfo);
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  /**
   */
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(storageInfo.getLayoutVersion());
    out.writeInt(storageInfo.getNamespaceID());
    out.writeLong(storageInfo.getCTime());
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    storageInfo.layoutVersion = in.readInt();
    storageInfo.namespaceID = in.readInt();
    storageInfo.cTime = in.readLong();
  }
}
