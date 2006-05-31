package org.apache.hadoop.dfs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

import org.apache.hadoop.io.UTF8;

/** 
 * Data storage information file.
 * <p>
 * During startup the datanode reads its data storage file.
 * The data storage file is stored in the dfs.data.dir directory.
 * It contains version and storageID.
 * Datanode holds a lock on the dataStorage file while it runs so that other 
 * datanodes were not able to start working with the same data storage.
 * The lock is released when the datanode stops (normally or abnormally).
 * 
 * @author Konstantin Shvachko
 */
class DataStorage {
  public static final String STORAGE_INFO_FILE_NAME = "storage";

  // persistent fields
  private int version = 0;  /// stored version
  private String storageID; /// unique per cluster storageID
  
  // non persistent fields
  private RandomAccessFile storageFile = null;
  private FileLock storageLock = null;
  
  /**
   * Create DataStorage and verify its version.
   * 
   * @param datadir data storage directory
   * @throws IOException
   */
  public DataStorage( File datadir ) throws IOException {
    this( DataNode.DFS_CURRENT_VERSION, datadir );
    
    if( version != DataNode.DFS_CURRENT_VERSION )
      throw new IncorrectVersionException( version, "data storage" );
  }
  
  /**
   * Create DataStorage.
   * 
   * Read data storage file if exists or create it if not.
   * Lock the file.
   * 
   * @param curVersion can be used to read file saved with a previous version.
   * @param datadir data storage directory
   * @throws IOException
   */
  public DataStorage( int curVersion, File datadir ) throws IOException {
    this.version = curVersion;
    storageFile = new RandomAccessFile( 
                        new File(datadir, STORAGE_INFO_FILE_NAME ), 
                        "rws" );
    lock();
    boolean needToSave;
    try {
      needToSave = read();
    } catch( java.io.EOFException e ) {
      storageID = "";
      needToSave = true;
    }
    
    if( needToSave )
      write();
  }
  
  public int getVersion() {
    return version;
  }

  public String getStorageID() {
    return storageID;
  }
  
  public void setStorageID( String newStorageID ) {
    this.storageID = newStorageID;
  }
  
  public void setVersion( int newVersion ) {
    this.version = newVersion;
  }
  
  /**
   * Lock datastoarge file.
   * 
   * @throws IOException
   */
  public void lock() throws IOException {
    storageLock = storageFile.getChannel().tryLock();
    if( storageLock == null )
      throw new IOException( "Cannot start multiple Datanode instances "
                              + "sharing the same data directory.\n" 
                              + STORAGE_INFO_FILE_NAME + " is locked. ");
  }
  
  /**
   * Unlock datastoarge file.
   * 
   * @throws IOException
   */
  public void unlock() throws IOException {
    storageLock.release();
  }
  
  /**
   * Close datastoarge file.
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    storageLock.release();
    storageFile.close();
  }
  
  /**
   * Read data storage file.
   * 
   * @return whether the data storage file need to be updated.
   * @throws IOException
   */
  public boolean read() throws IOException {
    storageFile.seek(0);
    this.version = storageFile.readInt();
    UTF8 uID = new UTF8();
    uID.readFields( storageFile );
    this.storageID = uID.toString();
    return false;
  }

  /**
   * Write data storage file.
   * 
   * @throws IOException
   */
  public void write() throws IOException {
    storageFile.seek(0);
    storageFile.writeInt( this.version );
    UTF8 uID = new UTF8( this.storageID );
    uID.write( storageFile );
  }
}
