package org.apache.hadoop.dfs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.UTF8;

/** 
 * Data storage information file.
 * <p>
 * During startup the datanode reads its data storage file.
 * The data storage file is stored in all the dfs.data.dir directories.
 * It contains version and storageID.
 * Datanode holds a lock on all the dataStorage files while it runs so that other 
 * datanodes were not able to start working with the same data storage.
 * The locks are released when the datanode stops (normally or abnormally).
 * 
 * @author Konstantin Shvachko
 */
class DataStorage {
  public static final String STORAGE_INFO_FILE_NAME = "storage";
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.DataStorage");

  // persistent fields
  private int version = 0;  /// stored version
  private String storageID; /// unique per cluster storageID
  
  // non persistent fields
  private ArrayList storageFiles = new ArrayList();
  private ArrayList storageLocks = new ArrayList();
  
  // cache away the names of all passed in dirs
  private File[] origDirs = null;
  
  // cache away the names of locked dirs
  private File[] dirs = null;
  
  private int numLocked = 0;
  
  /**
   * Create DataStorage and verify its version.
   * 
   * @param dataDirs array of data storage directories
   * @throws IOException
   */
  public DataStorage( File[] dataDirs ) throws IOException {
    this( DataNode.DFS_CURRENT_VERSION, dataDirs );
    
    if( version < FSConstants.DFS_CURRENT_VERSION ) // future version
      throw new IncorrectVersionException( version, "data storage" );
  }
  
  /**
   * Create DataStorage.
   * 
   * Read data storage files if they exist or create them if not.
   * Lock the files.
   * 
   * @param curVersion can be used to read file saved with a previous version.
   * @param dataDirs Array of data storage directories
   * @throws IOException
   */
  public DataStorage( int curVersion, File[] dataDirs ) throws IOException {
    this.version = curVersion;
    this.origDirs = dataDirs;
    for (int idx = 0; idx < dataDirs.length; idx++) {
      storageFiles.add(idx, new RandomAccessFile( 
                          new File(dataDirs[idx], STORAGE_INFO_FILE_NAME ), 
                          "rws" ));
      lock(idx);
      boolean needToSave;
      try {
        needToSave = read(idx);
      } catch( java.io.EOFException e ) {
        storageID = "";
        needToSave = true;
      }
    
      if( needToSave ) { write(idx); }
      
      RandomAccessFile file = (RandomAccessFile) storageFiles.get(idx);
      if (file != null) { numLocked++; }
    }
    if (numLocked > 0) {
      this.dirs = new File[numLocked];
      int curidx = 0;
      for (int idx = 0; idx < dataDirs.length; idx++) {
        if (storageFiles.get(idx) != null) {
          dirs[curidx] = dataDirs[idx];
          curidx++;
        }
      }
    }
  }
  
  public int getVersion() {
    return version;
  }

  public String getStorageID() {
    return storageID;
  }
  
  public int getNumLocked() {
    return numLocked;
  }
  
  public File[] getLockedDirs() {
    return dirs;
  }
  
  public void setStorageID( String newStorageID ) {
    this.storageID = newStorageID;
  }
  
  public void setVersion( int newVersion ) {
    this.version = newVersion;
  }
  
  /**
   * Lock datastorage file.
   * 
   * @throws IOException
   */
  private void lock(int idx) throws IOException {
    RandomAccessFile file = (RandomAccessFile) storageFiles.get(idx);
    FileLock lock = file.getChannel().tryLock();
    if (lock == null) {
      // log a warning
      LOG.warn("Cannot lock storage file in directory "+origDirs[idx].getName());
      // remove the file from fileList, and close it
      storageFiles.add(idx, null);
      file.close();
    }
    storageLocks.add(idx, lock);
  }
  
  /**
   * Unlock datastorage file.
   * @param idx File index
   * 
   * @throws IOException
   */
  private void unlock(int idx) throws IOException {
    FileLock lock = (FileLock) storageLocks.get(idx);
    if (lock != null) { lock.release(); }
  }
  
  /**
   * Close a datastorage file.
   * @param idx file index
   * @throws IOException
   */
  private void close(int idx) throws IOException {
    FileLock lock = (FileLock) storageLocks.get(idx);
    if (lock == null) { return; }
    lock.release();
    RandomAccessFile file = (RandomAccessFile) storageFiles.get(idx);
    file.close();
  }
  
  /**
   * Close all datastorage files.
   * @throws IOException
   */
  public void closeAll() throws IOException {
    for (int idx = 0; idx < dirs.length; idx++) {
      close(idx);
    }
  }
  
  /**
   * Read data storage file.
   * @param idx File index
   * @return whether the data storage file need to be updated.
   * @throws IOException
   */
  private boolean read(int idx) throws IOException {
    RandomAccessFile file = (RandomAccessFile) storageFiles.get(idx);
    if (file == null) { return false; }
    file.seek(0);
    this.version = file.readInt();
    this.storageID = UTF8.readString( file );
    return false;
  }

  /**
   * Write data storage file.
   * @param idx File index
   * @throws IOException
   */
  private void write(int idx) throws IOException {
    RandomAccessFile file = (RandomAccessFile) storageFiles.get(idx);
    if (file == null) { return; }
    file.seek(0);
    file.writeInt( this.version );
    UTF8.writeString( file, this.storageID );
  }
  
  /**
   * Write all data storage files.
   * @throws IOException
   */
  public void writeAll() throws IOException {
    for (int idx = 0; idx < dirs.length; idx++) {
      write(idx);
    }
  }
  
}
