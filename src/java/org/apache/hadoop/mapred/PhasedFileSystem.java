package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * This class acts as a proxy to the actual file system being used. 
 * It writes files to a temporary location and on
 * commit, moves to final location. On abort or a failure in 
 * commit the temporary file is deleted  
 * PhasedFileSystem works in context of a task. A different instance of 
 * PhasedFileSystem should be used for every task.  
 * Temporary files are written in  ("mapred.system.dir")/<jobid>/<taskid>
 * If one tasks opens a large number of files in succession then its 
 * better to commit(Path) individual files when done. Otherwise
 * commit() can be used to commit all open files at once. 
 */
public class PhasedFileSystem extends FileSystem {

  private FileSystem baseFS ;
  // Map from final file name to temporary file name
  private Map<Path, FileInfo> finalNameToFileInfo = new HashMap(); 
  
  private String jobid ; 
  private String tipid ; 
  private String taskid ; 
  
  private Path tempDir ; 
  /**
   * This Constructor is used to wrap a FileSystem object to a 
   * Phased FilsSystem.  
   * @param fs base file system object
   * @param jobid JobId
   * @param tipid tipId 
   * @param taskid taskId
   */
  public PhasedFileSystem(FileSystem fs, String jobid, 
      String tipid, String taskid) {
    super(fs.getConf()); // not used
    
    this.baseFS = fs ; 
    this.jobid = jobid; 
    this.tipid = tipid ; 
    this.taskid = taskid ; 
    
    tempDir = new Path(baseFS.getConf().get("mapred.system.dir") ); 
  }
  /**
   * This Constructor is used to wrap a FileSystem object to a 
   * Phased FilsSystem.  
   * @param fs base file system object
   * @param conf JobConf
   */
  public PhasedFileSystem(FileSystem fs, JobConf conf) {
    super(fs.getConf()); // not used
    
    this.baseFS = fs ; 
    this.jobid = conf.get("mapred.job.id"); 
    this.tipid = conf.get("mapred.tip.id"); 
    this.taskid = conf.get("mapred.task.id") ; 
    
    tempDir = new Path(baseFS.getConf().get("mapred.system.dir") ); 
  }
  /**
   * This Constructor should not be used in this or any derived class. 
   * @param conf
   */
  protected PhasedFileSystem(Configuration conf){
    super(conf);
    throw new UnsupportedOperationException("Operation not supported"); 
  }
  
  private Path setupFile(Path finalFile, boolean overwrite) throws IOException{
    if( finalNameToFileInfo.containsKey(finalFile) ){
      if( !overwrite ){
        throw new IOException("Error, file already exists : " + 
            finalFile.toString()); 
      }else{
        // delete tempp file and let create a new one. 
        FileInfo fInfo = finalNameToFileInfo.get(finalFile); 
        try{
          fInfo.getOpenFileStream().close();
        }catch(IOException ioe){
          // ignore if already closed
        }
        if( baseFS.exists(fInfo.getTempPath())){
          baseFS.delete( fInfo.getTempPath() );
        }
        finalNameToFileInfo.remove(finalFile); 
      }
    }
    
    String uniqueFile = jobid + "/" + tipid + "/" + taskid + "/" + finalFile.getName();
    
    Path tempPath = new Path(tempDir, new Path(uniqueFile)); 
    FileInfo fInfo = new FileInfo(tempPath, finalFile, overwrite); 
    
    finalNameToFileInfo.put(finalFile, fInfo);
    
    return tempPath ; 
  }
  
  @Override
  public FSOutputStream createRaw(
      Path f, boolean overwrite, short replication, long blockSize)
      throws IOException {
    
    // for reduce output its checked in job client but lets check it anyways
    // as tasks with side effect may write to locations not set in jobconf
    // as output path. 
    if( baseFS.exists(f) && !overwrite ){
      throw new IOException("Error creating file - already exists : " + f); 
    }
    FSOutputStream stream = 
      baseFS.createRaw(setupFile(f, overwrite), overwrite, replication, blockSize); 
    finalNameToFileInfo.get(f).setOpenFileStream(stream); 
    return stream; 
  }

  @Override
  public FSOutputStream createRaw(
      Path f, boolean overwrite, short replication, long blockSize,
      Progressable progress)
      throws IOException {
    if( baseFS.exists(f) && !overwrite ){
      throw new IOException("Error creating file - already exists : " + f); 
    }
    FSOutputStream stream = 
      baseFS.createRaw(setupFile(f, overwrite), overwrite, replication, 
          blockSize, progress);
    finalNameToFileInfo.get(f).setOpenFileStream(stream); 
    return stream ; 
  }
  /**
   * Commits a single file file to its final locations as passed in create* methods. 
   * If a file already exists in final location then temporary file is deleted. 
   * @param fPath path to final file. 
   * @throws IOException thrown if commit fails
   */
  public void commit(Path fPath) throws IOException{
    commit(fPath, true); 
  }
  
  // use extra method arg to avoid concurrentModificationException 
  // if committing using this method while iterating.  
  private void commit(Path fPath , boolean removeFromMap)throws IOException{
    FileInfo fInfo = finalNameToFileInfo.get(fPath) ; 
    if( null == fInfo ){
      throw new IOException("Error committing file! File was not created " + 
          "with PhasedFileSystem : " + fPath); 
    }
    try{
      fInfo.getOpenFileStream().close();
    }catch(IOException ioe){
      // ignore if already closed
      ioe.printStackTrace();
    }
    Path tempPath = fInfo.getTempPath(); 
    // ignore .crc files 
    if(! tempPath.toString().endsWith(".crc")){
      if( !baseFS.exists(fPath) || fInfo.isOverwrite()){
        if(! baseFS.exists(fPath.getParent())){
          baseFS.mkdirs(fPath.getParent());
        }
        
        if( baseFS.exists(fPath) && fInfo.isOverwrite()){
          baseFS.delete(fPath); 
        }
        
        try {
          if( ! baseFS.rename(fInfo.getTempPath(), fPath) ){
            // delete the temp file if rename failed
            baseFS.delete(fInfo.getTempPath());
          }
        }catch(IOException ioe){
          // rename failed, log error and delete temp files
          LOG.error("PhasedFileSystem failed to commit file : " + fPath 
              + " error : " + ioe.getMessage()); 
          baseFS.delete(fInfo.getTempPath());
        }
      }else{
        // delete temp file
        baseFS.delete(fInfo.getTempPath());
      }
      // done with the file
      if( removeFromMap ){
        finalNameToFileInfo.remove(fPath);
      }
    }
  }

  /**
   * Commits files to their final locations as passed in create* methods. 
   * If a file already exists in final location then temporary file is deleted. 
   * This methods ignores crc files (ending with .crc). This method doesnt close
   * the file system so it can still be used to create new files. 
   * @throws IOException if any file fails to commit
   */
  public void commit() throws IOException {
    for( Path fPath : finalNameToFileInfo.keySet()){
      commit(fPath, false);  
    }
    // safe to clear map now
    finalNameToFileInfo.clear();
  } 
  /**
   * Aborts a single file. The temporary created file is deleted. 
   * @param p the path to final file as passed in create* call
   * @throws IOException if File delete fails  
   */
  public void abort(Path p)throws IOException{
    abort(p, true); 
  }
  
  // use extra method arg to avoid concurrentModificationException 
  // if aborting using this method while iterating.  
  private void abort(Path p, boolean removeFromMap) throws IOException{
    FileInfo fInfo = finalNameToFileInfo.get(p); 
    if( null != fInfo ){
      try{
        fInfo.getOpenFileStream().close();
      }catch(IOException ioe){
        // ignore if already closed
      }
      baseFS.delete(fInfo.getTempPath()); 
      if( removeFromMap ){
        finalNameToFileInfo.remove(p);
      }
    }
  }
  /**
   * Aborts the file creation, all uncommitted files created by this PhasedFileSystem 
   * instance are deleted. This does not close baseFS because handle to baseFS may still 
   * exist can be used to create new files. 
   * 
   * @throws IOException
   */
  public void abort() throws IOException {
    for(Path fPath : finalNameToFileInfo.keySet() ){
      abort(fPath, false); 
    }
    // safe to clean now
    finalNameToFileInfo.clear();
  }
  /**
   * Closes base file system. 
   */
  public void close() throws IOException { 
    baseFS.close(); 
  } 
  
  @Override
  public short getReplication(
      Path src)
      throws IOException {
    // keep replication same for temp file as for 
    // final file. 
    return baseFS.getReplication(src);
  }

  @Override
  public boolean setReplicationRaw(
      Path src, short replication)
      throws IOException {
    // throw IOException for interface compatibility with 
    // base class. 
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public boolean renameRaw(
      Path src, Path dst)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public boolean deleteRaw(
      Path f)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public boolean exists(Path f)
      throws IOException {
    return baseFS.exists(f);
  }

  @Override
  public boolean isDirectory(Path f)
      throws IOException {
    return baseFS.isDirectory(f);  
  }

  @Override
  public long getLength(Path f)
      throws IOException {
    return baseFS.getLength(f); 
  }

  @Override
  public Path[] listPathsRaw(Path f)
      throws IOException {
    return baseFS.listPathsRaw(f);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    baseFS.setWorkingDirectory(new_dir);   
  }

  @Override
  public Path getWorkingDirectory() {
    return baseFS.getWorkingDirectory();  
  }

  @Override
  public boolean mkdirs(Path f)
      throws IOException {
    return baseFS.mkdirs(f) ;
  }

  @Override
  public void lock(
      Path f, boolean shared)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public void release(
      Path f)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public void copyFromLocalFile(
      Path src, Path dst)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public void moveFromLocalFile(
      Path src, Path dst)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public void copyToLocalFile(
      Path src, Path dst)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public Path startLocalOutput(
      Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
 }

  @Override
  public void completeLocalOutput(
      Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
 }

  @Override
  public void reportChecksumFailure(
      Path f, FSInputStream in, long start, long length, int crc) {
    baseFS.reportChecksumFailure(f, in, start, length, crc); 
  }

  @Override
  public long getBlockSize(
      Path f)
      throws IOException {
    return baseFS.getBlockSize(f);
  }

  @Override
  public long getDefaultBlockSize() {
    return baseFS.getDefaultBlockSize();
  }

  @Override
  public short getDefaultReplication() {
    return baseFS.getDefaultReplication();
  }

  @Override
  public String[][] getFileCacheHints(
      Path f, long start, long len)
      throws IOException {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException("Operation not supported");  
  }

  @Override
  public FSInputStream openRaw(Path f)
      throws IOException {
    return baseFS.openRaw(f);   
  }
  
  private class FileInfo {
    private Path tempPath ;
    private Path finalPath ; 
    private FSOutputStream openFileStream ; 
    private boolean overwrite ;
    
    FileInfo(Path tempPath, Path finalPath, boolean overwrite){
      this.tempPath = tempPath ; 
      this.finalPath = finalPath ; 
      this.overwrite = overwrite; 
    }
    public FSOutputStream getOpenFileStream() {
      return openFileStream;
    }
    public void setOpenFileStream(
        FSOutputStream openFileStream) {
      this.openFileStream = openFileStream;
    }
    public Path getFinalPath() {
      return finalPath;
    }
    public void setFinalPath(
        Path finalPath) {
      this.finalPath = finalPath;
    }
    public boolean isOverwrite() {
      return overwrite;
    }
    public void setOverwrite(
        boolean overwrite) {
      this.overwrite = overwrite;
    }
    public Path getTempPath() {
      return tempPath;
    }
    public void setTempPath(
        Path tempPath) {
      this.tempPath = tempPath;
    }
    
  }

}
