/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.commons.logging.*;

/**
 * A simple logger to handle the task-specific user logs.
 * This class uses the system property <code>hadoop.log.dir</code>.
 * 
 * @author Arun C Murthy
 */
class TaskLog {
  private static final Log LOG =
    LogFactory.getLog(TaskLog.class.getName());

  private static final File LOG_DIR = 
    new File(System.getProperty("hadoop.log.dir"), "userlogs");
  
  private static final String SPLIT_INDEX_NAME = "split.idx";
  
  static {
    if (!LOG_DIR.exists()) {
      LOG_DIR.mkdirs();
    }
  }

  /**
   * The log-writer responsible for handling writing user-logs
   * and maintaining splits and ensuring job-specifc limits 
   * w.r.t logs-size etc. are honoured.
   *  
   * @author Arun C Murthy
   */
  static class Writer {
    private String taskId;
    private JobConf conf;

    private final File taskLogDir;
    private final int noKeepSplits;
    private final long splitFileSize;
    private final boolean purgeLogSplits;
    private final int logsRetainHours;

    private boolean initialized = false;
    private long splitOffset = 0;
    private long splitLength = 0;
    private int noSplits = 0;
    
    private File currentSplit;            // current split filename
    private OutputStream out;               // current split
    private OutputStream splitIndex;        // split index file
    
    private int flushCtr = 0;
    private final static int FLUSH_BYTES = 256;

    /**
     * Creates a new TaskLog writer.
     * @param conf configuration of the task
     * @param taskId taskid of the task
     */
    Writer(JobConf conf, String taskId) {
      this.conf = conf;
      this.taskId = taskId;
      this.taskLogDir = new File(LOG_DIR, this.taskId);
      
      noKeepSplits = this.conf.getInt("mapred.userlog.num.splits", 4);
      splitFileSize = 
        (this.conf.getInt("mapred.userlog.limit.kb", 100) * 1024) / noKeepSplits; 
      purgeLogSplits = this.conf.getBoolean("mapred.userlog.purgesplits", 
                                      true);
      logsRetainHours = this.conf.getInt("mapred.userlog.retain.hours", 12);
    }
    
    private static class TaskLogsPurgeFilter implements FileFilter {
      long purgeTimeStamp;
    
      TaskLogsPurgeFilter(long purgeTimeStamp) {
        this.purgeTimeStamp = purgeTimeStamp;
      }

      public boolean accept(File file) {
        LOG.debug("PurgeFilter - file: " + file + ", mtime: " + file.lastModified() + ", purge: " + purgeTimeStamp);
        return file.lastModified() < purgeTimeStamp;
      }
    }

    private File getLogSplit(int split) {
      String splitName = "part-" + String.format("%1$06d", split);
      return new File(taskLogDir, splitName); 
    }
    
    private void deleteDir(File dir) throws IOException {
      File[] files = dir.listFiles();
      if (files != null) {
        for (int i=0; i < files.length; ++i) {
          files[i].delete();
        }
      }
      boolean del = dir.delete();
      LOG.debug("Deleted " + del + ": " + del);
    }
    
    /**
     * Initialize the task log-writer.
     * 
     * @throws IOException
     */
    public synchronized void init() throws IOException {
      if (!initialized) {
        // Purge logs of tasks on this tasktracker if their  
        // mtime has exceeded "mapred.task.log.retain" hours
        long purgeTimeStamp = System.currentTimeMillis() - 
                              (logsRetainHours*60*60*1000);
        File[] oldTaskLogs = LOG_DIR.listFiles(
                                new TaskLogsPurgeFilter(purgeTimeStamp)
                              );
        if (oldTaskLogs != null) {
          for (int i=0; i < oldTaskLogs.length; ++i) {
            deleteDir(oldTaskLogs[i]);
          }
        }

        // Initialize the task's log directory
        if (taskLogDir.exists()) {
          deleteDir(taskLogDir);
        }
        taskLogDir.mkdirs();
        
        // Create the split index
        splitIndex = new BufferedOutputStream(
            new FileOutputStream(new File(taskLogDir, SPLIT_INDEX_NAME))
            );

        out = createLogSplit(noSplits);
        initialized = true;
      }
    }
    
    /**
     * Write a log message to the task log.
     * 
     * @param b bytes to be writter
     * @param off start offset
     * @param len length of data
     * @throws IOException
     */
    public synchronized void write(byte[] b, int off, int len) 
    throws IOException {
      // Check if we need to rotate the log
      if (splitLength > splitFileSize) {
        LOG.debug("Total no. of bytes written to split#" + noSplits + 
            " -> " + splitLength);
        logRotate();
      }
      
      // Periodically flush data to disk
      if (flushCtr > FLUSH_BYTES) {
        out.flush();
        flushCtr = 0;
      }
      
      // Write out to the log-split
      out.write(b, off, len);
      splitLength += len;
      flushCtr += len;
    }
    
    /**
     * Close the task log.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
      // Close the final split
      if (out != null) {
        out.close();
      }

      // Close the split-index
      if (splitIndex != null) {
        writeIndexRecord();
        splitIndex.close();
      }
    }

    private synchronized OutputStream createLogSplit(int split) 
    throws IOException {
      currentSplit =  getLogSplit(split);
      LOG.debug("About to create the split: " + currentSplit);
      return new BufferedOutputStream(new FileOutputStream(currentSplit));
    }
    
    private synchronized void writeIndexRecord() throws IOException {
      String indexRecord = new String(currentSplit + "|" + 
          splitOffset + "|" + splitLength + "\n");
      splitIndex.write(indexRecord.getBytes());
      splitIndex.flush();
    }
    
    private synchronized void logRotate() throws IOException {
      // Close the current split
      LOG.debug("About to rotate-out the split: " + noSplits);
      out.close();
      
      // Record the 'split' in the index
      writeIndexRecord();
      
      // Re-initialize the state
      splitOffset += splitLength;
      splitLength = 0;
      flushCtr = 0;

      // New 'split'
      ++noSplits;

      // Check if we need to purge an old split
      if (purgeLogSplits) {
        if (noSplits >= noKeepSplits) {   // noSplits is zero-based
          File purgeLogSplit = getLogSplit((noSplits-noKeepSplits));
          purgeLogSplit.delete();
          LOG.debug("Purged log-split #" + (noSplits-noKeepSplits) + " - " + 
              purgeLogSplit);
        }
      }
      
      // Rotate the log
      out = createLogSplit(noSplits); 
    }
    
  } // TaskLog.Writer

  /**
   * The log-reader for reading the 'split' user-logs.
   *
   * @author Arun C Murthy
   */
  static class Reader {
    private String taskId;
    private File taskLogDir;
    private boolean initialized = false;
    
    private IndexRecord[] indexRecords = null;
    private BufferedReader splitIndex;
    
    private long logFileSize = 0;
    
    /**
     * Create a new task log reader.
     * 
     * @param taskId task id of the task.
     */
    public Reader(String taskId) {
      this.taskId = taskId;
      this.taskLogDir = new File(LOG_DIR, this.taskId);
    }

    private static class IndexRecord {
      String splitName;
      long splitOffset;
      long splitLength;
      
      IndexRecord(String splitName, long splitOffset, long splitLength) {
        this.splitName = splitName;
        this.splitOffset = splitOffset;
        this.splitLength = splitLength;
      }
    }
    
    private synchronized void init() throws IOException {
      this.splitIndex = new BufferedReader(new InputStreamReader(
                          new FileInputStream(new File(taskLogDir, SPLIT_INDEX_NAME))
                          ));

      // Parse the split-index and store the offsets/lengths
      ArrayList<IndexRecord> records = new ArrayList<IndexRecord>();
      String line;
      while ((line = splitIndex.readLine()) != null) {
        String[] fields = line.split("\\|");
        if (fields.length != 3) {
          throw new IOException("Malformed split-index with " + 
              fields.length + " fields");
        }
        
        IndexRecord record = new IndexRecord(
                                fields[0], 
                                Long.valueOf(fields[1]).longValue(), 
                                Long.valueOf(fields[2]).longValue()
                              );
        LOG.debug("Split: <" + record.splitName + ", " + record.splitOffset + 
            ", " + record.splitLength + ">");
        
        // Save 
        records.add(record);
        logFileSize += record.splitLength;
      }

      indexRecords = new IndexRecord[records.size()];
      indexRecords = records.toArray(indexRecords);
      initialized = true;
      LOG.debug("Log size: " + logFileSize);
    }

    /**
     * Return the total 'logical' log-size written by the task, including
     * purged data.
     * 
     * @return the total 'logical' log-size written by the task, including
     *         purged data.
     * @throws IOException
     */
    public long getTotalLogSize() throws IOException {
      if (!initialized) {
        init();
      }
      
      return logFileSize;
    }
    /**
     * Return the entire user-log (remaining splits).
     * 
     * @return Returns a <code>byte[]</code> containing the data in user-log.
     * @throws IOException
     */
    public byte[] fetchAll() throws IOException {
      if (!initialized) {
        init();
      }
      
      // Get all splits 
      Vector<InputStream> streams = new Vector<InputStream>();
      int totalLogSize = 0;
      for (int i=0; i < indexRecords.length; ++i) {
        InputStream stream = getLogSplit(i);
        if (stream != null) {
          streams.add(stream);
          totalLogSize += indexRecords[i].splitLength;
          LOG.debug("Added split: " + i);
        }
      }
      LOG.debug("Total log-size on disk: " + totalLogSize + 
          "; actual log-size: " + logFileSize);

      // Copy log data into buffer
      byte[] b = new byte[totalLogSize];
      SequenceInputStream in = new SequenceInputStream(streams.elements());
      int bytesRead = 0, totalBytesRead = 0;
      int off = 0, len = totalLogSize;
      LOG.debug("Attempting to read " + len + " bytes from logs");
      while ((bytesRead = in.read(b, off, len)) > 0) {
        LOG.debug("Got " + bytesRead + " bytes");
        off += bytesRead;
        len -= bytesRead;
        
        totalBytesRead += bytesRead;
      }

      if (totalBytesRead != totalLogSize) {
        LOG.debug("Didn't not read all requisite data in logs!");
      }
      
      return b;
    }
    
    /**
     * Tail the user-log.
     * 
     * @param b the buffer into which the data is read.
     * @param off the start offset in array <code>b</code>
     *            at which the data is written.
     * @param len the maximum number of bytes to read.
     * @param tailSize the no. of bytes to be read from end of file.
     * @param tailWindow the sliding window for tailing the logs.
     * @return the total number of bytes of user-logs dataread into the buffer.
     * @throws IOException
     */
    public synchronized int tail(byte[] b, int off, int len, 
        long tailSize, int tailWindow) 
    throws IOException {
      if (!initialized) {
        init();
      }
      
      LOG.debug("tailSize: " + tailSize + " - tailWindow: " + tailWindow);
      
      if (tailSize*tailWindow > logFileSize) {
        tailSize = logFileSize;
        tailWindow = 1;
      }
      
      return read(b, off, len, 
          (long)(logFileSize-(tailSize*tailWindow)), tailSize);
    }

    /**
     * Read user-log data given an offset/length.
     * 
     * @param b the buffer into which the data is read.
     * @param off the start offset in array <code>b</code>
     *            at which the data is written.
     * @param len the maximum number of bytes to read.
     * @param logOffset the offset of the user-log from which to get data.
     * @param logLength the maximum number of bytes of user-log data to fetch. 
     * @return the total number of bytes of user-logs dataread into the buffer.
     * @throws IOException
     */
    public synchronized int read(byte[] b, int off, int len, 
        long logOffset, long logLength) 
    throws IOException {
      LOG.debug("TaskeLog.Reader.read: logOffset: " + logOffset + " - logLength: " + logLength);

      // Sanity check
      if (logLength == 0) {
        return 0;
      }
      
      if (!initialized) {
        init();
      }
      
      // Locate the requisite splits 
      Vector<InputStream> streams = new Vector<InputStream>();
      long offset = logOffset;
      int startIndex = -1, stopIndex = -1;
      boolean inRange = false;
      for (int i=0; i < indexRecords.length; ++i) {
        LOG.debug("offset: " + offset + " - (split, splitOffset) : (" + 
            i + ", " + indexRecords[i].splitOffset + ")");
        
        if (offset <= indexRecords[i].splitOffset) {
          if (!inRange) {
            startIndex = i - ((i > 0) ? 1 : 0);
            LOG.debug("Starting at split: " + startIndex);
            offset += logLength;
            InputStream stream = getLogSplit(startIndex);
            if (stream != null) {
              streams.add(stream);
            }
            LOG.debug("Added split: " + startIndex);
            inRange = true;
          } else {
            stopIndex = i-1;
            LOG.debug("Stop at split: " + stopIndex);
            break;
          }
        }
        
        if (inRange) {
          InputStream stream = getLogSplit(i);
          if (stream != null) {
            streams.add(stream);
          }
          LOG.debug("Added split: " + i);
        }
      }
      if (startIndex == -1) {
        throw new IOException("Illegal logOffset/logLength");
      }
      if (stopIndex == -1) {
        stopIndex = indexRecords.length - 1;
        LOG.debug("Stop at split: " + stopIndex);
        
        // Check if request exceeds the log-file size
        if ((logOffset+logLength) > logFileSize) {
          LOG.debug("logOffset+logLength exceeds log-file size");
          logLength = logFileSize - logOffset;
        }
      }
      
      // Copy requisite data into user buffer
      SequenceInputStream in = new SequenceInputStream(streams.elements());
      if (streams.size() == (stopIndex - startIndex +1)) {
        // Skip to get to 'logOffset' if logs haven't been purged
        long skipBytes = 
          in.skip(logOffset - indexRecords[startIndex].splitOffset);
        LOG.debug("Skipped " + skipBytes + " bytes from " + 
            startIndex + " stream");
      }
      int bytesRead = 0, totalBytesRead = 0;
      len = Math.min((int)logLength, len);
      LOG.debug("Attempting to read " + len + " bytes from logs");
      while ((bytesRead = in.read(b, off, len)) > 0) {
        off += bytesRead;
        len -= bytesRead;
        
        totalBytesRead += bytesRead;
      }
      
      return totalBytesRead;
    }

    private synchronized InputStream getLogSplit(int split) 
    throws IOException {
      String splitName = indexRecords[split].splitName;
      LOG.debug("About to open the split: " + splitName);
      InputStream in = null;
      try {
        in = new BufferedInputStream(new FileInputStream(new File(splitName)));
      } catch (FileNotFoundException fnfe) {
        in = null;
        LOG.debug("Split " + splitName + " not found... probably purged!");
      }
      
      return in;
    }

  } // TaskLog.Reader

} // TaskLog
