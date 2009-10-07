/**
 * Copyright 2007 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

/**
 * Writes the records into a Lucene index writer.
 */
public class IndexRecordWriter 
extends RecordWriter<ImmutableBytesWritable, LuceneDocumentWrapper> {

  static final Log LOG = LogFactory.getLog(IndexRecordWriter.class);
  
  private long docCount = 0;
  private TaskAttemptContext context = null;
  private FileSystem fs = null;
  private IndexWriter writer = null;
  private IndexConfiguration indexConf = null;
  private Path perm = null;
  private Path temp = null;
  
  /**
   * Creates a new instance.
   * 
   * @param context  The task context.
   * @param fs  The file system.
   * @param writer  The index writer.
   * @param indexConf  The index configuration.
   * @param perm  The permanent path in the DFS.
   * @param temp  The temporary local path.
   */
  public IndexRecordWriter(TaskAttemptContext context, FileSystem fs, 
      IndexWriter writer, IndexConfiguration indexConf, Path perm, Path temp) {
    this.context = context;
    this.fs = fs;
    this.writer = writer;
    this.indexConf = indexConf;
    this.perm = perm;
    this.temp = temp;
  }
  
  /**
   * Writes the record into an index.
   * 
   * @param key  The current key.
   * @param value  The current value.
   * @throws IOException When the index is faulty.
   * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
   */
  @Override
  public void write(ImmutableBytesWritable key, LuceneDocumentWrapper value)
  throws IOException {
    // unwrap and index doc
    Document doc = value.get();
    writer.addDocument(doc); 
    docCount++;
    context.progress();
  } 

  /**
   * Closes the writer.
   * 
   * @param context  The current context.
   * @throws IOException When closing the writer fails.
   * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void close(TaskAttemptContext context) throws IOException {
    // spawn a thread to give progress heartbeats
    HeartbeatsThread prog = new HeartbeatsThread();
    try {
      prog.start();

      // optimize index
      if (indexConf.doOptimize()) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Optimizing index.");
        }
        writer.optimize();
      }

      // close index
      writer.close();
      if (LOG.isInfoEnabled()) {
        LOG.info("Done indexing " + docCount + " docs.");
      }

      // copy to perm destination in dfs
      fs.completeLocalOutput(perm, temp);
      if (LOG.isInfoEnabled()) {
        LOG.info("Copy done.");
      }
    } finally {
      prog.setClosed();
    }
  }

  class HeartbeatsThread extends Thread {

    /** Flag to track when to finish. */
    private boolean closed = false;
    
    /**
     * Runs the thread. Sending heart beats to the framework.
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      context.setStatus("Closing");
      while (!closed) {
        try {
          context.progress();            
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          continue;
        } catch (Throwable e) {
          return;
        }
      }
    }
    
    /**
     * Switches the flag. 
     */
    public void setClosed() {
      closed = true;
    }
    
  }
  
}
