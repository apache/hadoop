package org.apache.hadoop.chukwa.datacollection;

import org.apache.hadoop.chukwa.Chunk;

public interface ChunkReceiver {
  
  /**
   *  Add a chunk to the queue, potentially blocking.
   * @param event
   * @throws InterruptedException if thread is interrupted while blocking
   */
  public void add(Chunk event) throws java.lang.InterruptedException;
  

}
