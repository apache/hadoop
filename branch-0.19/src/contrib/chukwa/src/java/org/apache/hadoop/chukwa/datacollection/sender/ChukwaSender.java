package org.apache.hadoop.chukwa.datacollection.sender;

/**
 * Encapsulates all of the communication overhead needed for chunks to be delivered
 * to a collector.
 */
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.sender.ChukwaHttpSender.CommitListEntry;

public interface ChukwaSender {

  /**
   * 
   * @param chunksToSend a list of chunks to commit
   * @return the list of committed chunks
   * @throws InterruptedException if interrupted while trying to send 
   */
  public List<CommitListEntry> send(List<Chunk> chunksToSend) throws InterruptedException, java.io.IOException;
  
  public void setCollectors(Iterator<String> collectors);
  
}
