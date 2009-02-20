/*
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
package org.apache.hadoop.chukwa.datacollection.collector.servlet;

import java.io.PrintStream;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * One per post
 */
public class ServletDiagnostics {

  static Logger log=  Logger.getLogger(ServletDiagnostics.class);
  
  private static class PostStats { //statistics about a chunk
    public PostStats(String src, int count, long receivedTs)
    {
      this.count = count;
      this.src = src;
      this.receivedTs = receivedTs;
      types = new String[count];
      names = new String[count];
      lengths = new int[count];
      
      seenChunkCount = 0;
      dataSize = 0;
    }
    final int count;
    final String src;
    final long receivedTs;
    final String[] types, names;
    final int[] lengths;
    
    int seenChunkCount;
    long dataSize;
    public void addChunk(ChunkImpl c, int position)
    {
      if(position != seenChunkCount)
        log.warn("servlet collector is passing chunk " + position + " but diagnostics has seen" +
            seenChunkCount);
      else if(seenChunkCount >= count){
        log.warn("too many chunks in post declared as length " +count);
      } else {
        types[seenChunkCount] = c.getDataType(); 
        lengths[seenChunkCount] = c.getData().length;
        names[seenChunkCount] = c.getStreamName();
        dataSize += c.getData().length;
        ++seenChunkCount;
      }
    }
  }
  
  static {
    lastPosts = new LinkedList<PostStats>();
  }

  static LinkedList<PostStats> lastPosts;
  PostStats curPost;
  static int CHUNKS_TO_KEEP = 300;

  
  public void sawPost(String source, int chunks, long receivedTs) {
    if(curPost != null) {
      log.warn("should only have one HTTP post per ServletDiagnostics");
      doneWithPost();
    }
    curPost = new PostStats(source, chunks, receivedTs);
  }
  
  public void sawChunk(ChunkImpl c, int pos) {
    curPost.addChunk(c, pos);
  }

  public static void printPage(PrintStream out) {
    
    HashMap<String, Long> bytesFromHost = new HashMap<String, Long>();    
    long timeWindowOfSample = Long.MAX_VALUE;
    long now = System.currentTimeMillis();


    out.println("<ul>");
    
    synchronized(lastPosts) {
      if(!lastPosts.isEmpty())
        timeWindowOfSample = now -  lastPosts.peek().receivedTs;
      
      for(PostStats stats: lastPosts) {
        out.print("<li>");
        
        out.print(stats.dataSize + " bytes from " + stats.src + " at timestamp " + stats.receivedTs);
        out.println(" which was " +  ((now - stats.receivedTs)/ 1000) + " seconds ago");
        Long oldBytes = bytesFromHost.get(stats.src);
        long newBytes = stats.dataSize;
        if(oldBytes != null)
          newBytes += oldBytes;
        bytesFromHost.put(stats.src, newBytes);
        out.println("<ol>");
        for(int i =0; i < stats.count; ++i)
          out.println("<li> "+ stats.lengths[i] + " bytes of type " +
              stats.types[i] + ".  Adaptor name ="+ stats.names[i] +" </li>");
        out.println("</ol></li>");
      }
    }
    out.println("</ul>");
    out.println("<ul>");
    for(Map.Entry<String, Long> h: bytesFromHost.entrySet()) {
      out.print("<li>rate from " + h.getKey() + " was " + (1000 * h.getValue() / timeWindowOfSample));
      out.println(" bytes/second in last " + timeWindowOfSample/1000 + " seconds.</li>");
    }
    

    out.println("</ul>");    
    out.println("total of " + bytesFromHost.size() + " unique hosts seen");

    out.println("<p>current time is " + System.currentTimeMillis() + " </p>");
  }

  public void doneWithPost() {
    synchronized(lastPosts) {
      if(lastPosts.size() > CHUNKS_TO_KEEP)
        lastPosts.remove();
      lastPosts.add(curPost);
    }
  }
  
}
