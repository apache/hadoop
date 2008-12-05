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

package org.apache.hadoop.chukwa.datacollection.adaptor;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.util.RecordConstants; 
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;


/**
 * File Adaptor push small size file in one chunk to collector
 */
public class FileAdaptor  implements Adaptor
{

	static Logger log;

	protected static Configuration conf = null;
	private int attempts = 0;
	
	File toWatch;
	/**
	 * next PHYSICAL offset to read
	 */
	protected long fileReadOffset;
	protected String type;
	private ChunkReceiver dest;
	protected RandomAccessFile reader = null;
	protected long adaptorID;
	
	/**
	 * The logical offset of the first byte of the file
	 */
	private long offsetOfFirstByte = 0;
	
	static {
		log =Logger.getLogger(FileAdaptor.class);
	}

	public void start(long adaptorID, String type, String params, long bytes, ChunkReceiver dest) {
	    //in this case params = filename 
		log.info("adaptor id: "+adaptorID+" started file adaptor on file " + params);
		this.adaptorID = adaptorID;
	    this.type = type;
	    this.dest = dest;
	    this.attempts = 0;
			  
	    String[] words = params.split(" ");
	    if(words.length > 1) {
	        offsetOfFirstByte = Long.parseLong(words[0]);
	        toWatch = new File(params.substring(words[0].length() + 1));
	    } else {
	        toWatch = new File(params);
	    }
	    try {
	  		reader = new RandomAccessFile(toWatch, "r");
	  		long bufSize = toWatch.length();
			byte[] buf = new byte[(int) bufSize];
			reader.read(buf);
	        long fileTime = toWatch.lastModified();
			int bytesUsed = extractRecords(dest, 0, buf, fileTime);
	    } catch(Exception e) {
	        e.printStackTrace();
	    }
		ChukwaAgent agent = ChukwaAgent.getAgent();
		if (agent != null) {
			agent.stopAdaptor(adaptorID, false);
		} else {
			log.info("Agent is null, running in default mode");
		}
		this.fileReadOffset= bytes;
	}

	/**
	 * Do one last tail, and then stop
	 * @see org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor#shutdown()
	 */
	public long shutdown() throws AdaptorException {
	  hardStop();
	  return fileReadOffset + offsetOfFirstByte;
	}
	/**
	 * Stop tailing the file, effective immediately.
	 */
	public void hardStop() throws AdaptorException {
	}

	public String getStreamName() {
		return toWatch.getPath();
	}
	
  /**
   * Extract records from a byte sequence
   * @param eq the queue to stick the new chunk[s] in
   * @param buffOffsetInFile the byte offset in the stream at which buf[] begins
   * @param buf the byte buffer to extract records from
   * @return the number of bytes processed
   * @throws InterruptedException
   */
  protected int extractRecords(ChunkReceiver eq, long buffOffsetInFile, byte[] buf, long fileTime) throws InterruptedException {
    ChunkImpl chunk = new ChunkImpl(type, toWatch.getAbsolutePath(), buffOffsetInFile + buf.length,
        buf, this);
    String tags = chunk.getTags();
    chunk.setTags(tags+" time=\""+fileTime+"\"");
    eq.add(chunk);
    return buf.length;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public String getCurrentStatus() throws AdaptorException {
    return type.trim() + " " + offsetOfFirstByte+ " " + toWatch.getPath() + " " + fileReadOffset;
  }
  
}