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

package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * An adaptor that repeatedly tails a specified file, sending the new bytes.
 * This class does not split out records, but just sends everything up to end of file.
 * Subclasses can alter this behavior by overriding extractRecords().
 * 
 */
public class FileTailingAdaptor implements Adaptor
{

	static Logger log;

	/**
	 * This is the maximum amount we'll read from any one file before moving on
	 * to the next. This way, we get quick response time for other files if one
	 * file is growing rapidly.
	 */
	public static final int MAX_READ_SIZE = 128 * 1024;

	File toWatch;
	/**
	 * next PHYSICAL offset to read
	 */
	protected long fileReadOffset;
	protected String type;
	private ChunkReceiver dest;
	
	/**
	 * The logical offset of the first byte of the file
	 */
	private long offsetOfFirstByte = 0;
	
	private static FileTailer tailer;

	static {
		tailer = new FileTailer();
		log =Logger.getLogger(FileTailingAdaptor.class);
	}

	public void start(String type, String params, long bytes, ChunkReceiver dest) {
	  //in this case params = filename 
		log.info("started file tailer on file " + params);
	  this.type = type;
	  this.dest = dest;
			  
	  String[] words = params.split(" ");
	  if(words.length > 1) {
	    offsetOfFirstByte = Long.parseLong(words[0]);
	    toWatch = new File(params.substring(words[0].length() + 1));
	  }
	  else
	    toWatch = new File(params);
	  
		this.fileReadOffset= bytes - offsetOfFirstByte;
		tailer.startWatchingFile(this);
	}

	/**
	 * Do one last tail, and then stop
	 * @see org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor#shutdown()
	 */
	public long shutdown() throws AdaptorException {
	  try{
	    tailFile(tailer.eq); // get tail end of file.
	  } catch(InterruptedException e) {
	    Thread.currentThread().interrupt();
	  }
		hardStop();//need to do this afterwards, so that offset stays visible during tailFile().
		return fileReadOffset + offsetOfFirstByte;
	}
	/**
	 * Stop tailing the file, effective immediately.
	 */
	public void hardStop() throws AdaptorException {
    tailer.stopWatchingFile(this);
	}

  /**
   * @see org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor#getCurrentStatus()
   */
	public String getCurrentStatus() {
		return type + " " + offsetOfFirstByte+ " " + toWatch.getPath();
		// can make this more efficient using a StringBuilder
	}

	public String toString() {
		return "Tailer on " + toWatch;
	}

	/**
	 * Looks at the tail of the associated file, adds some of it to event queue
	 * This method is not thread safe. Returns true if there's more data in the
	 * file
	 * 
	 * @param eq the queue to write Chunks into
	 */
	public synchronized boolean tailFile(ChunkReceiver eq) throws InterruptedException {
    boolean hasMoreData = false;
    try {
      if(!toWatch.exists())
        return false;  //no more data
      
    	RandomAccessFile reader = new RandomAccessFile(toWatch, "r");
    	long len = reader.length();
    	if (len > fileReadOffset) {
    		reader.seek(fileReadOffset);
    
    		long bufSize = len - fileReadOffset;
    		if (bufSize > MAX_READ_SIZE) {
    			bufSize = MAX_READ_SIZE;
    			hasMoreData = true;
    		}
    		byte[] buf = new byte[(int) bufSize];
    		reader.read(buf);
    		assert reader.getFilePointer() == fileReadOffset + bufSize : " event size arithmetic is broken: "
    				+ " pointer is "
    				+ reader.getFilePointer()
    				+ " but offset is " + fileReadOffset + bufSize;
    
    		int bytesUsed = extractRecords(dest, fileReadOffset + offsetOfFirstByte, buf);
    		fileReadOffset = fileReadOffset + bytesUsed;
    	}
    	reader.close();
    } catch (IOException e) {
    	log.warn("failure reading " + toWatch, e);
    }
    return hasMoreData;
	}
	
  /**
   * Extract records from a byte sequence
   * @param eq the queue to stick the new chunk[s] in
   * @param buffOffsetInFile the byte offset in the stream at which buf[] begins
   * @param buf the byte buffer to extract records from
   * @return the number of bytes processed
   * @throws InterruptedException
   */
  protected int extractRecords(ChunkReceiver eq, long buffOffsetInFile, byte[] buf)
      throws InterruptedException
  {
    ChunkImpl chunk = new ChunkImpl(type, toWatch.getAbsolutePath(), buffOffsetInFile + buf.length,
        buf, this);

    eq.add(chunk);
    return buf.length;
  }

  @Override
  public String getType() {
    return type;
  }


}
