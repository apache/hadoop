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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

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
	public static final int DEFAULT_MAX_READ_SIZE = 128 * 1024 ;
	public static int MAX_READ_SIZE = DEFAULT_MAX_READ_SIZE ;
	public static int MAX_RETRIES = 300;
	public static int GRACEFUL_PERIOD = 3 * 60 * 1000; // 3 minutes
  
	protected static Configuration conf = null;
	private int attempts = 0;
	private long gracefulPeriodExpired = 0l;
	private boolean adaptorInError = false;
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
	
	private static FileTailer tailer;

	static {
		tailer = new FileTailer();
		log =Logger.getLogger(FileTailingAdaptor.class);
	}

	public void start(long adaptorID, String type, String params, long bytes, ChunkReceiver dest) {
	    //in this case params = filename 
		log.info("started file tailer on file " + params);
		this.adaptorID = adaptorID;
	    this.type = type;
	    this.dest = dest;
	    this.attempts = 0;
			
	    Pattern cmd = Pattern.compile("(\\d+)\\s+(.+)\\s");
	    Matcher m = cmd.matcher(params);
	    if(m.matches()) {
	        offsetOfFirstByte = Long.parseLong(m.group(1));
	        toWatch = new File(m.group(2));
	    } else {
	        toWatch = new File(params.trim());
	    }
	  
		this.fileReadOffset= bytes;
		tailer.startWatchingFile(this);
	}

	/**
	 * Do one last tail, and then stop
	 * @see org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor#shutdown()
	 */
	public long shutdown() throws AdaptorException {
	  try{
	    if(toWatch.exists()) {
	    	int retry=0;
	    	tailer.stopWatchingFile(this);
			TerminatorThread lastTail = new TerminatorThread(this,tailer.eq);
			lastTail.setDaemon(true);
			lastTail.start();
			while(lastTail.isAlive() && retry < 60) {
				try {
					log.info("Retry:"+retry);
				    Thread.currentThread().sleep(1000);
				    retry++;
				} catch(InterruptedException ex) {
				}
			}
	    }
	  } finally {
	    return fileReadOffset + offsetOfFirstByte;
	  }

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
		return type.trim() + " " + offsetOfFirstByte+ " " + toWatch.getPath() + " " + fileReadOffset;
		// can make this more efficient using a StringBuilder
	}

	public String toString() {
		return "Tailer on " + toWatch;
	}

	public String getStreamName() {
		return toWatch.getPath();
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
	      if( (adaptorInError == true)  && (System.currentTimeMillis() > gracefulPeriodExpired)) {
	        if (!toWatch.exists()) {
	          log.warn("Adaptor|" + adaptorID +"|attempts=" +  attempts + "| File does not exist: "+toWatch.getAbsolutePath()+", streaming policy expired.  File removed from streaming.");
	        } else if (!toWatch.canRead()) {
	          log.warn("Adaptor|" + adaptorID +"|attempts=" +  attempts + "| File cannot be read: "+toWatch.getAbsolutePath()+", streaming policy expired.  File removed from streaming.");
	        } else {
	          // Should have never been there
	          adaptorInError = false;
	          gracefulPeriodExpired = 0L;
	          attempts = 0;
	          return false;
	        }


	        ChukwaAgent agent = ChukwaAgent.getAgent();
	        if (agent != null) {
	          agent.stopAdaptor(adaptorID, false);
	        } else {
	          log.info("Agent is null, running in default mode");
	        }
	        return false;

	      } else if(!toWatch.exists() || !toWatch.canRead()) {
	        if (adaptorInError == false) {
	          long now = System.currentTimeMillis();
	          gracefulPeriodExpired = now + GRACEFUL_PERIOD;
	          adaptorInError = true;
	          attempts = 0;
	          log.warn("failed to stream data for: "+toWatch.getAbsolutePath()+", graceful period will Expire at now:" + now 
	              + " + " +  GRACEFUL_PERIOD + " secs, i.e:" + gracefulPeriodExpired);
	        } else if (attempts%10 == 0) {
	            log.info("failed to stream data for: "+toWatch.getAbsolutePath()+", attempt: "+attempts);  
	        }

	        attempts++;
	        return false;  //no more data
	      } 
	        
	      	if (reader == null)
	      	{
	      		reader = new RandomAccessFile(toWatch, "r");
	      		log.info("Adaptor|" + adaptorID + "|Opening the file for the first time|seek|" + fileReadOffset);
	      	}
	      	
	      	long len = 0L;
	    	try {
		      	RandomAccessFile newReader = new RandomAccessFile(toWatch,"r");
		    	len = reader.length();
		    	long newLength = newReader.length();
		      	if(newLength<len && fileReadOffset >= len) {
		      		reader.close();
		      		reader = newReader;
		      		fileReadOffset=0L;
		      		log.debug("Adaptor|" + adaptorID +"| File size mismatched, rotating: "+toWatch.getAbsolutePath());
		      	} else {
		      		try {
		      		    newReader.close();
		      		} catch(IOException e) {
		      			// do nothing.
		      		}
		      	}
	    	} catch(IOException e) {
      			// do nothing, if file doesn't exist.	    		
	    	}
	    	if (len >= fileReadOffset) {
	    		if(offsetOfFirstByte>fileReadOffset) {
	    			// If the file rotated, the recorded offsetOfFirstByte is greater than file size,
	    			// reset the first byte position to beginning of the file.	
	    			fileReadOffset=0;
	    			offsetOfFirstByte = 0L;       
	    			log.warn("offsetOfFirstByte>fileReadOffset, resetting offset to 0");
	    		}
	    		
	    		log.debug("Adaptor|" + adaptorID + "|seeking|" + fileReadOffset );
	    		reader.seek(fileReadOffset);
	    
	    		long bufSize = len - fileReadOffset;
	    		
	    		if (conf == null)
	    		{
	    			ChukwaAgent agent = ChukwaAgent.getAgent();
	    			if (agent != null)
	    			{
	    				conf = agent.getConfiguration();
	        			if (conf != null)
	        			{
	        				MAX_READ_SIZE= conf.getInt("chukwaAgent.fileTailingAdaptor.maxReadSize", DEFAULT_MAX_READ_SIZE);
	        				log.info("chukwaAgent.fileTailingAdaptor.maxReadSize: " + MAX_READ_SIZE);
	        			}	
	        			else
	        			{
	        				log.info("Conf is null, running in default mode");
	        			}
	    			}
	    			else
	    			{
	    				log.info("Agent is null, running in default mode");
	    			}
	    		}
	    		
	    		if (bufSize > MAX_READ_SIZE) {
	    			bufSize = MAX_READ_SIZE;
	    			hasMoreData = true;
	    		}
	    		byte[] buf = new byte[(int) bufSize];
	    		
	    		
	    		long curOffset = fileReadOffset;
	    		
	    		int bufferRead = reader.read(buf);
	    		assert reader.getFilePointer() == fileReadOffset + bufSize : " event size arithmetic is broken: "
	    		  + " pointer is "
	    		  + reader.getFilePointer()
	    		  + " but offset is " + fileReadOffset + bufSize;

	    		int bytesUsed = extractRecords(dest, fileReadOffset + offsetOfFirstByte, buf);

	    		// ===   WARNING   ===
	    		// If we couldn't found a complete record AND
	    		// we cannot read more, i.e bufferRead == MAX_READ_SIZE 
	    		// it's because the record is too BIG
	    		// So log.warn, and drop current buffer so we can keep moving
	    		// instead of being stopped at that point for ever
	    		if ( bytesUsed == 0 && bufferRead ==  MAX_READ_SIZE) {
	    		  log.warn("bufferRead == MAX_READ_SIZE AND bytesUsed == 0, droping current buffer: startOffset=" 
	    		      + curOffset + ", MAX_READ_SIZE=" + MAX_READ_SIZE + ", for " + toWatch.getPath());
	    		  bytesUsed = buf.length;
	    		}

	    		fileReadOffset = fileReadOffset + bytesUsed;
	    		
	    		
	    		log.debug("Adaptor|" + adaptorID + "|start|" + curOffset + "|end|"+ fileReadOffset);
	    		
	    		
	    	} else {
	    	  // file has rotated and no detection
	    	  reader.close();
	    	  reader=null;
	    	  fileReadOffset = 0L;
	    	  offsetOfFirstByte = 0L;
	    	  hasMoreData = true;
	    	  log.warn("Adaptor|" + adaptorID +"| file: " + toWatch.getPath() +", has rotated and no detection - reset counters to 0L");	    	
	    	}
	    } catch (IOException e) {
	    	log.warn("failure reading " + toWatch, e);
	    }
	    attempts=0;
	    adaptorInError = false;
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
