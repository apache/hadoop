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

package org.apache.hadoop.chukwa.datacollection.writer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

/**
 * This class <b>is</b> thread-safe -- rotate() and save() both synchronize on
 * this object.
 * 
 */
public class SeqFileWriter implements ChukwaWriter
{
	public static final boolean ENABLE_ROTATION = true;
	
	static final int STAT_INTERVAL_SECONDS = 30;
	static final Object lock = new Object();
	
    static Logger log = Logger.getLogger(SeqFileWriter.class);
  
	private FileSystem fs = null;
	private ChukwaConfiguration conf = null;

	private String outputDir = null;
	private Calendar calendar = Calendar.getInstance();

	private Path currentPath = null;
	private String currentFileName = null;
	private FSDataOutputStream currentOutputStr = null;
	private static SequenceFile.Writer seqFileWriter = null;
	
	private static ClientAck clientAck = new ClientAck();
	private static long nextRotate = 0;
	private static int rotateInterval = 1000*60;
	
	private static Timer clientAckTimer = null;
	
	private Timer timer = null;

	private Timer statTimer = null;
	private volatile long dataSize = 0;

	
	private int initWriteChunkRetries = 10;
	private int writeChunkRetries = initWriteChunkRetries;
	
	public SeqFileWriter() throws WriterException
	{
		conf = new ChukwaConfiguration(true);
		init();
	}

	public void init() throws WriterException
	{
		outputDir = conf.get("chukwaCollector.outputDir", "/chukwa");

		rotateInterval = conf.getInt("chukwaCollector.rotateInterval",
				1000 * 60 * 5);//defaults to 5 minutes
		nextRotate = System.currentTimeMillis() + rotateInterval;
		
		initWriteChunkRetries = conf.getInt("chukwaCollector.writeChunkRetries", 10);
		writeChunkRetries = initWriteChunkRetries;
		
		//check if they've told us the file system to use
	    String fsname = conf.get("writer.hdfs.filesystem");
	    if (fsname == null || fsname.equals(""))
	    {
	      //otherwise try to get the filesystem from hadoop
	      fsname = conf.get("fs.default.name");
	    }
		

		log.info("rotateInterval is " + rotateInterval);
		log.info("outputDir is " + outputDir);
		log.info("fsname is " + fsname);
		log.info("filesystem type from core-default.xml is "
				+ conf.get("fs.hdfs.impl"));

		if (fsname == null) {
			log.error("no filesystem name");
			throw new WriterException("no filesystem");
		}	try {
			fs = FileSystem.get(new URI(fsname), conf);
			if (fs == null) {
				log.error("can't connect to HDFS at " + fs.getUri());
				return;
			} else
				log.info("filesystem is " + fs.getUri());
		} catch (IOException e) {
			log.error(
							"can't connect to HDFS, trying default file system instead (likely to be local)",
							e);
			try	{
				fs = FileSystem.get(conf);
			} catch (IOException err) {
				log.error("can't connect to default file system either", e);
			}
		} catch (URISyntaxException e) 	{
			log.error("problem generating new URI from config setting");
			return;
		}

		// Setup everything by rotating
		rotate();
		 
		clientAckTimer = new Timer();
		clientAckTimer.schedule(new TimerTask()
		{
			public void run() 
			{
				synchronized (lock) 
				{
					ClientAck previous = clientAck ;
					SeqFileWriter.clientAck = new ClientAck();
					
					try
					{
						// SeqFile is uncompressed for now
						// So we can flush every xx secs
						// But if we're using block Compression
						// this is not true anymore
						// because this will trigger
						// the compression
						if (currentOutputStr != null)
						{
							currentOutputStr.flush(); 
						}
						previous.releaseLock(ClientAck.OK, null);
						long now = System.currentTimeMillis();
						if (now >= nextRotate)
						{
							nextRotate = System.currentTimeMillis() + rotateInterval;
							rotate();
						}
					}
					catch(Throwable e)
					{
						previous.releaseLock(ClientAck.KO, e);
						log.warn("Exception when flushing ", e);
						e.printStackTrace();
					}	
				}
			}

		}, (5*1000), (5*1000));
		
		statTimer = new Timer();
		statTimer.schedule(new StatReportingTask(), 1000, STAT_INTERVAL_SECONDS * 1000);
		
		
		
	}

	private class StatReportingTask extends TimerTask
	{
		private long lastTs = System.currentTimeMillis();

		public void run()
		{
			
		  long time = System.currentTimeMillis();
			long currentDs = dataSize;
			dataSize = 0;
			
		  long interval = time - lastTs;
			lastTs = time;

			long dataRate = 1000 * currentDs / interval; // kb/sec
			log.info("stat:datacollection.writer.hdfs dataSize=" + currentDs + " dataRate=" + dataRate);
		}
	};

	
	void rotate()
	{
		calendar.setTimeInMillis(System.currentTimeMillis());

		log.info("start Date [" + calendar.getTime() + "]");
		log.info("Rotate from " + Thread.currentThread().getName());

		String newName = new java.text.SimpleDateFormat("yyyyddHHmmssSSS").format(calendar.getTime());
		newName += "_" + new java.rmi.server.UID().toString();
		newName = newName.replace("-", "");
		newName = newName.replace(":", "");
		newName = newName.replace(".", "");
		newName = outputDir + "/" + newName.trim();


		synchronized (lock) 
		{
			try
			{
				FSDataOutputStream previousOutputStr = currentOutputStr;
				Path previousPath = currentPath;
				String previousFileName = currentFileName;

				if (previousOutputStr != null) 	
				{
					previousOutputStr.close();
					fs.rename(previousPath,
							new Path(previousFileName + ".done"));
				}
				Path newOutputPath = new Path(newName + ".chukwa");			
				FSDataOutputStream newOutputStr = fs.create(newOutputPath);
				currentOutputStr = newOutputStr;
				currentPath = newOutputPath;
				currentFileName = newName;
				// Uncompressed for now
				seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
						ChukwaArchiveKey.class, ChunkImpl.class,
						SequenceFile.CompressionType.NONE, null);
			}
			catch (IOException e)
			{
				log.fatal("IO Exception in rotate. Exiting!");
				e.printStackTrace();
				// TODO  
				// As discussed for now:
				// Everytime this happen in the past it was because HDFS was down, 
				// so there's nothing we can do
				// Shutting down the collector for now
				// Watchdog will re-start it automatically
				System.exit(-1);
			}		
		}

		log.debug("finished rotate()");
	}

	// TODO merge the 2 add functions
	@Override
	public void add(List<Chunk> chunks) throws WriterException
	{
		if (chunks != null) 	
		{
			try 
			{
				ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();

				// FIXME compute this once an hour
				// 
				synchronized (calendar)
				{
					calendar.setTimeInMillis(System.currentTimeMillis());
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);

					archiveKey.setTimePartition(calendar.getTimeInMillis());
				}

				ClientAck localClientAck = null;					
				synchronized(lock)
				{
					localClientAck = SeqFileWriter.clientAck;
					for (Chunk chunk : chunks)
					{
						archiveKey.setDataType(chunk.getDataType());
						archiveKey.setStreamName(chunk.getTags() + "/" + chunk.getSource() + "/" + chunk.getStreamName());
						archiveKey.setSeqId(chunk.getSeqID());

						if (chunk != null) 	
						{
							seqFileWriter.append(archiveKey, chunk);
							// compute size for stats
							dataSize += chunk.getData().length;
						}

					}
				}// End synchro

				localClientAck.wait4Ack();
				if (localClientAck.getStatus() != ClientAck.OK)
				{
					log.warn("Exception after notyfyAll on the lock - Thread:" + Thread.currentThread().getName(),localClientAck.getException());
					throw new WriterException(localClientAck.getException());
				}
				else
				{
					// sucess
					writeChunkRetries = initWriteChunkRetries;
				}

			}
			catch (IOException e) 
			{
				writeChunkRetries --;
				log.error("Could not save the chunk. ", e);

				if (writeChunkRetries < 0)
				{
					log.fatal("Too many IOException when trying to write a chunk, Collector is going to exit!");
					System.exit(-1);
				}
				throw new WriterException(e);
			}
		}

	}
	
	public void add(Chunk chunk) throws WriterException
	{
	  
		if (chunk != null) 	{
			try {
				ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();

				// FIXME compute this once an hour
				synchronized (calendar)
				{
					calendar.setTimeInMillis(System.currentTimeMillis());
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);

					archiveKey.setTimePartition(calendar.getTimeInMillis());
				}

				archiveKey.setDataType(chunk.getDataType());
				archiveKey.setStreamName(chunk.getTags() + "/" + chunk.getSource() + "/" + chunk.getStreamName());
				archiveKey.setSeqId(chunk.getSeqID());

				ClientAck localClientAck = null;
				synchronized(lock)
				{
					localClientAck = SeqFileWriter.clientAck;
					log.info("[" + Thread.currentThread().getName() + "] Client >>>>>>>>>>>> Current Ack object ===>>>>" + localClientAck.toString());
					seqFileWriter.append(archiveKey, chunk);
					
					// compute size for stats
					dataSize += chunk.getData().length;
				}
				localClientAck.wait4Ack();
				
				if (localClientAck.getStatus() != ClientAck.OK)
				{
					log.warn("Exception after notyfyAll on the lock - Thread:" + Thread.currentThread().getName(),localClientAck.getException());
					throw new WriterException(localClientAck.getException());
				}	
				else
				{
					// sucess
					writeChunkRetries = initWriteChunkRetries;
				}
			} 
			catch (IOException e) 
			{
				writeChunkRetries --;
				log.error("Could not save the chunk. ", e);
	
				if (writeChunkRetries < 0)
				{
					log.fatal("Too many IOException when trying to write a chunk, Collector is going to exit!");
					System.exit(-1);
				}
				throw new WriterException(e);
			}
		}
	}

	public void close()
	{
		synchronized (lock)
		{
		  if (timer != null)
			  timer.cancel();
		  if (statTimer != null)
			  statTimer.cancel();
		  if (clientAckTimer != null)
			  clientAckTimer.cancel();
			try {
				
				if (this.currentOutputStr != null)
				{
					this.currentOutputStr.close();
				}
					
				clientAck.releaseLock(ClientAck.OK, null);
				fs.rename(currentPath, new Path(currentFileName + ".done"));
			} catch (IOException e) 
			{
				clientAck.releaseLock(ClientAck.OK, e);
				log.error("failed to close and rename stream", e);
			}
		}
	}

}
