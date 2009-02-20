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
	static Logger log = Logger.getLogger(SeqFileWriter.class);
	public static final boolean ENABLE_ROTATION = true;

	private FileSystem fs = null;
	private ChukwaConfiguration conf = null;

	private String outputDir = null;
	private Calendar calendar = Calendar.getInstance();

	private Path currentPath = null;
	private String currentFileName = null;
	private FSDataOutputStream currentOutputStr = null;
	private static SequenceFile.Writer seqFileWriter = null;

	private Timer timer = null;

	private Timer statTimer = null;
	private volatile long dataSize = 0;

	public SeqFileWriter() throws IOException
	{
		conf = new ChukwaConfiguration(true);
		init();
	}

	public void init() throws IOException
	{
		outputDir = conf.get("chukwaCollector.outputDir", "/chukwa");

		int rotateInterval = conf.getInt("chukwaCollector.rotateInterval",
				1000 * 60 * 5);//defaults to 5 minutes
		//check if they've told us the file system to use
    String fsname = conf.get("writer.hdfs.filesystem");
    if (fsname == null || fsname.equals("")){
      //otherwise try to get the filesystem from hadoop
      fsname = conf.get("fs.default.name");
    }
		

		log.info("rotateInterval is " + rotateInterval);
		log.info("ENABLE_ROTATION is " + ENABLE_ROTATION);
		log.info("outputDir is " + outputDir);
		log.info("fsname is " + fsname);
		log.info("filesystem type from hadoop-default.xml is "
				+ conf.get("fs.hdfs.impl"));

		if (fsname == null)
		{
			log.error("no filesystem name");
			throw new IOException("no filesystem");
		}
		try
		{
			fs = FileSystem.get(new URI(fsname), conf);
			if (fs == null)
			{
				log.error("can't connect to HDFS at " + fs.getUri());
				return;
			} else
				log.info("filesystem is " + fs.getUri());
		} catch (IOException e)
		{
			log.error(
							"can't connect to HDFS, trying default file system instead (likely to be local)",
							e);
			try
			{
				fs = FileSystem.get(conf);
			} catch (IOException err)
			{
				log.error("can't connect to default file system either", e);
			}
		} catch (URISyntaxException e)
		{
			log.error("problem generating new URI from config setting");
			return;
		}

		calendar.setTimeInMillis(System.currentTimeMillis());
		int minutes = calendar.get(Calendar.MINUTE);
		// number of minutes at current time

		int dec = minutes / 10; // 'tens' digit of current time

		int m = minutes - (dec * 10); // 'units' digit
		if (m < 5)
		{
			m = 5 - m;
		} else
		{
			m = 10 - m;
		}

		log.info("Current date [" + calendar.getTime().toString()
				+ "] next schedule [" + m + "]");
		rotate();

		timer = new Timer();

		if (ENABLE_ROTATION)
		{
			log.info("sink rotation enabled, rotating every " + rotateInterval
					+ " millis");
			timer.schedule(new TimerTask()
			{
				public void run()
				{
					rotate();
				}

			}, Math.min(rotateInterval, m * 60 * 1000), rotateInterval);

			statTimer = new Timer();
		} else
			log.warn("sink rotation is OFF!!");

		statTimer.schedule(new StatReportingTask(), 1000, 60 * 1000);
	}

	private class StatReportingTask extends TimerTask
	{
		private long lastTs = System.currentTimeMillis();
		private long lastDataSize = 0;

		public void run()
		{
			long time = System.currentTimeMillis();
			long interval = time - lastTs;
			lastTs = time;

			long ds = dataSize;
			long dataRate = 1000 * (ds - lastDataSize) / interval; // kb/sec
			lastDataSize = ds;

			log.info("stat=datacollection.writer.hdfs|dataSize=" + dataSize);
			log.info("stat=datacollection.writer.hdfs|dataRate=" + dataRate);
		}
	};

	void rotate()
	{
		calendar.setTimeInMillis(System.currentTimeMillis());

		log.info("start Date [" + calendar.getTime() + "]");
		//granularity of rollover directory structure is hourly
		String newDir = new java.text.SimpleDateFormat("yyyy_dd_HH")
				.format(calendar.getTime());

		log.info("Rotate from " + Thread.currentThread().getName());

		Path newDirPath = new Path(outputDir + "/" + newDir);
		log.info("Rotate directory[" + newDirPath.toString() + "]");
		try
		{
			if (!fs.exists(newDirPath))
			{
				log.info("Create new directory:" + newDirPath.toString());
				try
				{
					fs.mkdirs(newDirPath);
				} catch (Exception e)
				{
					if (!fs.exists(newDirPath))
					{
						log.info("Failed to create new directory:"
								+ newDirPath.toString() + "] ", e);
					}
				}
			} else // use the existing directory, because we haven't hit a new hour yet
			{
				log.info("Rotate from [" + Thread.currentThread().getName()
						+ "] directory (" + newDirPath + ") already exists.");

			}
	    String newName = new java.text.SimpleDateFormat("yyyy_dd_HH_mm_ss_SSS").format(calendar.getTime());
	    newName += "_" + new java.rmi.server.UID().toString();
	    newName = newName.replace("-", "");
	    newName = newName.replace(":", "");
	    newName = newName.replace(".", "");

			newName = newDirPath + "/" + newName.trim();

			Path newOutputPath = new Path(newName + ".chukwa");

			FSDataOutputStream newOutputStr = fs.create(newOutputPath);
			FSDataOutputStream previousOutputStr = null;
			Path previousPath = null;
			String previousFileName = null;

			synchronized (this)
			{
				previousOutputStr = currentOutputStr;
				previousPath = currentPath;
				previousFileName = currentFileName;

				currentOutputStr = newOutputStr;
				currentPath = newOutputPath;
				currentFileName = newName;
				if (previousOutputStr != null)
				{
					previousOutputStr.close();
					fs.rename(previousPath,
							new Path(previousFileName + ".done"));
				}

				// Turn compression ON if the 5 mins archives are big
				seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
						ChukwaArchiveKey.class, ChunkImpl.class,
						SequenceFile.CompressionType.NONE, null);
			}
		} catch (IOException e)
		{
			log.error("failed to do rotate", e);
		}
		log.debug("finished rotate()");
	}

	public synchronized void add(Chunk chunk) throws IOException
	{
		if (chunk != null)
		{
			try
			{
				assert chunk instanceof ChunkImpl : "bad input type";
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
				archiveKey.setStreamName(chunk.getStreamName());
				archiveKey.setSeqId(chunk.getSeqID());

				seqFileWriter.append(archiveKey, chunk);

				dataSize += chunk.getData().length;
				// currentOutput.sync(); //force file out to stable storage on
				// the cluster.
				// note that seqFileWriter.sync() would do something completely
				// different
			} catch (IOException e)
			{
				log.error(e.getMessage());
				rotate();
				throw e;
			}
		}
	}

	public void close()
	{
		synchronized (this)
		{
			try
			{
				this.currentOutputStr.close();
				fs.rename(currentPath, new Path(currentFileName + ".done"));
			} catch (IOException e)
			{
				log.error("failed to close and rename stream", e);
			}
		}
	}

	/*
	 * public static class SeqFileKey implements
	 * org.apache.hadoop.io.WritableComparable<SeqFileKey>{
	 * 
	 * public long seqID; public String streamName; public long
	 * collectorTimestamp;
	 * 
	 * public SeqFileKey() {} // for use in deserializing
	 * 
	 * SeqFileKey(Chunk event) { seqID = event.getSeqID(); streamName =
	 * event.getStreamName() + "_" + event.getSource(); collectorTimestamp =
	 * System.currentTimeMillis(); }
	 * 
	 * public void readFields(DataInput in) throws IOException { seqID =
	 * in.readLong(); streamName = in.readUTF(); collectorTimestamp =
	 * in.readLong(); }
	 * 
	 * public void write(DataOutput out) throws IOException {
	 * out.writeLong(seqID); out.writeUTF(streamName);
	 * out.writeLong(collectorTimestamp); }
	 * 
	 * public int compareTo(SeqFileKey o) { int cmp =
	 * streamName.compareTo(o.streamName); if(cmp == 0) { if(seqID < o.seqID)
	 * return -1; else if (seqID == o.seqID) return 0; else return 1; } else
	 * return cmp; }
	 * 
	 * public boolean equals(Object o) { return (o instanceof SeqFileKey) &&
	 * (compareTo((SeqFileKey) o) == 0); }
	 * 
	 * public int hashCode() { return streamName.hashCode() ^ (int)(seqID >> 32) ^
	 * (int) seqID; }
	 *  }
	 */
}
