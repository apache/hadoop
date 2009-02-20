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

package org.apache.hadoop.chukwa;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.chukwa.datacollection.adaptor.*;

/**
 * A chunk is a sequence of bytes at a particular logical offset in a stream,
 * and containing one or more "records".
 *  Chunks have various metadata, such as source, format,
 * and pointers to record boundaries within the chunk.
 * 
 */
public interface Chunk {
	
//these conceptually are really network addresses
	public String getSource();
	public void setSource(String logSource);
	
	/**
	 * Get the name of the stream that this Chunk is a chunk of
	 * @return the name of this stream; e.g. file name
	 */
	public String getStreamName();
	public void setStreamName(String streamName);
	
	public String getApplication();  
  public void setApplication(String a);
	
  //These describe the format of the data buffer
  public String getDataType();
  public void setDataType(String t);

  /**
   * @return the user data in the chunk
   */
	public byte[] getData();
	/**
	 * @param logEvent the user data in the chunk
	 */
	public void setData(byte[] logEvent);
	
	/**
	 * get/set the <b>end</b> offsets of records in the buffer.
	 * 
	 * We use end, rather than start offsets, since the first start
	 * offset is always 0, but the last end offset specifies how much of the buffer is valid.
	 * 
	 * More precisely, offsets[i] is the offset in the Chunk of the last byte of record i
	 *  in this chunk.
	 * @return a list of record end offsets
	 */
	public int[] getRecordOffsets();
	public void setRecordOffsets(int[] offsets);
	
	/**
	 * @return  the byte offset of the first byte not in this chunk.
	 * 
	 * We pick this convention so that subtracting sequence IDs yields length.
	 */
	public long getSeqID();
	public void setSeqID(long l);

	/**
	 * Retrieve a reference to the adaptor that sent this event.
	 * Used by LocalAgent and Connectors to deliver acks to the appropriate place.
	 */
	public Adaptor getInitiator();
	
  /**
   * Estimate the size of this Chunk on the wire, assuming each char of metadata takes two bytes
   * to serialize.  This is pessimistic.
   * @return size in bytes that this Chunk might take once serialized.
   */
  public int getSerializedSizeEstimate();
  
  public void write(DataOutput data) throws IOException;
}
