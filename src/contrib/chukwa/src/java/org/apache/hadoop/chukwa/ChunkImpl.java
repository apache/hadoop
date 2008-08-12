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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;

public class ChunkImpl implements org.apache.hadoop.io.Writable, Chunk 
{
  
  private String source = "";
  private String application = "";
  private String dataType = "";
  private byte[] data = null;
  private int[] recordEndOffsets;
  
  private String debuggingInfo="";
  
  private transient Adaptor initiator;
  long seqID;
  
  ChunkImpl() {
  }
  
  public static ChunkImpl getBlankChunk() {
    return new ChunkImpl();
  }
  
  public ChunkImpl(String dataType, String streamName, long seq, byte[] data, Adaptor source) {
    this.seqID = seq;
    this.application = streamName;
    this.dataType = dataType;
    this.data = data;
    this.initiator = source;
    this.source = localHostAddr;
  }
  
  /**
   *  @see org.apache.hadoop.chukwa.Chunk#getData()
   */
  public byte[] getData()	{
  	return data;
  }
  
  /**
   *  @see org.apache.hadoop.chukwa.Chunk#setData(byte[])
   */
  public void setData(byte[] logEvent) {
  	this.data = logEvent;
  }
  
  /**
   * @see org.apache.hadoop.chukwa.Chunk#getStreamName()
   */
  public String getStreamName() {
  	return application;
  }
  
  public void setStreamName(String logApplication)	{
  	this.application = logApplication;
  }
   
  public String getSource() {
    return source;
  }
  
  public void setSource(String logSource)	{
  	this.source = logSource;
  }
  
  public String getDebugInfo() {
  	return debuggingInfo;
  }
  
  public void setDebugInfo(String a) {
  	this.debuggingInfo = a;
  }
  
  /**
   * @see org.apache.hadoop.chukwa.Chunk#getSeqID()
   */
  public long getSeqID()  {
    return seqID;
  }
  
  public void setSeqID(long l) {
    seqID=l;
  }
  
  public String getApplication(){
    return application;
  }
  
  public void setApplication(String a){
    application = a;
  }
  
  public Adaptor getInitiator() {
    return initiator;
  }
  
  public void setInitiator(Adaptor a) {
    initiator = a;
  }
  
  
  public void setLogSource() {
    source = localHostAddr;
  }
  
  public int[] getRecordOffsets() {

    if(recordEndOffsets == null)
      recordEndOffsets = new int[] {data.length -1};
    return recordEndOffsets;
  }
  
  public void setRecordOffsets(int[] offsets) {
    recordEndOffsets = offsets;
  }
  
  public String getDataType() {
    return dataType;
  }
  
  public void setDataType(String t) {
    dataType = t;
  }
  
  /**
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    setSeqID(in.readLong());
    setSource(in.readUTF());
    setApplication(in.readUTF());
    setDataType(in.readUTF());
    setDebugInfo(in.readUTF());
    
    int numRecords = in.readInt();
    recordEndOffsets = new int[numRecords];
    for(int i=0; i < numRecords; ++i)
      recordEndOffsets[i] = in.readInt();
    data = new byte[recordEndOffsets[recordEndOffsets.length -1]+1 ] ;
    in.readFully(data);
    
  }
  /**
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(seqID);
    out.writeUTF(source);
    out.writeUTF(application);
    out.writeUTF(dataType);
    out.writeUTF(debuggingInfo);
    
    if(recordEndOffsets == null)
      recordEndOffsets = new int[] {data.length -1};
      
    out.writeInt(recordEndOffsets.length);
    for(int i =0; i < recordEndOffsets.length; ++i)
      out.writeInt(recordEndOffsets[i]);
    
    out.write(data, 0, recordEndOffsets[recordEndOffsets.length -1] + 1); //byte at last offset is valid
  }
  
  public static ChunkImpl read(DataInput in) throws IOException {
    ChunkImpl w = new ChunkImpl();
    w.readFields(in);
    return w;
  }
  
    //FIXME: should do something better here, but this is OK for debugging
  public String toString() {
    return source+":" + application +":"+ new String(data)+ "/"+seqID;
  }
  
  private static String localHostAddr;
  static
  {
    try {
      localHostAddr = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      localHostAddr = "localhost";
    }
  }
  
  /**
   * @see org.apache.hadoop.chukwa.Chunk#getSerializedSizeEstimate()
   */
  public int getSerializedSizeEstimate() {
    int size= 2 * (source.length() + application.length() + 
        dataType.length() + debuggingInfo.length()); //length of strings (pessimistic)
    size += data.length + 4;
    if(recordEndOffsets == null)
      size+=8;
    else
      size += 4 * (recordEndOffsets.length + 1); //+1 for length of array
    size += 8; //uuid
    return size;
  }

  public void setRecordOffsets(java.util.Collection<Integer> carriageReturns)
  {
    recordEndOffsets = new int [carriageReturns.size()];
    int i = 0;
    for(Integer offset:carriageReturns )
      recordEndOffsets[i++] = offset;
  }
	
}
