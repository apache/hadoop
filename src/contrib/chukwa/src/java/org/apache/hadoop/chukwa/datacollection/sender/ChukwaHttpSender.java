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

package org.apache.hadoop.chukwa.datacollection.sender;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.log4j.Logger;

/**
 * Encapsulates all of the http setup and connection details needed for
 * chunks to be delivered to a collector.
 * <p>
 * On error, tries the list of available collectors, pauses for a minute, and then repeats.
 * </p>
 * <p> Will wait forever for collectors to come up. </p>
 */
public class ChukwaHttpSender implements ChukwaSender{
  final int MAX_RETRIES_PER_COLLECTOR ; //fast retries, in http client
  final int SENDER_RETRIES; 
  final int WAIT_FOR_COLLECTOR_REBOOT; 
    //FIXME: this should really correspond to the timer in RetryListOfCollectors
  
  static Logger log = Logger.getLogger(ChukwaHttpSender.class);
  static HttpClient client = null;
  static MultiThreadedHttpConnectionManager connectionManager = null;
  static String currCollector = null;

  
  protected Iterator<String> collectors;
  
  static
  {
    connectionManager = 
          new MultiThreadedHttpConnectionManager();
    client = new HttpClient(connectionManager);
    connectionManager.closeIdleConnections(1000);
  }
  
  public static class CommitListEntry {
    public Adaptor adaptor;
    public long uuid;
    
    public CommitListEntry(Adaptor a, long uuid)  {
      adaptor = a;
      this.uuid = uuid;
    }
  }
  
//FIXME: probably we're better off with an EventListRequestEntity
  static class BuffersRequestEntity implements RequestEntity {
    List<DataOutputBuffer> buffers;
    
    public BuffersRequestEntity(List<DataOutputBuffer> buf) {
      buffers=buf;
    }

    public long getContentLength()  {
      long len=4;//first we send post length, then buffers
      for(DataOutputBuffer b: buffers)
        len += b.getLength();
      return len;
    }

    public String getContentType() {
      return "application/octet-stream";
    }

    public boolean isRepeatable()  {
      return true;
    }

    public void writeRequest(OutputStream out) throws IOException  {
      DataOutputStream dos = new DataOutputStream(out);
      dos.writeInt(buffers.size());
      for(DataOutputBuffer b: buffers)
        dos.write(b.getData(), 0, b.getLength());
    }
  }

  public ChukwaHttpSender(Configuration c){
    //setup default collector
    ArrayList<String> tmp = new ArrayList<String>();
    this.collectors = tmp.iterator();
    log.info("added a single collector to collector list in ConnectorClient constructor, it's hasNext is now: " + collectors.hasNext());

    MAX_RETRIES_PER_COLLECTOR = c.getInt("chukwaAgent.sender.fastRetries", 4);
    SENDER_RETRIES= c.getInt("chukwaAgent.sender.retries", 144000);
    WAIT_FOR_COLLECTOR_REBOOT= c.getInt("chukwaAgent.sender.retryInterval", 20*1000);
  }
  
  /**
   * Set up a single connector for this client to send {@link Chunk}s to
   * @param collector the url of the collector
   */
  public void setCollectors(String collector){
   }
  
  /**
   * Set up a list of connectors for this client to send {@link Chunk}s to
   * @param collectors
   */
  public void setCollectors(Iterator<String> collectors){
    this.collectors = collectors; 
    //setup a new destination from our list of collectors if one hasn't been set up
    if (currCollector == null){
      if (collectors.hasNext()){
        currCollector = collectors.next();
      }
      else
        log.error("No collectors to try in send(), not even trying to do doPost()");
    }
  }
  
  
  /**
   * grab all of the chunks currently in the chunkQueue, stores a copy of them locally, calculates
   * their size, sets them up 
   * @return array of chunk id's which were ACKed by collector
   */
  public List<CommitListEntry> send(List<Chunk> toSend) throws InterruptedException, IOException{
    List<DataOutputBuffer> serializedEvents = new ArrayList<DataOutputBuffer>();
    List<CommitListEntry> commitResults = new ArrayList<CommitListEntry>();
    
    log.info("collected " + toSend.size() + " chunks");

    //Serialize each chunk in turn into it's own DataOutputBuffer and add that buffer to serializedEvents  
    for(Chunk c: toSend) {
      DataOutputBuffer b = new DataOutputBuffer(c.getSerializedSizeEstimate());
      try {
        c.write(b);
      }catch(IOException err) {
        log.error("serialization threw IOException", err);
      }
      serializedEvents.add(b);
      //store a CLE for this chunk which we will use to ack this chunk to the caller of send()
      //(e.g. the agent will use the list of CLE's for checkpointing)
      commitResults.add(new CommitListEntry(c.getInitiator(), c.getSeqID()));
    }
    toSend.clear();
    
    //collect all serialized chunks into a single buffer to send
    RequestEntity postData = new BuffersRequestEntity(serializedEvents);


    int retries = SENDER_RETRIES; 
    while(currCollector != null)
    {
      //need to pick a destination here
      PostMethod method = new PostMethod();
      try   {
        doPost(method, postData, currCollector);

        retries = SENDER_RETRIES; //reset count on success
        //if no exception was thrown from doPost, ACK that these chunks were sent
        return commitResults;
      } catch (Throwable e) {
        log.error("Http post exception", e);
        log.info("Checking list of collectors to see if another collector has been specified for rollover");
        if (collectors.hasNext()){
          currCollector = collectors.next();
          log.info("Found a new collector to roll over to, retrying HTTP Post to collector " + currCollector);
        } else {
          if(retries > 0) {
            log.warn("No more collectors to try rolling over to; waiting " + WAIT_FOR_COLLECTOR_REBOOT +
                " ms (" + retries + "retries left)");
            Thread.sleep(WAIT_FOR_COLLECTOR_REBOOT);
            retries --;
          } else {
            log.error("No more collectors to try rolling over to; aborting");
            throw new IOException("no collectors");
          }
        }
      }
      finally  {
        // be sure the connection is released back to the connection manager
        method.releaseConnection();
      }
    } //end retry loop
    return new ArrayList<CommitListEntry>();
  }
  
  /**
   * Handles the HTTP post. Throws HttpException on failure
   */
  @SuppressWarnings("deprecation")
  private void doPost(PostMethod method, RequestEntity data, String dest)
      throws IOException, HttpException
  {
    
    HttpMethodParams pars = method.getParams();
    pars.setParameter (HttpMethodParams.RETRY_HANDLER, (Object) new HttpMethodRetryHandler()
    {
      public boolean retryMethod(HttpMethod m, IOException e, int exec)
      {
        return !(e instanceof java.net.ConnectException) && (exec < MAX_RETRIES_PER_COLLECTOR);
      }
    });
    
    pars.setParameter(HttpMethodParams.SO_TIMEOUT , new Integer(30000));
    
    
    
    method.setParams(pars);
    method.setPath(dest);
    
     //send it across the network
    method.setRequestEntity(data);
    
    log.info(">>>>>> HTTP post to " + dest+" length = "+ data.getContentLength());
    // Send POST request
    
    //client.setTimeout(15*1000);
    int statusCode = client.executeMethod(method);
      
    if (statusCode != HttpStatus.SC_OK)  {
      log.error(">>>>>> HTTP post response statusCode: " +statusCode + ", statusLine: " + method.getStatusLine());
      //do something aggressive here
      throw new HttpException("got back a failure from server");
    }
    //implicitly "else"
    log.info(">>>>>> HTTP Got success back from the remote collector; response length "+ method.getResponseContentLength());

      //FIXME: should parse acks here
    InputStream rstream = null;
    
    // Get the response body
    byte[] resp_buf = method.getResponseBody();
    rstream = new ByteArrayInputStream(resp_buf); 
    BufferedReader br = new BufferedReader(new InputStreamReader(rstream));
    String line;
    while ((line = br.readLine()) != null) {
      System.out.println("response: " + line);
    }
  }
  
  public static void main(String[] argv) throws InterruptedException{
    //HttpConnectorClient cc = new HttpConnectorClient();
    //do something smarter than to hide record headaches, like force them to create and add records to a chunk
    //cc.addChunk("test-source", "test-streamName", "test-application", "test-dataType", new byte[]{1,2,3,4,5});
  }
}
