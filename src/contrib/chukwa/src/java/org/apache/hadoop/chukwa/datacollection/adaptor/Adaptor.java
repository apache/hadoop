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
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;

/**
 * An adaptor is a component that runs within the Local Agent, producing 
 * chunks of monitoring data.
 * 
 * An adaptor can, but need not, have an associated thread. If an adaptor
 * lacks a thread, it needs to arrange some mechanism to periodically get control
 * and send reports such as a callback somewhere.
 * 
 * Adaptors must be able to stop and resume without losing data, using
 * a byte offset in the stream.
 * 
 * If an adaptor crashes at byte offset n, and is restarted at byte offset k,
 * with k < n, it is allowed to send different values for bytes k through n the 
 * second time around.  However, the stream must still be parseable, assuming that
 * bytes 0-k come from the first run,and bytes k - n come from the second.
 */
public interface Adaptor
{
  /**
   * Start this adaptor
   * @param type the application type, who is starting this adaptor
   * @param status the status string to use for configuration.
   * @param offset the stream offset of the first byte sent by this adaptor
   * @throws AdaptorException
   */
	public void start(String type, String status, long offset, ChunkReceiver dest) throws AdaptorException;
	
	/**
	 * Return the adaptor's state
	 * Should not include class name, datatype or byte offset, which are written by caller.
	 * @return the adaptor state as a string
	 * @throws AdaptorException
	 */
	public String getCurrentStatus() throws AdaptorException;
	public String getType();
	/**
	 * Signals this adaptor to come to an orderly stop.
	 * The adaptor ought to push out all the data it can
	 * before exiting.
	 * 
	 * This method is synchronous:
	 * In other words, after shutdown() returns, no new data should be written.
	 * 
	 * @return the logical offset at which the adaptor stops
	 * @throws AdaptorException
	 */
	public long shutdown() throws AdaptorException;
	
	/**
	 * Signals this adaptor to come to an abrupt stop, as quickly as it can.
	 * The use case here is "Whups, I didn't mean to start that adaptor tailing
	 * a gigabyte file, stop it now".
	 * 
	 * Adaptors might need to do something nontrivial here, e.g., in the case in which  
	 * they have registered periodic timer interrupts, or use a shared worker thread
	 * from which they need to disengage.
	 * 
	 * This method is synchronous:
   * In other words, after shutdown() returns, no new data should be written.
   *
	 * @throws AdaptorException
	 */
	public void hardStop() throws AdaptorException;

}