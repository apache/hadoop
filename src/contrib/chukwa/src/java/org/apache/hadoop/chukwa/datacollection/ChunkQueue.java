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

package org.apache.hadoop.chukwa.datacollection;

import java.util.List;

import org.apache.hadoop.chukwa.Chunk;

/**
 * A generic interface for queues of Chunks.
 * 
 * Differs from a normal queue interface primarily by having collect().
 */
public interface ChunkQueue extends ChunkReceiver
{
  /**
   *  Add a chunk to the queue, blocking if queue is full.
   * @param event
   * @throws InterruptedException if thread is interrupted while blocking
   */
	public void add(Chunk event) throws InterruptedException;
	
	/**
	 * Return at least one, and no more than count, Chunks into events.
	 * Blocks if queue is empty.
	 */
	public void collect(List<Chunk> events,int count) throws InterruptedException;
	
	/**
	 * Return an approximation of the number of chunks in the queue currently.
	 * No guarantees are made about the accuracy of this number. 
	 */
	public int size();
}
