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

package org.apache.hadoop.chukwa.datacollection.connector;

/**
 * This class is responsible for setting up a long living process that repeatedly calls the 
 * <code>send</code> function of a Sender.
 */

public interface Connector
{
	static final int proxyTimestampField = 0;
	/**
	 * 
	 */
	static final int proxyURIField = 1;
	static final int proxyRetryField = 2;
	
	static final int adaptorTimestampField = 3;
	static final int adaptorURIField = 4;

	static final int logTimestampField = 5;
	static final int logSourceField = 6;
	static final int logApplicationField = 7;
	static final int logEventField = 8;

	
	public void start();
  public void shutdown();
}
