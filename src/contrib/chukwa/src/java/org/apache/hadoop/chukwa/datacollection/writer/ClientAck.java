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

import org.apache.log4j.Logger;

public class ClientAck
{
	static Logger log = Logger.getLogger(ClientAck.class);
	
	// TODO move all constant to config
	
	public static final int OK = 100;
	public static final int KO = -100;
	public static final int KO_LOCK = -200;
	
	private long ts = 0;
	
	private Object lock = new Object();
	private int status = 0;
	private Throwable exception = null;
	private int waitTime = 6*1000;// 6 secs
	private int timeOut = 15*1000;
	
	public ClientAck()
	{
		this.ts = System.currentTimeMillis() + timeOut;
	}
	
  public int getTimeOut()
  {
    return timeOut;
  }

  public void wait4Ack()
	{
		synchronized(lock)
		{
//			log.info(">>>>>>>>>>>>>>>>>>>>>>>>> Client synch");
			while (this.status == 0)
			{
//				log.info(">>>>>>>>>>>>>>>>>>>>>>>>> Client Before wait");
				try { lock.wait(waitTime);}
				catch(InterruptedException e)
				{}
				long now = System.currentTimeMillis();
				if (now > ts)
				{
					this.status = KO_LOCK;
					this.exception = new RuntimeException("More than maximum time lock [" + this.toString() +"]");
				}
			}
//			log.info("[" + Thread.currentThread().getName() + "] >>>>>>>>>>>>>>>>> Client after wait status [" + status +  "] [" + this.toString() + "]");
		}
	}

	public void releaseLock(int status, Throwable exception)
	{
		this.exception = exception;
		this.status = status;
		
//		log.info("[" + Thread.currentThread().getName() + "] <<<<<<<<<<<<<<<<< Server synch [" + status + "] ----->>>> [" + this.toString() + "]");
		synchronized(lock)
		{		
//			log.info("<<<<<<<<<<<<<<< Server before notify");
			lock.notifyAll();
		}
//		log.info("<<<<<<<<<<<<<<< Server after notify");
	}
	
	public int getStatus()
	{
		return status;
	}

	public void setStatus(int status)
	{
		this.status = status;
	}

	public Throwable getException()
	{
		return exception;
	}

	public void setException(Throwable exception)
	{
		this.exception = exception;
	}
}
