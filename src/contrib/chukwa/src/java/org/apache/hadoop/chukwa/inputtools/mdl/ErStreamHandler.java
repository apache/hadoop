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
package org.apache.hadoop.chukwa.inputtools.mdl;

import java.lang.Thread;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.lang.StringBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ErStreamHandler extends Thread{
	InputStream inpStr;
	String command;
	boolean record;
	
    private static Log log = LogFactory.getLog(ErStreamHandler.class);	
    
	public ErStreamHandler(InputStream inpStr,String command,boolean record){
		this.inpStr=inpStr;
		this.command=command;
		this.record=record;

	}

	public void run(){
		try {
			InputStreamReader inpStrd=new InputStreamReader(inpStr);
			BufferedReader buffRd=new BufferedReader(inpStrd);
			String line=null;
			StringBuffer sb=new StringBuffer();
			while((line=buffRd.readLine())!= null){
                 sb.append(line);			
			}
			buffRd.close();
			
			if (record && sb.length()>0) {
				log.error(command+" execution error:"+sb.toString());				
			}
			
		}catch (Exception e){
			log.error(command+" error:"+e.getMessage());
		}
	}
	
	
}
