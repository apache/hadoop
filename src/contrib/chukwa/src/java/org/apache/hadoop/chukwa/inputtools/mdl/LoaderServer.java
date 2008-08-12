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

import java.io.IOException;
import java.io.File;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.channels.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class LoaderServer {
	
	String name;
	private static Log log = LogFactory.getLog(LoaderServer.class);
	private static FileLock lock = null;
        private static FileOutputStream pidFileOutput = null;
	
	public LoaderServer(String name){
		this.name=name;
	}
	
	public void init() throws IOException{
  	     String pidLong=ManagementFactory.getRuntimeMXBean().getName();
  	     String[] items=pidLong.split("@");
  	     String pid=items[0];
	     String chukwaPath=System.getProperty("CHUKWA_HOME");
	     StringBuffer pidFilesb=new StringBuffer();
	     pidFilesb.append(chukwaPath).append("/var/run/").append(name).append(".pid");
	     try{
	         File pidFile= new File(pidFilesb.toString());

	         pidFileOutput= new FileOutputStream(pidFile);
             pidFileOutput.write(pid.getBytes());
	         pidFileOutput.flush();
	         FileChannel channel = pidFileOutput.getChannel();
	         LoaderServer.lock = channel.tryLock();
             if(LoaderServer.lock!=null) {
	             log.info("Initlization succeeded...");
             } else {
                 throw(new IOException());
             }
	     }catch (IOException ex){
	    	 System.out.println("Initializaiton failed: can not write pid file.");
	    	 log.error("Initialization failed...");
	    	 log.error(ex.getMessage());
	    	 System.exit(-1);
	    	 throw ex;
	    	 
	     }
	   
	}	
	
	public void clean(){
        String chukwaPath=System.getenv("CHUKWA_HOME");
        StringBuffer pidFilesb=new StringBuffer();
        pidFilesb.append(chukwaPath).append("/var/run/").append(name).append(".pid"); 
        String pidFileName=pidFilesb.toString();

        File pidFile=new File(pidFileName);
        if (!pidFile.exists()) {
    	   log.error("Delete pid file, No such file or directory: "+pidFileName);
        } else {
           try {
               lock.release();
	       pidFileOutput.close();
           } catch(IOException e) {
               log.error("Unable to release file lock: "+pidFileName);
           }
        }

        boolean result=pidFile.delete();
        if (!result){
    	   log.error("Delete pid file failed, "+pidFileName);
        }
	}

}
