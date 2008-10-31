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
import java.lang.management.ManagementFactory;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.io.IOException;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.chukwa.inputtools.mdl.TorqueInfoProcessor;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.util.PidFile;

public class TorqueDataLoader {
	   private static Log log = LogFactory.getLog("TorqueDataLoader");

	    private TorqueInfoProcessor tp=null;
        private PidFile loader=null;
	  
	    
	    public TorqueDataLoader (DataConfig mdlConfig, int interval){
	    	log.info("in torqueDataLoader");
	   	    tp = new TorqueInfoProcessor(mdlConfig, interval);
	   	    loader=new PidFile("TorqueDataLoader");
	    }
	    
	    	    	    	    
	    public void run(){
 	        boolean first=true;
	   	    while(true){
	       	   try{
	   	           tp.setup(first);
	   	           first=false;
	   	        }catch (Exception ex){
	   	    	   tp.shutdown();
	   	           
	   	    	   if (first){
	   	    	      log.error("setup error");
	   	    	      ex.printStackTrace();
	   	    	      loader.clean(); // only call before system.exit()
	   	    	      System.exit(1);
	   	            }
	   	           log.error("setup fail, retry after 10 minutes");
	   	           try {
	                     Thread.sleep(600*1000);
	               } catch (InterruptedException e) {
	                // TODO Auto-generated catch block
	                	 log.error(e.getMessage());
	               // e.printStackTrace();
	               }
	   		       continue;   		 
	   		 
	   	       }
	   	     
	   	       try{
	   		        tp.run_forever();
	   	       }catch (SQLException ex) {
	   		        tp.shutdown();
	   	    	    log.error("processor died, reconnect again after 10 minutes");
	   	    	    ex.printStackTrace();
	   		        try {
	                     Thread.sleep(600*1000);
	                } catch (InterruptedException e) {
	                     // TODO Auto-generated catch block
	                	    log.error(e.getMessage());
	                     // e.printStackTrace();
	                }
	   	       }catch (Exception ex){
	   		        try {
	                     Thread.sleep(16*1000);
	                } catch (InterruptedException e) {
	                            ;
	                }
	   	    	   tp.shutdown();
	   	    	   log.error("process died...."+ex.getMessage());
	   	    	   loader.clean();
	   	    	   System.exit(1);
	   	       }
	   	       
	   	  }//while
	   
	    }
	    
	    
		 public static void main(String[] args) {
			   /* if (args.length < 2 || args[0].startsWith("-h")
			        || args[0].startsWith("--h")) {
			      System.out.println("Usage: UtilDataLoader interval(sec)");
			      System.exit(1);puvw-./chij
			    }
			    String interval = args[0];
			    int intervalValue=Integer.parseInt(interval);
			    */
			   int intervalValue=60;


	           DataConfig mdlConfig=new DataConfig();
	           
	           TorqueDataLoader tdl = new TorqueDataLoader(mdlConfig, intervalValue);
	           tdl.run();

		 }        
		
}
