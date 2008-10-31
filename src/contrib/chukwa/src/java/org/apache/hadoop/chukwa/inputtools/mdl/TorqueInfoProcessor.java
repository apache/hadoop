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

import java.sql.SQLException;
import java.sql.ResultSet;
import java.lang.Exception;
import java.util.Calendar;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.Iterator;
import java.lang.StringBuffer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.lang.Thread;
import java.util.Timer;
import java.lang.ProcessBuilder;
import java.lang.Process;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.InterruptedException;
import java.lang.System;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.inputtools.mdl.TorqueTimerTask;
import org.apache.hadoop.chukwa.inputtools.mdl.ErStreamHandler;
import org.apache.hadoop.chukwa.util.DatabaseWriter;


public class TorqueInfoProcessor {
    
	private static Log log = LogFactory.getLog(TorqueInfoProcessor.class);
    
    private int intervalValue=60;
	private String torqueServer = null;
	private String torqueBinDir= null;
	private String domain = null;

    private TreeMap <String,TreeMap<String,String>> currentHodJobs;
    
    
	public TorqueInfoProcessor(DataConfig mdlConfig, int interval){
		this.intervalValue=interval;
		
		torqueServer=System.getProperty("TORQUE_SERVER");
		torqueBinDir=System.getProperty("TORQUE_HOME")+File.separator+"bin";
		domain=System.getProperty("DOMAIN");
	    currentHodJobs=new TreeMap<String,TreeMap<String,String>>();
	}
	
	
	
	public void setup(boolean recover)throws Exception {
	 }
		 
	 private void  getHodJobInfo() throws IOException {
		 StringBuffer sb=new StringBuffer();
		 sb.append(torqueBinDir).append("/qstat -a");
	 
		 String[] getQueueInfoCommand=new String [3];
		 getQueueInfoCommand[0]="ssh";
		 getQueueInfoCommand[1]=torqueServer;
		 getQueueInfoCommand[2]=sb.toString();
		
		 
         String command=getQueueInfoCommand[0]+" "+getQueueInfoCommand[1]+" "+getQueueInfoCommand[2];
		 ProcessBuilder pb= new ProcessBuilder(getQueueInfoCommand);
         
		 Process p=pb.start();
		 
		 Timer timeout=new Timer();
		 TorqueTimerTask torqueTimerTask=new TorqueTimerTask(p, command);
		 timeout.schedule(torqueTimerTask, TorqueTimerTask.timeoutInterval*1000);

		 BufferedReader result = new BufferedReader (new InputStreamReader (p.getInputStream()));
		 ErStreamHandler errorHandler=new ErStreamHandler(p.getErrorStream(),command,true);
		 errorHandler.start();
		 
		 String line = null;
		 boolean start=false;
         TreeSet<String> jobsInTorque=new TreeSet<String>();
		 while((line=result.readLine())!=null){
			 if (line.startsWith("---")){				 
				 start=true;
				 continue;
			 }

			 if(start){
				 String [] items=line.split("\\s+");
				 if (items.length>=10){
				     String hodIdLong=items[0];				     
				     String hodId=hodIdLong.split("[.]")[0];
				     String userId=items[1];
				     String numOfMachine=items[5];
				     String status=items[9];
				     jobsInTorque.add(hodId);
                     if (!currentHodJobs.containsKey(hodId)) {
                         TreeMap <String,String> aJobData=new TreeMap <String,String>();
                     
				         aJobData.put("userId", userId);
				         aJobData.put("numOfMachine",numOfMachine);
				         aJobData.put("traceCheckCount","0");
                         aJobData.put("process", "0");
				         aJobData.put("status",status);
				         currentHodJobs.put(hodId,aJobData);
				     }else {
				    	 TreeMap <String,String> aJobData= currentHodJobs.get(hodId);
				    	 aJobData.put("status", status);
				         currentHodJobs.put(hodId,aJobData);
				     }//if..else
				 }				 
			 }
		 }//while
		 
         try {
        	 errorHandler.join();
         }catch (InterruptedException ie){
        	 log.error(ie.getMessage());
         }
		 timeout.cancel();
		 
		 Set<String> currentHodJobIds=currentHodJobs.keySet();
		 Iterator<String> currentHodJobIdsIt=currentHodJobIds.iterator();
		 TreeSet<String> finishedHodIds=new TreeSet<String>();
		 while (currentHodJobIdsIt.hasNext()){
			 String hodId=currentHodJobIdsIt.next();
			 if (!jobsInTorque.contains(hodId)) {
				TreeMap <String,String> aJobData=currentHodJobs.get(hodId);
				String process=aJobData.get("process");
				if (process.equals("0") || process.equals("1")) {	
					aJobData.put("status", "C");
				}else {
					finishedHodIds.add(hodId);
				}
			 }
		 }//while
		 
		 Iterator<String >finishedHodIdsIt=finishedHodIds.iterator();
		 while (finishedHodIdsIt.hasNext()){
			 String hodId=finishedHodIdsIt.next();
			 currentHodJobs.remove(hodId);
		 }
		  		 		 	 
	 }
	 
	 private boolean loadQstatData(String hodId) throws IOException, SQLException {
		 TreeMap<String,String> aJobData=currentHodJobs.get(hodId);
		 String userId=aJobData.get("userId");
		 
		 StringBuffer sb=new StringBuffer();
		 sb.append(torqueBinDir).append("/qstat -f -1 ").append(hodId);
		 String[] qstatCommand=new String [3];
		 qstatCommand[0]="ssh";
		 qstatCommand[1]=torqueServer;
		 qstatCommand[2]=sb.toString();
		 
         String command=qstatCommand[0]+" "+qstatCommand[1]+" "+qstatCommand[2];
		 ProcessBuilder pb= new ProcessBuilder(qstatCommand);         
		 Process p=pb.start();
		 
		 Timer timeout=new Timer();
		 TorqueTimerTask torqueTimerTask=new TorqueTimerTask(p, command);
		 timeout.schedule(torqueTimerTask, TorqueTimerTask.timeoutInterval*1000);

		 BufferedReader result = new BufferedReader (new InputStreamReader (p.getInputStream()));
		 ErStreamHandler errorHandler=new ErStreamHandler(p.getErrorStream(),command,false);
		 errorHandler.start();
		 String line=null;
         String hosts=null;
         long startTimeValue=-1;
         long endTimeValue=Calendar.getInstance().getTimeInMillis();
         long executeTimeValue=Calendar.getInstance().getTimeInMillis();
         boolean qstatfinished;
        
		 while((line=result.readLine())!=null){
			 if (line.indexOf("ctime")>=0){
				 String startTime=line.split("=")[1].trim();
				 //Tue Sep  9 23:44:29 2008
				 SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
				 Date startTimeDate;
				try {
					startTimeDate = sdf.parse(startTime);
					 startTimeValue=startTimeDate.getTime();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 
			 }
			 if (line.indexOf("mtime")>=0){
				 String endTime=line.split("=")[1].trim();
				 SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
				 Date endTimeDate;
				try {
					endTimeDate = sdf.parse(endTime);
					endTimeValue=endTimeDate.getTime();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 
			 }
			 if (line.indexOf("etime")>=0){
				 String executeTime=line.split("=")[1].trim();
				 SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
				 Date executeTimeDate;
				try {
					executeTimeDate = sdf.parse(executeTime);
					executeTimeValue=executeTimeDate.getTime();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 
			 }			 
			 if (line.indexOf("exec_host")>=0){
				 hosts=line.split("=")[1].trim();
			 }
		  }
		 
		  if (hosts!=null && startTimeValue>=0) {
			 String [] items2=hosts.split("[+]"); 
			 int num=0;
		     for (int i=0;i<items2.length;i++) {
		    	 String machinetmp=items2[i];
		    	 if( machinetmp.length()>3){
 			    	 String machine=items2[i].substring(0,items2[i].length()-2);
            	     StringBuffer data=new StringBuffer();
            	     data.append("HodId=").append(hodId);
            	     data.append(", Machine=").append(machine);
            	     if(domain!=null) {
            	    	 data.append(".").append(domain);
            	     }
            	     log.info(data);
            	     num++;   
			      }
		     } 	 
			 Timestamp startTimedb=new Timestamp(startTimeValue);
			 Timestamp endTimedb=new Timestamp(endTimeValue);
			 StringBuffer data=new StringBuffer();
			 long timeQueued=executeTimeValue-startTimeValue;
			 data.append("HodID=").append(hodId);
			 data.append(", UserId=").append(userId);		
			 data.append(", StartTime=").append(startTimedb);
			 data.append(", TimeQueued=").append(timeQueued);
			 data.append(", NumOfMachines=").append(num);
			 data.append(", EndTime=").append(endTimedb);
    	     //log.info(data);
			 qstatfinished=true;
			 
	      } else{
		   		   
             qstatfinished=false;
          }
		    
          try {
        	 errorHandler.join();
          }catch (InterruptedException ie){
        	 log.error(ie.getMessage());
          }
          result.close();
          timeout.cancel();
         	   
          return qstatfinished;
	 }
	
	 
	 private boolean loadTraceJobData(String hodId) throws IOException,SQLException{
		 TreeMap<String,String> aJobData=currentHodJobs.get(hodId);
		 //String queue=aJobData.get("queue");
		 String userId=aJobData.get("userId");
		 String process=aJobData.get("process");
		 //String numOfMachine=aJobData.get("numOfMachine");
		 
		 //StringBuffer traceJobsb=new StringBuffer();
		 StringBuffer sb=new StringBuffer();
		 sb.append(torqueBinDir).append("/tracejob -n 10 -l -m -s ").append(hodId);
	   	 //ProcessBuilder pb= new ProcessBuilder(getQueueInfoCommand.toString());
		 String[] traceJobCommand=new String [3];
		 traceJobCommand[0]="ssh";
		 traceJobCommand[1]=torqueServer;
		 traceJobCommand[2]=sb.toString();
		 
         String command=traceJobCommand[0]+" "+traceJobCommand[1]+" "+traceJobCommand[2];
		 //System.out.println(command);
		 ProcessBuilder pb= new ProcessBuilder(traceJobCommand);
         
         //String testCommand="/home/lyyang/work/chukwa/src/java/org/apache/hadoop/chukwa/ikit/sleeping";
         //ProcessBuilder pb= new ProcessBuilder(testCommand);
		 //pb.redirectErrorStream(false);

		 Process p=pb.start();
		 
		 Timer timeout=new Timer();
		 TorqueTimerTask torqueTimerTask=new TorqueTimerTask(p, command);
		 timeout.schedule(torqueTimerTask, TorqueTimerTask.timeoutInterval*1000);

		 BufferedReader result = new BufferedReader (new InputStreamReader (p.getInputStream()));
		 ErStreamHandler errorHandler=new ErStreamHandler(p.getErrorStream(),command,false);
		 errorHandler.start();
		 String line=null;
		 /*
		 BufferedReader error = new BufferedReader (new InputStreamReader(p.getErrorStream()));
		 String line = null;
		 boolean start=false;
         TreeSet<String> jobsInTorque=new TreeSet<String>();
         String errorLine = null;;
         while((errorLine=error.readLine())!=null) {
        	 //discard the error message;
        	 ;
         }
         */
         String exit_status=null;
         String hosts=null;
         long timeQueued=-1;
         long startTimeValue=-1;
         long endTimeValue=-1;
         boolean findResult=false;

        
		 while((line=result.readLine())!=null&& ! findResult){
			 if (line.indexOf("end")>=0 &&line.indexOf("Exit_status")>=0 && line.indexOf("qtime")>=0){
			      TreeMap <String,String> jobData=new TreeMap <String,String>() ;
			      String [] items=line.split("\\s+");
			      for (int i=0;i<items.length; i++) {
			    	 String [] items2 = items[i].split("=");
			    	 if (items2.length>=2){
			    		 jobData.put(items2[0], items2[1]);
			    	 }

			      }
	              String startTime=jobData.get("ctime");
			      startTimeValue=Long.valueOf(startTime);
			      startTimeValue=startTimeValue-startTimeValue%(60);
			      Timestamp startTimedb=new Timestamp(startTimeValue*1000);
			       
			      String queueTime=jobData.get("qtime");
			      long queueTimeValue=Long.valueOf(queueTime);
			      
			      String sTime=jobData.get("start");
			      long sTimeValue=Long.valueOf(sTime);
			      			      
			      timeQueued=sTimeValue-queueTimeValue;
			      
			      String endTime=jobData.get("end");
			      endTimeValue=Long.valueOf(endTime);
			      endTimeValue=endTimeValue-endTimeValue%(60);
			      Timestamp endTimedb=new Timestamp(endTimeValue*1000);
			      
			      exit_status=jobData.get("Exit_status");
			      //if (process.equals("0")){
			    	  hosts=jobData.get("exec_host");
			    	  String [] items2=hosts.split("[+]");
			    	  int num=0;
			    	  for (int i=0;i<items2.length;i++) {
			    		  String machinetemp=items2[i];
			    		  if (machinetemp.length()>=3){
	            		 
			    			  String machine=items2[i].substring(0,items2[i].length()-2);
			    			  StringBuffer data=new StringBuffer();
			    			  data.append("HodId=").append(hodId);
			    			  data.append(", Machine=").append(machine);
		            	      if(domain!=null) {
			            	   	 data.append(".").append(domain);
			            	  }
			    			  log.info(data.toString());
			    			  num++;
			    		  }  
			    	  }
			      
			    	  StringBuffer data=new StringBuffer();
			    	  data.append("HodID=").append(hodId);
			    	  data.append(", UserId=").append(userId);		
			    	  data.append(", Status=").append(exit_status);
			    	  data.append(", TimeQueued=").append(timeQueued);
			    	  data.append(", StartTime=").append(startTimedb);
			    	  data.append(", EndTime=").append(endTimedb);
			    	  data.append(", NumOfMachines=").append(num);
			          log.info(data.toString());
//			      } else{
//			    	  StringBuffer data=new StringBuffer();
//			    	  data.append("HodID=").append(hodId);
//			    	  data.append(", TimeQueued=").append(timeQueued);
//			    	  data.append(", EndTime=").append(endTimedb);
//			    	  data.append(", Status=").append(exit_status);
//			    	  log.info(data.toString());
//			      }
				  findResult=true;
		          log.debug(" hod info for job "+hodId+" has been loaded ");
			 }//if
			 
		}//while 

         try {
        	 errorHandler.join();
         }catch (InterruptedException ie){
        	 log.error(ie.getMessage());
         }
         
        timeout.cancel();
        boolean tracedone=false;
        if (!findResult){
        	
            String traceCheckCount=aJobData.get("traceCheckCount");
            int traceCheckCountValue=Integer.valueOf(traceCheckCount);
            traceCheckCountValue=traceCheckCountValue+1;           
            aJobData.put("traceCheckCount",String.valueOf(traceCheckCountValue));

            
            log.debug("did not find tracejob info for job "+hodId+", after "+traceCheckCountValue+" times checking");
            if (traceCheckCountValue>=2){ 
            	tracedone= true;
            	
//                StringBuffer deletesb1=new StringBuffer();
//                deletesb1.append(" Delete from ").append(hodJobTable);
//                deletesb1.append(" where hodid='").append(hodId).append("'");
//                String delete1=deletesb1.toString();
//                
////                dbWriter.execute(delete1);
//                
//                StringBuffer deletesb2=new StringBuffer();
//                deletesb2.append(" Delete from  ").append(hodMachineTable);
//                deletesb2.append(" where hodid='").append(hodId).append("'");
//                String delete2=deletesb2.toString();
////                dbWriter.execute(delete2);
            }
        }
        boolean finished=findResult|tracedone;
       
	   
        return finished;
      
    //  return true;   
	 }
	 
		 
		 
	 private void process_data() throws SQLException{
	
		 long currentTime=System.currentTimeMillis();
		 currentTime=currentTime-currentTime%(60*1000);
		 Timestamp timestamp=new Timestamp(currentTime);
		 
		 Set<String> hodIds=currentHodJobs.keySet();
		 
		 Iterator<String> hodIdsIt=hodIds.iterator();
		 while (hodIdsIt.hasNext()){
			 String hodId=(String) hodIdsIt.next();
			 TreeMap<String,String> aJobData=currentHodJobs.get(hodId);
			 //String queue=aJobData.get("queue");
			 //String numOfMachine=aJobData.get("numOfMachine");
			 String status=aJobData.get("status");
			 String process=aJobData.get("process");
			 if (process.equals("0") && (status.equals("R") ||status.equals("E"))){
			     try {
			    	 boolean result=loadQstatData(hodId);
			    	 if (result){
			    		 aJobData.put("process","1");
				    	 currentHodJobs.put(hodId, aJobData);			    		 
			    	 }
			     }catch (IOException ioe){
			    	 log.error("load qsat data Error:"+ioe.getMessage());
			    	  
			     }
			 }
			 if (! process.equals("2") && status.equals("C")){
				 try {
			    	boolean result=loadTraceJobData(hodId);
			    	
			    	if (result){
			    		aJobData.put("process","2");
			    		currentHodJobs.put(hodId, aJobData);
			    	}
				 }catch (IOException ioe){
					 log.error("loadTraceJobData Error:"+ioe.getMessage());
				 }
			 }//if
			
			 
		 } //while
		 
	 }
	 
	 
	 private void handle_jobData() throws SQLException{		 
		 try{
		     getHodJobInfo();
		 }catch (IOException ex){
			 log.error("getQueueInfo Error:"+ex.getMessage());
			 return;
		 }
		 try{    
	         process_data();
		 } catch (SQLException ex){
			 log.error("process_data Error:"+ex.getMessage());
			 throw ex;
		 }
	 }
     
	 
	 public void run_forever() throws SQLException{    	            
     	  while(true){
          	  handle_jobData();
              try {
                  log.debug("sleeping ...");
                  Thread.sleep(this.intervalValue*1000);
              } catch (InterruptedException e) {
                  log.error(e.getMessage()); 	
              }
          }
     }
     
	 
	 public void shutdown(){
     }
   	  
	

}
