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

package org.apache.hadoop.chukwa.datacollection.controller;


import java.net.*;
import java.io.*;
import java.util.*;

import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;

/**
 * A convenience library for applications to communicate to the {@link ChukwaAgent}. Can be used
 * to register and unregister new {@link Adaptor}s. Also contains functions for applications to
 * use for handling log rations.  
 */
public class ChukwaAgentController {
  
  public class AddAdaptorTask extends TimerTask {
    String adaptorName;
    String type;
    String params;
    long offset;
    long numRetries;
    long retryInterval;
    
    AddAdaptorTask(String adaptorName, String type, String params,
        long offset, long numRetries, long retryInterval){
      this.adaptorName = adaptorName;
      this.type = type;
      this.params = params;
      this.offset = offset;
      this.numRetries = numRetries;
      this.retryInterval = retryInterval;
    }
    @Override
    public void run() {
      add(adaptorName, type, params, offset, numRetries, retryInterval);
    }
  }

  //our default adaptors, provided here for convenience
  public static final String CharFileTailUTF8 = "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8";
  public static final String CharFileTailUTF8NewLineEscaped = "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped";
  
    
  static String DEFAULT_FILE_TAILER = CharFileTailUTF8NewLineEscaped;
  static int DEFAULT_PORT = 9093;
  static String DEFAULT_HOST = "localhost";
  static int numArgs = 0;
  
  class Adaptor{
    public long id = -1;    
    final public String name;
    final public String params;
    final public String appType;
    public long offset; 

    
    Adaptor(String adaptorName, String appType, String params, long offset){
      this.name = adaptorName;
      this.appType = appType;
      this.params = params;
      this.offset = offset;
    }
    
    Adaptor(long id, String adaptorName, String appType, String params, long offset){
      this.id = id;
      this.name = adaptorName;
      this.appType = appType;
      this.params = params;
      this.offset = offset;
    }
    
    /**
     * Registers this {@link Adaptor} with the agent running at the specified hostname and portno  
     * @return The id number of the this {@link Adaptor}, assigned by the agent upon successful registration
     * @throws IOException
     */
    long register() throws IOException{
      Socket s = new Socket(hostname, portno);
      PrintWriter bw = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
      bw.println("ADD " + name + " " + appType + " " + params + " " + offset);
      bw.flush();
      BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
      String resp = br.readLine();
      if(resp != null){
        String[] fields = resp.split(" ");
        if(fields[0].equals("OK")){
          try{
            id = Long.parseLong(fields[fields.length -1]);
          }
          catch (NumberFormatException e){}
        }
      }
      s.close();
      return id;
    }
    
    void unregister() throws IOException{
      Socket s = new Socket(hostname, portno);
      PrintWriter bw = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
      bw.println("SHUTDOWN " + id);
      bw.flush();

      BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
      String resp = br.readLine();
      if( resp == null || !resp.startsWith("OK"))
      {
        //error.  What do we do?
      } else if (resp.startsWith("OK")){
        String[] respSplit = resp.split(" ");
        String newOffset = respSplit[respSplit.length-1];
        try {
          offset = Long.parseLong(newOffset);
        }catch (NumberFormatException nfe){
          System.err.println("adaptor didn't shutdown gracefully.\n" + nfe);
        }
      }
      
      s.close();
    }
    
    public String toString(){
      String[] namePieces = name.split("\\.");
      String shortName = namePieces[namePieces.length-1];
      return id + " " + shortName + " " + appType + " " + params + " " + offset; 
    }
  }
  
  Map<Long, ChukwaAgentController.Adaptor> runningAdaptors = new HashMap<Long,Adaptor>();
  Map<Long, ChukwaAgentController.Adaptor> pausedAdaptors;
  String hostname;
  int portno;
  private Timer addFileTimer = new Timer();
  
  public ChukwaAgentController(){
    portno = DEFAULT_PORT;
    hostname = DEFAULT_HOST;
    pausedAdaptors = new HashMap<Long,Adaptor>();
    
    syncWithAgent();
  }
  
  public ChukwaAgentController(String hostname, int portno)
  {
    this.hostname = hostname;
    this.portno = portno;
    pausedAdaptors = new HashMap<Long,Adaptor>();

    syncWithAgent();
  }

  private boolean syncWithAgent() {
    //set up adaptors by using list here
    try{
      runningAdaptors = list();
      return true;
    }catch(IOException e){
      System.err.println("Error initializing ChukwaClient with list of " +
          "currently registered adaptors, clearing our local list of adaptors");
      //e.printStackTrace();
      //if we can't connect to the LocalAgent, reset/clear our local view of the Adaptors.
      runningAdaptors = new HashMap<Long,ChukwaAgentController.Adaptor>();
      return false;
    }
  }
  
  /**
   * Registers a new adaptor. Makes no guarantee about success. On failure,
   * we print a message to stderr and ignore silently so that an application
   * doesn't crash if it's attempt to register an adaptor fails. This call does
   * not retry a conection. for that use the overloaded version of this which
   * accepts a time interval and number of retries
   * @return the id number of the adaptor, generated by the agent
   */
   public long add(String adaptorName, String type, String params, long offset){
     return add(adaptorName, type, params, offset, 20, 15* 1000);//retry for five minutes, every fifteen seconds
   }
   
   /**
    * Registers a new adaptor. Makes no guarantee about success. On failure,
    * to connect to server, will retry <code>numRetries</code> times, every
    * <code>retryInterval</code> milliseconds.
    * @return the id number of the adaptor, generated by the agent
    */
   public long add(String adaptorName, String type, String params, long offset, long numRetries, long retryInterval){
     ChukwaAgentController.Adaptor adaptor = new ChukwaAgentController.Adaptor(adaptorName, type, params, offset);
     long adaptorID = -1;
     if (numRetries >= 0){
       try{
         adaptorID = adaptor.register();

         if (adaptorID > 0){
           runningAdaptors.put(adaptorID,adaptor);
         }
         else{
           System.err.println("Failed to successfully add the adaptor in AgentClient, adaptorID returned by add() was negative.");
         }
       }catch(IOException ioe){
         System.out.println("AgentClient failed to contact the agent (" + hostname + ":" + portno + ")");
         System.out.println("Scheduling a agent connection retry for adaptor add() in another " +
             retryInterval + " milliseconds, " + numRetries + " retries remaining");
         
         addFileTimer.schedule(new AddAdaptorTask(adaptorName, type, params, offset, numRetries-1, retryInterval), retryInterval);
       }
     }else{
       System.err.println("Giving up on connecting to the local agent");
     }
     return adaptorID;
   } 

   public synchronized ChukwaAgentController.Adaptor remove(long adaptorID) throws IOException
   {
     syncWithAgent();
     ChukwaAgentController.Adaptor a = runningAdaptors.remove(adaptorID);
     a.unregister();
     return a;
     
   }
   
   public void remove(String className, String appType, String filename) throws IOException
   {
     syncWithAgent();
     // search for FileTail adaptor with string of this file name
     // get its id, tell it to unregister itself with the agent,
     // then remove it from the list of adaptors
     for (Adaptor a : runningAdaptors.values()){
       if (a.name.equals(className) && a.params.equals(filename) && a.appType.equals(appType)){
         remove(a.id);
       }
     }
   }
   
   
   public void removeAll(){
     syncWithAgent();
     Long[] keyset = runningAdaptors.keySet().toArray(new Long[]{});

     for (long id : keyset){
       try {
         remove(id);
       }catch(IOException ioe){
         System.err.println("Error removing an adaptor in removeAll()");
         ioe.printStackTrace();
       }
       System.out.println("Successfully removed adaptor " + id);
     }
   }
   
   Map<Long,ChukwaAgentController.Adaptor> list() throws IOException
   {  
     Socket s = new Socket(hostname, portno);
     PrintWriter bw = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
   
     bw.println("LIST");
     bw.flush();
     BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
     String ln;
     Map<Long,Adaptor> listResult = new HashMap<Long,Adaptor>();
     while((ln = br.readLine())!= null)
     {
       if (ln.equals("")){
         break;
       }else{
         String[] parts = ln.split("\\s+");
         if (parts.length >= 4){ //should have id, className appType, params, offset
           long id = Long.parseLong(parts[0].substring(0,parts[0].length()-1)); //chop off the right-paren
           long offset = Long.parseLong(parts[parts.length-1]);
           String tmpParams = parts[3];
           for (int i = 4; i<parts.length-1; i++){
             tmpParams += " " + parts[i];
           }
           listResult.put(id, new Adaptor(id,parts[1],parts[2],tmpParams, offset));
         }
       }
     }
     s.close();
     return listResult;
   }
   
   //************************************************************************
   // The following functions are convenience functions, defining an easy
   // to use API for application developers to integrate chukwa into their app
   //************************************************************************
   
   /**
    * Registers a new "LineFileTailUTF8" adaptor and starts it at offset 0. Checks to
    * see if the file is being watched already, if so, won't register another adaptor
    * with the agent. If you have run the tail adaptor on this file before and rotated 
    * or emptied the file you should use {@link ChukwaAgentController#pauseFile(String, String)}
    * and {@link ChukwaAgentController#resumeFile(String, String)} which will store the adaptors 
    * metadata and re-use them to pick up where it left off.
    * @param type the datatype associated with the file to pass through
    * @param filename of the file for the tail adaptor to start monitoring
    * @return the id number of the adaptor, generated by the agent
    */
  public long addFile(String appType, String filename, long numRetries, long retryInterval)
  {
    filename = new File(filename).getAbsolutePath();
    //TODO: Mabye we want to check to see if the file exists here?
    //      Probably not because they might be talking to an agent on a different machine?
    
    //check to see if this file is being watched already, if yes don't set up another adaptor for it
    boolean isDuplicate = false;
    for (Adaptor a : runningAdaptors.values()){
      if (a.name.equals(DEFAULT_FILE_TAILER) && a.appType.equals(appType) && a.params.endsWith(filename)){
        isDuplicate = true;
      }
    }
    if (!isDuplicate){
      return add(DEFAULT_FILE_TAILER, appType, 0L + " " + filename,0L, numRetries, retryInterval);
    }
    else{
      System.out.println("An adaptor for filename \"" + filename + "\", type \""
          + appType + "\", exists already, addFile() command aborted");
      return -1;
    }
  }
  
  public long addFile(String appType, String filename){
    return addFile(appType, filename, 0, 0);
  }
 
  /**
   * Pause all active adaptors of the default file tailing type who are tailing this file
   * This means we actually stop the adaptor and it goes away forever, but we store it
   * state so that we can re-launch a new adaptor with the same state later.
   * @param appType
   * @param filename
   * @return array of adaptorID numbers which have been created and assigned the state of the formerly paused adaptors 
   * @throws IOException
   */
  public Collection<Long> pauseFile(String appType, String filename) throws IOException{
    syncWithAgent();
    //store the unique streamid of the file we are pausing.
    //search the list of adaptors for this filename
    //store the current offset for it
    List<Long> results = new ArrayList<Long>();
    for (Adaptor a : runningAdaptors.values()){
      if (a.name.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename) && a.appType.equals(appType)){
        pausedAdaptors.put(a.id,a); //add it to our list of paused adaptors
        remove(a.id); //tell the agent to remove/unregister it
        results.add(a.id);
      }
    }
    return results;
  }
  
  public boolean isFilePaused(String appType, String filename){
    for (Adaptor a : pausedAdaptors.values()){
      if (a.name.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename) && a.appType.equals(appType)){
        return true;
      }
    }
    return false;
  }
  
  /**
   * Resume all adaptors for this filename that have been paused
   * @param appType the appType
   * @param filename filename by which to lookup adaptors which are paused (and tailing this file)
   * @return an array of the new adaptor ID numbers which have resumed where the old adaptors left off
   * @throws IOException
   */
  public Collection<Long> resumeFile(String appType, String filename) throws IOException{
    syncWithAgent();
    //search for a record of this paused file
    List<Long> results = new ArrayList<Long>();
    for (Adaptor a : pausedAdaptors.values()){
      if (a.name.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename) && a.appType.equals(appType)){
        long newID = add(DEFAULT_FILE_TAILER, a.appType, a.offset + " " + filename, a.offset);
        pausedAdaptors.remove(a.id);
        a.id = newID;
        results.add(a.id);
      }
    }
    return results;
  }
  
  
  public void removeFile(String appType, String filename) throws IOException
  {
    syncWithAgent();
    // search for FileTail adaptor with string of this file name
    // get its id, tell it to unregister itself with the agent,
    // then remove it from the list of adaptors
    for (Adaptor a : runningAdaptors.values()){
      if (a.name.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename) && a.appType.equals(appType)){
        remove(a.id);
      }
    }
  }
  
  //************************************************************************
  // command line utilities
  //************************************************************************
  
  public static void main(String[] args)
  {
    ChukwaAgentController c = getClient(args);
    if (numArgs >= 3 && args[0].toLowerCase().equals("addfile")){
      doAddFile(c, args[1], args[2]);
    }
    else if (numArgs >= 3 && args[0].toLowerCase().equals("removefile")){
      doRemoveFile(c, args[1], args[2]);
    }
    else if(numArgs >= 1 && args[0].toLowerCase().equals("list")){
      doList(c);
    }
    else if(numArgs >= 1 && args[0].equalsIgnoreCase("removeall")){
      doRemoveAll(c);
    }
    else{
      System.err.println("usage: ChukwaClient addfile <apptype> <filename> [-h hostname] [-p portnumber]");
      System.err.println("       ChukwaClient removefile adaptorID [-h hostname] [-p portnumber]");
      System.err.println("       ChukwaClient removefile <apptype> <filename> [-h hostname] [-p portnumber]");
      System.err.println("       ChukwaClient list [IP] [port]");
      System.err.println("       ChukwaClient removeAll [IP] [port]");
    }
  }
  
  private static ChukwaAgentController getClient(String[] args){
    int portno = 9093;
    String hostname = "localhost";

    numArgs = args.length;
    
    for (int i = 0; i < args.length; i++){
      if(args[i].equals("-h") && args.length > i + 1){
        hostname = args[i+1];
        System.out.println ("Setting hostname to: " + hostname);
        numArgs -= 2; //subtract for the flag and value
      }
      else if (args[i].equals("-p") && args.length > i + 1){
        portno = Integer.parseInt(args[i+1]);
        System.out.println ("Setting portno to: " + portno);
        numArgs -= 2; //subtract for the flat, i.e. -p, and value
      }
    }
    return new ChukwaAgentController(hostname, portno);
  }
  
  private static long doAddFile(ChukwaAgentController c, String appType, String params){
    System.out.println("Adding adaptor with filename: " + params);
    long adaptorID = c.addFile(appType, params);
    if (adaptorID != -1){
      System.out.println("Successfully added adaptor, id is:" + adaptorID);
    }else{
      System.err.println("Agent reported failure to add adaptor, adaptor id returned was:" + adaptorID);
    }
    return adaptorID;
  }
  
  private static void doRemoveFile(ChukwaAgentController c, String appType, String params){
    try{
      System.out.println("Removing adaptor with filename: " + params);
      c.removeFile(appType,params);
          }
    catch(IOException e)
    {
      e.printStackTrace();
    }
  }
  
  private static void doList(ChukwaAgentController c){
    try{
      Iterator<Adaptor> adptrs = c.list().values().iterator();
      while (adptrs.hasNext()){
        System.out.println(adptrs.next().toString());
      }
    } catch(Exception e){
      e.printStackTrace();
    }
  }
  
  private static void doRemoveAll(ChukwaAgentController c){
    System.out.println("Removing all adaptors");
    c.removeAll();
  }
}
