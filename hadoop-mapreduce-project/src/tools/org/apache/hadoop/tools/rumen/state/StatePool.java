/**
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
package org.apache.hadoop.tools.rumen.state;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.Anonymizer;
import org.apache.hadoop.tools.rumen.datatypes.DataType;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.module.SimpleModule;

/**
 * A pool of states. States used by {@link DataType}'s can be managed the 
 * {@link StatePool}. {@link StatePool} also supports persistence. Persistence
 * is key to share states across multiple {@link Anonymizer} runs.
 */
@SuppressWarnings("unchecked")
public class StatePool {
  private static final long VERSION = 1L;
  private boolean isUpdated = false;
  private boolean isInitialized = false;
  private Configuration conf;
  
  // persistence configuration
  public static final String DIR_CONFIG = "rumen.anonymization.states.dir";
  public static final String RELOAD_CONFIG = 
    "rumen.anonymization.states.reload";
  public static final String PERSIST_CONFIG = 
    "rumen.anonymization.states.persist";
  
  // internal state management configs
  private static final String COMMIT_STATE_FILENAME = "latest";
  private static final String CURRENT_STATE_FILENAME = "temp";
  
  private String timeStamp;
  private Path persistDirPath;
  private boolean reload;
  private boolean persist;
  
  /**
   * A wrapper class that binds the state implementation to its implementing 
   * class name.
   */
  public static class StatePair {
    private String className;
    private State state;
    
    public StatePair(State state) {
      this.className = state.getClass().getName();
      this.state = state;
    }
    
    public String getClassName() {
      return className;
    }
    
    public void setClassName(String className) {
      this.className = className;
    }
    
    public State getState() {
      return state;
    }
    
    public void setState(State state) {
      this.state = state;
    }
  }
  
  /**
   * Identifies to identify and cache {@link State}s.
   */
  private HashMap<String, StatePair> pool = new HashMap<String, StatePair>();
  
  public void addState(Class id, State state) {
    if (pool.containsKey(id.getName())) {
      throw new RuntimeException("State '" + state.getName() + "' added for the" 
          + " class " + id.getName() + " already exists!");
    }
    isUpdated = true;
    pool.put(id.getName(), new StatePair(state));
  }
  
  public State getState(Class clazz) {
    return pool.containsKey(clazz.getName()) 
           ? pool.get(clazz.getName()).getState() 
           : null;
  }
  
  // For testing
  @JsonIgnore
  public boolean isUpdated() {
    if (!isUpdated) {
      for (StatePair statePair : pool.values()) {
        // if one of the states have changed, then the pool is dirty
        if (statePair.getState().isUpdated()) {
          isUpdated = true;
          return true;
        }
      }
    }
    return isUpdated;
  }
  
  /**
   * Initialized the {@link StatePool}. This API also reloads the previously
   * persisted state. Note that the {@link StatePool} should be initialized only
   * once.
   */
  public void initialize(Configuration conf) throws Exception {
    if (isInitialized) {
      throw new RuntimeException("StatePool is already initialized!");
    }
    
    this.conf = conf;
    String persistDir = conf.get(DIR_CONFIG);
    reload = conf.getBoolean(RELOAD_CONFIG, false);
    persist = conf.getBoolean(PERSIST_CONFIG, false);
    
    // reload if configured
    if (reload || persist) {
      System.out.println("State Manager initializing. State directory : " 
                         + persistDir);
      System.out.println("Reload:" + reload + " Persist:" + persist);
      if (persistDir == null) {
        throw new RuntimeException("No state persist directory configured!" 
                                   + " Disable persistence.");
      } else {
        this.persistDirPath = new Path(persistDir);
      }
    } else {
      System.out.println("State Manager disabled.");
    }
    
    // reload
    reload();
    
    // now set the timestamp
    DateFormat formatter = 
      new SimpleDateFormat("dd-MMM-yyyy-hh'H'-mm'M'-ss'S'");
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(System.currentTimeMillis());
    timeStamp = formatter.format(calendar.getTime());
    
    isInitialized = true;
  }
  
  private void reload() throws Exception {
    if (reload) {
      // Reload persisted entries
      Path stateFilename = new Path(persistDirPath, COMMIT_STATE_FILENAME);
      FileSystem fs = stateFilename.getFileSystem(conf);
      if (fs.exists(stateFilename)) {
        reloadState(stateFilename, conf);
      } else {
        throw new RuntimeException("No latest state persist directory found!" 
                                   + " Disable persistence and run.");
      }
    }
  }
  
  private void reloadState(Path stateFile, Configuration conf) 
  throws Exception {
    FileSystem fs = stateFile.getFileSystem(conf);
    if (fs.exists(stateFile)) {
      System.out.println("Reading state from " + stateFile.toString());
      FSDataInputStream in = fs.open(stateFile);
      
      read(in);
      in.close();
    } else {
      System.out.println("No state information found for " + stateFile);
    }
  }
  
  private void read(DataInput in) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    
    // define a module
    SimpleModule module = new SimpleModule("State Serializer",  
        new Version(0, 1, 1, "FINAL"));
    // add the state deserializer
    module.addDeserializer(StatePair.class, new StateDeserializer());

    // register the module with the object-mapper
    mapper.registerModule(module);

    JsonParser parser = 
      mapper.getJsonFactory().createJsonParser((DataInputStream)in);
    StatePool statePool = mapper.readValue(parser, StatePool.class);
    this.setStates(statePool.getStates());
    parser.close();
  }
  
  /**
   * Persists the current state to the state directory. The state will be 
   * persisted to the 'latest' file in the state directory.
   */
  public void persist() throws IOException {
    if (!persist) {
      return;
    }
    if (isUpdated()) {
      System.out.println("State is updated! Committing.");
      Path currStateFile = new Path(persistDirPath, CURRENT_STATE_FILENAME);
      Path commitStateFile = new Path(persistDirPath, COMMIT_STATE_FILENAME);
      FileSystem fs = currStateFile.getFileSystem(conf);

      System.out.println("Starting the persist phase. Persisting to " 
                         + currStateFile.toString());
      // persist current state 
      //  write the contents of the current state to the current(temp) directory
      FSDataOutputStream out = fs.create(currStateFile, true);
      write(out);
      out.close();

      System.out.println("Persist phase over. The best known un-committed state"
                         + " is located at " + currStateFile.toString());

      // commit (phase-1) 
      //  copy the previous commit file to the relocation file
      if (fs.exists(commitStateFile)) {
        Path commitRelocationFile = new Path(persistDirPath, timeStamp);
        System.out.println("Starting the pre-commit phase. Moving the previous " 
            + "best known state to " + commitRelocationFile.toString());
        // copy the commit file to the relocation file
        FileUtil.copy(fs,commitStateFile, fs, commitRelocationFile, false, 
                      conf);
      }

      // commit (phase-2)
      System.out.println("Starting the commit phase. Committing the states in " 
                         + currStateFile.toString());
      FileUtil.copy(fs, currStateFile, fs, commitStateFile, true, true, conf);

      System.out.println("Commit phase successful! The best known committed " 
                         + "state is located at " + commitStateFile.toString());
    } else {
      System.out.println("State not updated! No commit required.");
    }
  }
  
  private void write(DataOutput out) throws IOException {
    // This is just a JSON experiment
    System.out.println("Dumping the StatePool's in JSON format.");
    ObjectMapper outMapper = new ObjectMapper();
    outMapper.configure(
        SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    // define a module
    SimpleModule module = new SimpleModule("State Serializer",  
        new Version(0, 1, 1, "FINAL"));
    // add the state serializer
    //module.addSerializer(State.class, new StateSerializer());

    // register the module with the object-mapper
    outMapper.registerModule(module);

    JsonFactory outFactory = outMapper.getJsonFactory();
    JsonGenerator jGen = 
      outFactory.createJsonGenerator((DataOutputStream)out, JsonEncoding.UTF8);
    jGen.useDefaultPrettyPrinter();

    jGen.writeObject(this);
    jGen.close();
  }
  
  /**
   * Getters and setters for JSON serialization
   */
  
  /**
   * To be invoked only by the Jackson JSON serializer.
   */
  public long getVersion() {
    return VERSION;
  }
  
  /**
   * To be invoked only by the Jackson JSON deserializer.
   */
  public void setVersion(long version) {
    if (version != VERSION) {
      throw new RuntimeException("Version mismatch! Expected " + VERSION 
                                 + " got " + version);
    }
  }
  
  /**
   * To be invoked only by the Jackson JSON serializer.
   */
  public HashMap<String, StatePair> getStates() {
    return pool;
  }
  
  /**
   * To be invoked only by the Jackson JSON deserializer.
   */
  public void setStates(HashMap<String, StatePair> states) {
    if (pool.size() > 0) {
      throw new RuntimeException("Pool not empty!");
    }
    
    //TODO Should we do a clone?
    this.pool = states;
  }
}