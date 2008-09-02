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

package org.apache.hadoop.hive.ql.session;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;
import java.net.URL;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.commons.lang.StringUtils;

public class SessionState {
  
  /**
   * current configuration
   */ 
  protected HiveConf conf;

  /**
   * silent mode
   */
  protected boolean isSilent;

  /**
   * cached current connection to Hive MetaStore
   */
  protected Hive db;

  /**
   * Streams to read/write from
   */
  public PrintStream out;
  public InputStream in;
  public PrintStream err;


  public HiveConf getConf() { return conf; }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public boolean getIsSilent() {
    return isSilent;
  }

  public void setIsSilent(boolean isSilent) {
    this.isSilent = isSilent;
  }

  public SessionState() {
    this(null, null);
  }

  public SessionState (HiveConf conf) {
    this (conf, null);
  }
  
  public SessionState (HiveConf conf, Hive db) {
    this.conf = conf;
    this.db = db;

    for(HiveConf.ConfVars oneVar: metaVars) {
      dbOptions.put(oneVar, conf.getVar(oneVar));
    }
  }

  /**
   * metastore related options that the db is initialized against
   */
  protected final static HiveConf.ConfVars [] metaVars = {
    HiveConf.ConfVars.METASTOREDIRECTORY,
    HiveConf.ConfVars.METASTOREWAREHOUSE,
    HiveConf.ConfVars.METASTOREURIS
  };

  /**
   * cached values of such options
   */
  private final HashMap<HiveConf.ConfVars, String> dbOptions =
    new HashMap<HiveConf.ConfVars, String> ();

  public Hive getDb() throws HiveException {
    boolean needsRefresh = false;

    for(HiveConf.ConfVars oneVar: metaVars) {
      if(!StringUtils.isEmpty(StringUtils.difference(dbOptions.get(oneVar), conf.getVar(oneVar)))) {
        needsRefresh = true;
        break;
      }
    }
    
    if((db == null) || needsRefresh) {
      db = Hive.get(conf);
    }
  
    return db;
  }

  public void setCmd(String cmdString) {
    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, cmdString);
  }

  public String getCmd() {
    return (conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
  }

  public String getSessionId() {
    return (conf.getVar(HiveConf.ConfVars.HIVESESSIONID));
  }


  /**
   * Singleton Session object
   *
   * For multiple sessions - we could store in a hashmap or have a thread local var
   **/
  private static SessionState ss;

  /**
   * start a new session
   */
  public static SessionState start(HiveConf conf) {
    ss = new SessionState (conf);
    ss.getConf().setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
    return (ss);
  }

  public static SessionState start(SessionState startSs) {
    ss = startSs;
    ss.getConf().setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
    return ss;
  }

  /**
   * get the current session
   */
  public static SessionState get() {
    return ss;
  }

  private static String makeSessionId() {
    GregorianCalendar gc = new GregorianCalendar();
    String userid = System.getProperty("user.name");

    return userid + "_" +
      String.format("%1$4d%2$02d%3$02d%4$02d%5$02d", gc.get(Calendar.YEAR),
                    gc.get(Calendar.MONTH) + 1,
                    gc.get(Calendar.DAY_OF_MONTH),
                    gc.get(Calendar.HOUR_OF_DAY),
                    gc.get(Calendar.MINUTE));
  }

  public static final String HIVE_L4J = "hive-log4j.properties";

  public static void initHiveLog4j () {
    // allow hive log4j to override any normal initialized one
    URL hive_l4j = SessionState.class.getClassLoader().getResource(HIVE_L4J);
    if(hive_l4j == null) {
      System.out.println(HIVE_L4J + " not found");
    } else {
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(hive_l4j);
    }
  }

  public static class LogHelper {

    protected Log LOG;
    protected boolean isSilent;
    protected SessionState ss;
    
    public LogHelper(SessionState ss, boolean isSilent, Log LOG) {
      this.LOG = LOG;
      this.isSilent = isSilent;
      this.ss = ss;
    }

    public LogHelper(Log LOG) {
      // the session control silent or not
      this(SessionState.get(), false, LOG);
    }

    public LogHelper(Log LOG, boolean isSilent) {
      // no session info - use isSilent setting passed in
      this(null, isSilent, LOG);
    }

    public PrintStream getOutStream() {
      return ((ss != null) && (ss.out != null)) ? ss.out : System.out;   
    }

    public PrintStream getErrStream() {
      return ((ss != null) && (ss.err != null)) ? ss.err : System.err;
    }

    public boolean getIsSilent() {
      // use the session or the one supplied in constructor
      return (ss != null) ? ss.getIsSilent() : isSilent;
    }

    public void printInfo(String info) {
      printInfo(info, null);
    }

    public void printInfo(String info, String detail) {
      if(!getIsSilent()) {
        getOutStream().println(info);
      }
      LOG.info(info + StringUtils.defaultString(detail));
    }

    public void printError(String error) {
      printError(error, null);
    }

    public void printError(String error, String detail) {
      getErrStream().println(error);
      LOG.error(error + StringUtils.defaultString(detail));
    }
  }
}
