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

package org.apache.hadoop.hive.conf;

import java.io.PrintStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import org.apache.commons.lang.StringUtils;

/**
 * Hive Configuration
 */
public class HiveConf extends Configuration {

  protected String hiveJar;
  protected Properties origProp;
  protected String auxJars;
  private static final Log l4j = LogFactory.getLog(HiveConf.class);

  public static enum ConfVars {
    // QL execution stuff
    SCRIPTWRAPPER("hive.exec.script.wrapper", null),
    PLAN("hive.exec.plan", null),
    SCRATCHDIR("hive.exec.scratchdir", "/tmp/"+System.getProperty("user.name")+"/hive"),
    SUBMITVIACHILD("hive.exec.submitviachild", "false"),

    // hadoop stuff
    HADOOPBIN("hadoop.bin.path", System.getProperty("user.dir") + "/../../../bin/hadoop"),
    HADOOPCONF("hadoop.config.dir", System.getProperty("user.dir") + "/../../../conf"),
    HADOOPFS("fs.default.name", "file:///"),
    HADOOPMAPFILENAME("map.input.file", null),
    HADOOPJT("mapred.job.tracker", "local"),
    HADOOPNUMREDUCERS("mapred.reduce.tasks", "1"),
    HADOOPJOBNAME("mapred.job.name", null),

    // MetaStore stuff.
    METASTOREDIRECTORY("hive.metastore.metadb.dir", ""),
    METASTOREWAREHOUSE("hive.metastore.warehouse.dir", ""),
    METASTOREURIS("hive.metastore.uris", ""),

    // Things we log in the jobconf

    // session identifier
    HIVESESSIONID("hive.session.id", ""),
    // query being executed (multiple per session)
    HIVEQUERYID("hive.query.string", ""),
    // id of the mapred plan being executed (multiple per query)
    HIVEPLANID("hive.query.planid", ""),
    // max jobname length
    HIVEJOBNAMELENGTH("hive.jobname.length", 50),
    
    // hive jar
    HIVEJAR("hive.jar.path", ""), 
    HIVEAUXJARS("hive.aux.jars.path", ""),
   
    // for hive script operator
    HIVETABLENAME("hive.table.name", ""),
    HIVEPARTITIONNAME("hive.partition.name", ""),
    HIVEPARTITIONPRUNER("hive.partition.pruning", "nonstrict"),
    HIVEALIAS("hive.alias", "");
    
    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final Class valClass;

    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.defaultVal = defaultVal;
      this.valClass = String.class;
      this.defaultIntVal = -1;
    }

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.defaultVal = null;
      this.defaultIntVal = defaultIntVal;
      this.valClass = Integer.class;
    }

    public String toString() {
      return varname;
    }
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    assert(var.valClass == Integer.class);
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public int getIntVar(ConfVars var) {
    return getIntVar(this, var);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    assert(var.valClass == String.class);
    return conf.get(var.varname, var.defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert(var.valClass == String.class);
    conf.set(var.varname, val);
  }

  public String getVar(ConfVars var) {
    return getVar(this, var);
  }

  public void setVar(ConfVars var, String val) {
    setVar(this, var, val);
  }

  public void logVars(PrintStream ps) {
    for(ConfVars one: ConfVars.values()) {
      ps.println(one.varname + "=" + ((get(one.varname) != null) ? get(one.varname) : ""));
    }
  }


  public HiveConf(Class cls) {
    super();
    initialize(cls);
  }

  public HiveConf(Configuration other, Class cls) {
    super(other);
    initialize(cls);
  }

  private Properties getUnderlyingProps() {
    Iterator<Map.Entry<String, String>> iter = this.iterator();
    Properties p = new Properties();
    while(iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      p.setProperty(e.getKey(), e.getValue());
    }
    return p;
  }


  private void initialize(Class cls) {
    hiveJar = (new JobConf(cls)).getJar();
    
    // preserve the original configuration
    origProp = getUnderlyingProps();
    
    // let's add the hive configuration 
    URL hconfurl = getClassLoader().getResource("hive-default.xml");
    if(hconfurl == null) {
      l4j.debug("hive-default.xml not found.");
    } else {
      addResource(hconfurl);
    }
    URL hsiteurl = getClassLoader().getResource("hive-site.xml");
    if(hsiteurl == null) {
      l4j.debug("hive-site.xml not found.");
    } else {
      addResource(hsiteurl);
    }

    // if hadoop configuration files are already in our path - then define 
    // the containing directory as the configuration directory
    URL hadoopconfurl = getClassLoader().getResource("hadoop-default.xml");
    if(hadoopconfurl == null) 
      hadoopconfurl = getClassLoader().getResource("hadoop-site.xml");
    if(hadoopconfurl != null) {
      String conffile = hadoopconfurl.getPath();
      this.setVar(ConfVars.HADOOPCONF, conffile.substring(0, conffile.lastIndexOf('/')));
    }

    applySystemProperties();

    // if the running class was loaded directly (through eclipse) rather than through a
    // jar then this would be needed
    if(hiveJar == null) {
      hiveJar = this.get(ConfVars.HIVEJAR.varname);
    }
    
    if(auxJars == null) {
      auxJars = this.get(ConfVars.HIVEAUXJARS.varname);
    }

  }

  public void applySystemProperties() {
    for(ConfVars oneVar: ConfVars.values()) {
      if(System.getProperty(oneVar.varname) != null) {
        if(System.getProperty(oneVar.varname).length() > 0)
        this.set(oneVar.varname, System.getProperty(oneVar.varname));
      }
    }
  }

  public Properties getChangedProperties() {
    Properties ret = new Properties();
    Properties newProp = getUnderlyingProps();

    for(Object one: newProp.keySet()) {
      String oneProp = (String)one;
      String oldValue = origProp.getProperty(oneProp);
      if(!StringUtils.equals(oldValue, newProp.getProperty(oneProp))) {
        ret.setProperty(oneProp, newProp.getProperty(oneProp));
      }
    }
    return (ret);
  }

  public Properties getAllProperties() {
    return getUnderlyingProps();
  }

  public String getJar() {
    return hiveJar;
  }

  /**
   * @return the auxJars
   */
  public String getAuxJars() {
    return auxJars;
  }

  /**
   * @param auxJars the auxJars to set
   */
  public void setAuxJars(String auxJars) {
    this.auxJars = auxJars;
  }
}
