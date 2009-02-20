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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.util.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Task implementation
 **/

public abstract class Task <T extends Serializable> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient boolean isdone;
  transient protected HiveConf conf;
  transient protected Hive db;
  transient protected Log LOG;
  transient protected LogHelper console;

  // Bean methods

  protected List<Task<? extends Serializable>> childTasks;
  protected List<Task<? extends Serializable>> parentTasks;

  public Task() {
    isdone = false;
    LOG = LogFactory.getLog(this.getClass().getName());
  }

  public void initialize (HiveConf conf) {
    isdone = false;
    this.conf = conf;
    
    SessionState ss = SessionState.get();
    try {
      if (ss == null) {
        // test case - no session setup perhaps
        db = Hive.get(conf);
      } else {
        // normal case - session has handle to db
        db = ss.getDb();
      }
    } catch (HiveException e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException (e);
    }

    console = new LogHelper(LOG);

    if(childTasks == null) {
      return;
    }

    for(Task<? extends Serializable> t: childTasks) {
      t.initialize(conf);
    }
  }

  public abstract int execute();
  
  // dummy method - FetchTask overwrites this
  public boolean fetch(Vector<String> res) { 
    assert false;
  	return false;
  }

  public void setChildTasks(List<Task<? extends Serializable>> childTasks) {
    this.childTasks = childTasks;
  }

  public List<Task<? extends Serializable>> getChildTasks() {
    return childTasks;
  }

  public void setParentTasks(List<Task<? extends Serializable>> parentTasks) {
    this.parentTasks = parentTasks;
  }

  public List<Task<? extends Serializable>> getParentTasks() {
    return parentTasks;
  }

  public void addDependentTask(Task<? extends Serializable> dependent) {
    if (getChildTasks() == null) {
      setChildTasks(new ArrayList<Task<? extends Serializable>>());
    }
    if (!getChildTasks().contains(dependent)) {
      getChildTasks().add(dependent);
      if (dependent.getParentTasks() == null) {
        dependent.setParentTasks(new ArrayList<Task<? extends Serializable>>());
      }
      if (!dependent.getParentTasks().contains(this)) {
        dependent.getParentTasks().add(this);
      }
    }
  }

  public boolean done() {
    return isdone;
  }

  public void setDone() {
    isdone = true;
  }

  public boolean isRunnable() {
    boolean isrunnable = true;
    if (parentTasks != null) {
      for(Task<? extends Serializable> parent: parentTasks) {
        if (!parent.done()) {
          isrunnable = false;
          break;
        }
      }
    }
    return isrunnable;
  }

  protected String id;
  protected T work;

  public void setWork(T work) {
    this.work = work;
  }

  public T getWork() {
    return work;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public boolean isMapRedTask() {
    return false;
  }

  public boolean hasReduce() {
    return false;
  }
}
