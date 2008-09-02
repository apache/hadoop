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

package org.apache.hadoop.hive.ql.parse;

import java.util.*;
import java.io.File;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.antlr.runtime.tree.CommonTree;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public abstract class BaseSemanticAnalyzer {
  protected String scratchDir;
  protected int randomid;
  protected int pathid;

  protected final Hive db;
  protected final HiveConf conf;
  protected List<Task<? extends Serializable>> rootTasks;
  protected final Log LOG;
  protected final LogHelper console;

  protected Context ctx;
  public BaseSemanticAnalyzer(HiveConf conf) throws SemanticException {
    try {
      this.conf = conf;
      db = Hive.get(conf);
      rootTasks = new ArrayList<Task<? extends Serializable>>();
      LOG = LogFactory.getLog(this.getClass().getName());
      console = new LogHelper(LOG);

      this.scratchDir = this.db.getConf().getVar(HiveConf.ConfVars.SCRATCHDIR);
      Random rand = new Random();
      this.randomid = Math.abs(rand.nextInt()%rand.nextInt());
      this.pathid = 10000;
    } catch (Exception e) {
      throw new SemanticException (e);
    }
  }

  public abstract void analyze(CommonTree ast, Context ctx) throws SemanticException;

  public List<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  protected void reset() {
    rootTasks = new ArrayList<Task<? extends Serializable>>();
  }

  public static String stripQuotes(String val) throws SemanticException {
    if (val.charAt(0) == '\'' && val.charAt(val.length() - 1) == '\'') {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }
  
  @SuppressWarnings("nls")
  public static String unescapeSQLString(String b) {
    assert(b.charAt(0) == '\'');
    assert(b.charAt(b.length()-1) == '\'');
    StringBuilder sb = new StringBuilder(b.length());
    for(int i=1; i+1<b.length(); i++) {
      if (b.charAt(i) == '\\' && i+2<b.length()) {
        char n=b.charAt(i+1);
        switch(n) {
        case '0': sb.append("\0"); break;
        case '\'': sb.append("'"); break;
        case '"': sb.append("\""); break;
        case 'b': sb.append("\b"); break;
        case 'n': sb.append("\n"); break;
        case 'r': sb.append("\r"); break;
        case 't': sb.append("\t"); break;
        case 'Z': sb.append("\u001A"); break;
        case '\\': sb.append("\\"); break;
        // The following 2 lines are exactly what MySQL does
        case '%': sb.append("\\%"); break;
        case '_': sb.append("\\_"); break;
        default: sb.append(n);
        }
        i++;
      } else {
        sb.append(b.charAt(i));
      }
    }
    return sb.toString();
  }
  
  public String getTmpFileName() 
  {
    // generate the temporary file
    String taskTmpDir = this.scratchDir + File.separator + this.randomid + '.' + this.pathid;
    this.pathid++;
    return taskTmpDir;
  }
  
  public static class tableSpec {
    public String tableName;
    public Table tableHandle;
    public HashMap<String, String> partSpec;
    public Partition partHandle;

    public tableSpec(Hive db, CommonTree ast) throws SemanticException {

      assert(ast.getToken().getType() == HiveParser.TOK_TAB);
      int childIndex = 0;

      try {
        // get table metadata
        tableName = ast.getChild(0).getText();
        tableHandle = db.getTable(tableName);

        // get partition metadata if partition specified
        if (ast.getChildCount() == 2) {
          childIndex = 1;
          CommonTree partspec = (CommonTree) ast.getChild(1);
          partSpec = new LinkedHashMap<String, String>();
          for (int i = 0; i < partspec.getChildCount(); ++i) {
            CommonTree partspec_val = (CommonTree) partspec.getChild(i);
            String val = stripQuotes(partspec_val.getChild(1).getText());
            partSpec.put(partspec_val.getChild(0).getText(), val);
          }
          partHandle = Hive.get().getPartition(tableHandle, partSpec, true);
        }
      } catch (InvalidTableException ite) {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(ast.getChild(0)), ite);
      } catch (HiveException e) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg(ast.getChild(childIndex), e.getMessage()), e);
      }
    }


    public String toString() {
      if(partHandle != null) 
        return partHandle.toString();
      else 
        return tableHandle.toString();
    }
  }
}
