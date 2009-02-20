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
import java.io.UnsupportedEncodingException;

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
  protected Task<? extends Serializable> fetchTask;
  protected boolean fetchTaskInit;
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

  public abstract void analyzeInternal(CommonTree ast, Context ctx) throws SemanticException;

  public void analyze(CommonTree ast, Context ctx) throws SemanticException {
    scratchDir = ctx.getScratchDir();
    analyzeInternal(ast, ctx);
  }
  
  public List<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  /**
	 * @return the fetchTask
	 */
	public Task<? extends Serializable> getFetchTask() {
		return fetchTask;
	}

	/**
	 * @param fetchTask the fetchTask to set
	 */
	public void setFetchTask(Task<? extends Serializable> fetchTask) {
		this.fetchTask = fetchTask;
	}

	public boolean getFetchTaskInit() {
		return fetchTaskInit;
	}

	public void setFetchTaskInit(boolean fetchTaskInit) {
		this.fetchTaskInit = fetchTaskInit;
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

  public static String charSetString(String charSetName, String charSetString) 
    throws SemanticException {
    try
    {
      // The character set name starts with a _, so strip that
      charSetName = charSetName.substring(1);
      if (charSetString.charAt(0) == '\'')
        return new String(unescapeSQLString(charSetString).getBytes(), charSetName);
      else                                       // hex input is also supported
      {
        assert charSetString.charAt(0) == '0';
        assert charSetString.charAt(1) == 'x';
        charSetString = charSetString.substring(2);
        
        byte[] bArray = new byte[charSetString.length()/2];
        int j = 0;
        for (int i = 0; i < charSetString.length(); i += 2)
        {
          int val = Character.digit(charSetString.charAt(i), 16) * 16 + Character.digit(charSetString.charAt(i+1), 16);
          if (val > 127)
            val = val - 256;
          bArray[j++] = new Integer(val).byteValue();
        }

        String res = new String(bArray, charSetName);
        return res;
      } 
    } catch (UnsupportedEncodingException e) {
      throw new SemanticException(e);
    }
  }

  @SuppressWarnings("nls")
  public static String unescapeSQLString(String b) {
    assert(b.charAt(0) == '\'');
    assert(b.charAt(b.length()-1) == '\'');

    // Some of the strings can be passed in as unicode. For example, the
    // delimiter can be passed in as \002 - So, we first check if the 
    // string is a unicode number, else go back to the old behavior
    StringBuilder sb = new StringBuilder(b.length());
    int i = 1;
    while (i < (b.length()-1)) {

      if (b.charAt(i) == '\\' && (i+4 < b.length())) {
        char i1 = b.charAt(i+1);
        char i2 = b.charAt(i+2);
        char i3 = b.charAt(i+3);
        if ((i1 >= '0' && i1 <= '1') &&
            (i2 >= '0' && i2 <= '7') &&
            (i3 >= '0' && i3 <= '7'))
        {
          byte bVal = (byte)((i3 - '0') + ((i2 - '0') * 8 ) + ((i1 - '0') * 8 * 8));
          byte[] bValArr = new byte[1];
          bValArr[0] = bVal;
          String tmp = new String(bValArr);
          sb.append(tmp);
          i += 4;
          continue;
        }
      }
        
      if (b.charAt(i) == '\\' && (i+2 < b.length())) {
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
      i++;
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

    public tableSpec(Hive db, CommonTree ast, boolean forceCreatePartition) throws SemanticException {

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
          partHandle = Hive.get().getPartition(tableHandle, partSpec, forceCreatePartition);
          if(partHandle == null) {
            throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(ast.getChild(childIndex)));
          }
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
