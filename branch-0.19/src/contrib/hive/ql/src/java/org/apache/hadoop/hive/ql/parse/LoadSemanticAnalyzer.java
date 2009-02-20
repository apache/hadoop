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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.copyWork;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.Context;

public class LoadSemanticAnalyzer extends BaseSemanticAnalyzer {

  boolean isLocal;
  boolean isOverWrite;
  FileSystem fs;

  public LoadSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  public static FileStatus [] matchFilesOrDir(FileSystem fs, Path path) throws IOException {
    FileStatus [] srcs = fs.globStatus(path);
    if((srcs != null) && srcs.length == 1) {
      if(srcs[0].isDir()) {
        srcs = fs.listStatus(srcs[0].getPath());
      }
    }
    return (srcs);
  }

  private URI initializeFromURI(String fromPath) throws IOException {
    // TODO: support hdfs relative path names by defaulting to /user/<user.name>

    Path p = new Path(fromPath);
    URI fromURI = p.toUri();

    String fromScheme = fromURI.getScheme();

    // initialize scheme for 'local' mode
    if(StringUtils.isEmpty(fromScheme)) {
      if(isLocal) {
        if(!fromPath.startsWith("/")) {
          // generate absolute path relative to current directory
          p = new Path(new Path("file://"+System.getProperty("user.dir")), fromPath);
        } else {
          p = new Path("file://"+fromPath);
        }
        fromURI = p.toUri();
        fromScheme = "file";
      }
    }


    fs = FileSystem.get(fromURI, conf);
    String fromAuthority = null;

    // fall back to configuration based scheme if necessary
    if(StringUtils.isEmpty(fromScheme)) {
      fromScheme = fs.getUri().getScheme();
      fromAuthority = fs.getUri().getAuthority();
    }

    // if using hdfs - authority must be specified. fall back using configuration if none specified.
    if(fromScheme.equals("hdfs")) {
      fromAuthority = StringUtils.isEmpty(fromURI.getAuthority()) ?
        fs.getUri().getAuthority() : fromURI.getAuthority();
    }

    try {
      fromURI = new URI(fromScheme, fromAuthority, fromURI.getPath(), null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException (e);
    }
    
    return fromURI;
  }


  private void applyConstraints(URI fromURI, URI toURI, Tree ast, boolean isLocal) throws SemanticException {
    if(!fromURI.getScheme().equals("file") && 
       !fromURI.getScheme().equals("hdfs")) {
      throw new SemanticException (ErrorMsg.INVALID_PATH.getMsg(ast, "only \"file\" or \"hdfs\" file systems accepted"));
    }

    // local mode implies that scheme should be "file"
    // we can change this going forward
    if(isLocal && !fromURI.getScheme().equals("file")) {
      throw new SemanticException (ErrorMsg.ILLEGAL_PATH.getMsg(ast));
    }

    try {
      FileStatus [] srcs = matchFilesOrDir(fs, new Path(fromURI.getScheme(),
                                                        fromURI.getAuthority(),
                                                        fromURI.getPath()));

      if(srcs == null || srcs.length == 0) {
        throw new SemanticException (ErrorMsg.INVALID_PATH.getMsg(ast, "No files matching path"));
      }


      for(FileStatus oneSrc: srcs) {
        if(oneSrc.isDir()) {
          throw new SemanticException
            (ErrorMsg.INVALID_PATH.getMsg(ast,
                                          "source contains directory: " + oneSrc.getPath().toString()));
        }
      }
    } catch (IOException e) {
      // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException (ErrorMsg.INVALID_PATH.getMsg(ast));
    }


    // only in 'local' mode do we copy stuff from one place to another. 
    // reject different scheme/authority in other cases.
    if(!isLocal && (!StringUtils.equals(fromURI.getScheme(), toURI.getScheme()) ||
                    !StringUtils.equals(fromURI.getAuthority(), toURI.getAuthority()))) {
      LOG.error("Move from: " + fromURI.toString() + " to: " + toURI.toString() + " is not valid");
      throw new SemanticException(ErrorMsg.ILLEGAL_PATH.getMsg(ast, "Cannot load data across filesystems, use load data local")) ;
    }
  }

  @Override
  public void analyzeInternal(CommonTree ast, Context ctx) throws SemanticException {
    isLocal = isOverWrite = false;
    Tree from_t = ast.getChild(0);
    Tree table_t = ast.getChild(1);

    if(ast.getChildCount() == 4) {
      isOverWrite = isLocal = true;
    }

    if(ast.getChildCount() == 3) {
      if(ast.getChild(2).getText().toLowerCase().equals("local")) {
        isLocal = true;
      } else {
        isOverWrite = true;
      }
    }

    // initialize load path
    URI fromURI;
    try {
      String fromPath = stripQuotes(from_t.getText());
      fromURI = initializeFromURI(fromPath);
    } catch (IOException e) {
      throw new SemanticException (ErrorMsg.INVALID_PATH.getMsg(from_t, e.getMessage()));
    } catch (RuntimeException e) {
      throw new SemanticException (ErrorMsg.INVALID_PATH.getMsg(from_t, e.getMessage()));
    }

    // initialize destination table/partition
    tableSpec ts = new tableSpec(db, (CommonTree) table_t, true);
    URI toURI = (ts.partHandle != null) ? ts.partHandle.getDataLocation() : ts.tableHandle.getDataLocation();

    // make sure the arguments make sense
    applyConstraints(fromURI, toURI, from_t, isLocal);

    Task<? extends Serializable> rTask = null;

    // create copy work
    if(isLocal) {
      // if the local keyword is specified - we will always make a copy. this might seem redundant in the case 
      // that the hive warehouse is also located in the local file system - but that's just a test case.
      URI copyURI;
      try {
        copyURI = new URI(toURI.getScheme(), toURI.getAuthority(),
                          conf.getVar(HiveConf.ConfVars.SCRATCHDIR) + "/" + Utilities.randGen.nextInt(),
                          null, null);                          
      } catch (URISyntaxException e) {
        // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        LOG.error("Invalid URI. Check value of variable: " + HiveConf.ConfVars.SCRATCHDIR.toString());
        throw new SemanticException("Cannot initialize temporary destination URI");
      }
      rTask = TaskFactory.get(new copyWork(fromURI.toString(), copyURI.toString()), this.conf);
      fromURI = copyURI;
    }

    // create final load/move work
    List<loadTableDesc> loadTableWork =  new ArrayList<loadTableDesc>();
    List<loadFileDesc> loadFileWork = new ArrayList<loadFileDesc>();

    loadTableWork.add(new loadTableDesc(fromURI.toString(), Utilities.getTableDesc(ts.tableHandle),
                                        (ts.partSpec != null) ? ts.partSpec : new HashMap<String, String> (),
                                        isOverWrite));

    if(rTask != null) {
      rTask.addDependentTask(TaskFactory.get(new moveWork(loadTableWork, loadFileWork), this.conf));
    } else {
      rTask = TaskFactory.get(new moveWork(loadTableWork, loadFileWork), this.conf);
    }

    rootTasks.add(rTask);
  }
}
