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

package org.apache.hadoop.hive.ql;

import java.io.File;
import java.io.DataInput;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.StringUtils;
import java.util.Random;

public class Context {
  private Path resFile;
  private Path resDir;
  private FileSystem fs;
  static final private Log LOG = LogFactory.getLog("hive.ql.Context");
  private Path[] resDirPaths;
  private int    resDirFilesNum;
  boolean initialized;
  private String scratchDir;
  private HiveConf conf;
  
  public Context(HiveConf conf) {
    try {
      this.conf = conf;
      fs = FileSystem.get(conf);
      initialized = false;
      resDir = null;
      resFile = null;
    } catch (IOException e) {
      LOG.info("Context creation error: " + StringUtils.stringifyException(e));
    }
  }

  public void makeScratchDir() throws Exception {
    Random rand = new Random();
    int randomid = Math.abs(rand.nextInt()%rand.nextInt());
    scratchDir = conf.getVar(HiveConf.ConfVars.SCRATCHDIR) + File.separator + randomid;
    Path tmpdir = new Path(scratchDir);
    fs.mkdirs(tmpdir);
  }

  public String getScratchDir() {
    return scratchDir;
  }

  public void removeScratchDir() throws Exception {
    Path tmpdir = new Path(scratchDir);
    fs.delete(tmpdir, true);
  }

  /**
   * @return the resFile
   */
  public Path getResFile() {
    return resFile;
  }

  /**
   * @param resFile the resFile to set
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
    resDir = null;
    resDirPaths = null;
    resDirFilesNum = 0;
  }

  /**
   * @return the resDir
   */
  public Path getResDir() {
    return resDir;
  }

  /**
   * @param resDir the resDir to set
   */
  public void setResDir(Path resDir) {
    this.resDir = resDir;
    resFile = null;

    resDirFilesNum = 0;
    resDirPaths = null;
  }  
  
  public void clear() {
    initialized = false;
    if (resDir != null)
    {
      try
      {
        fs.delete(resDir, true);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }

    if (resFile != null)
    {
      try
      {
      	fs.delete(resFile, false);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }

    resDir = null;
    resFile = null;
    resDirFilesNum = 0;
    resDirPaths = null;
  }

  public DataInput getStream() {
    try
    {
      if (!initialized) {
        initialized = true;
        if ((resFile == null) && (resDir == null)) return null;
      
        if (resFile != null)
          return (DataInput)fs.open(resFile);
        
        FileStatus status = fs.getFileStatus(resDir);
        assert status.isDir();
        FileStatus[] resDirFS = fs.globStatus(new Path(resDir + "/*"));
        resDirPaths = new Path[resDirFS.length];
        int pos = 0;
        for (FileStatus resFS: resDirFS)
          if (!resFS.isDir())
            resDirPaths[pos++] = resFS.getPath();
        if (pos == 0) return null;
        
        return (DataInput)fs.open(resDirPaths[resDirFilesNum++]);
      }
      else {
        return getNextStream();
      }
    } catch (FileNotFoundException e) {
      LOG.info("getStream error: " + StringUtils.stringifyException(e));
      return null;
    } catch (IOException e) {
      LOG.info("getStream error: " + StringUtils.stringifyException(e));
      return null;
    }
  }

  private DataInput getNextStream() {
    try
    {
      if (resDir != null && resDirFilesNum < resDirPaths.length && 
          (resDirPaths[resDirFilesNum] != null))
        return (DataInput)fs.open(resDirPaths[resDirFilesNum++]);
    } catch (FileNotFoundException e) {
      LOG.info("getNextStream error: " + StringUtils.stringifyException(e));
      return null;
    } catch (IOException e) {
      LOG.info("getNextStream error: " + StringUtils.stringifyException(e));
      return null;
    }
    
    return null;
  }
}

