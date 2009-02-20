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

package org.apache.hadoop.hive.ant;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.Project;

import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;

public class QTestGenTask extends Task {

  public class QFileFilter implements FileFilter {
  
    public boolean accept(File fpath) {
      if (fpath.isDirectory() ||
          !fpath.getName().endsWith(".q")) {
        return false;
      }
      return true;
    }
    
  }

  protected String templatePath;

  protected String outputDirectory;
 
  protected String queryDirectory;
 
  protected String queryFile;
 
  protected String resultsDirectory;

  protected String template;

  protected String className;

  protected String logFile;

  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public void setTemplate(String template) {
    this.template = template;
  }

  public String getTemplate() {
    return template;
  }

  public void setTemplatePath(String templatePath) throws Exception {
    StringBuffer resolvedPath = new StringBuffer();
    StringTokenizer st = new StringTokenizer(templatePath, ",");
    while (st.hasMoreTokens()) {
      // resolve relative path from basedir and leave
      // absolute path untouched.
      File fullPath = project.resolveFile(st.nextToken());
      resolvedPath.append(fullPath.getCanonicalPath());
      if (st.hasMoreTokens()) {
        resolvedPath.append(",");
      }
    }
    this.templatePath = resolvedPath.toString();
    System.out.println("Template Path:" + this.templatePath);
  }

  public String getTemplatePath() {
    return templatePath;
  }

  public void setOutputDirectory(File outputDirectory) {
    try {
      this.outputDirectory = outputDirectory.getCanonicalPath();
    }
    catch (IOException ioe) {
      throw new BuildException(ioe);
    }
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setResultsDirectory(String resultsDirectory) {
    this.resultsDirectory = resultsDirectory;
  }

  public String getResultsDirectory() {
    return this.resultsDirectory;
  }

  public void setQueryDirectory(String queryDirectory) {
    this.queryDirectory = queryDirectory;
  }

  public String getQueryDirectory() {
    return this.queryDirectory;
  }

  public void setQueryFile(String queryFile) {
    this.queryFile = queryFile;
  }

  public String getQueryFile() {
    return this.queryFile;
  }

  /**
   * Invoke {@link org.apache.hadoop.fs.FsShell#doMain FsShell.doMain} after a
   * few cursory checks of the configuration.
   */
  public void execute() throws BuildException {

    if (templatePath == null) {
      throw new BuildException("No templatePath attribute specified");
    }

    if (template == null) {
      throw new BuildException("No template attribute specified");
    }

    if (outputDirectory == null) {
      throw new BuildException("No outputDirectory specified");
    }

    if (queryDirectory == null && queryFile == null ) {
      throw new BuildException("No queryDirectory or queryFile specified");
    }

    if (resultsDirectory == null) {
      throw new BuildException("No resultsDirectory specified");
    }

    if (className == null) {
      throw new BuildException("No className specified");
    }

    File [] qFiles = null;
    File outDir = null;
    File resultsDir = null;
    
    try {
      File inpDir = null;
      if (queryDirectory != null) {
        inpDir = new File(queryDirectory);
      }

      if (queryFile != null && !queryFile.equals("")) {
        qFiles = new File[1];
        qFiles[0] = inpDir != null ? new File(inpDir, queryFile) : new File(queryFile);
      }
      else {
        qFiles = inpDir.listFiles(new QFileFilter());
      }

      // Make sure the output directory exists, if it doesn't
      // then create it.
      outDir = new File(outputDirectory);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }
      
      resultsDir = new File(resultsDirectory);
      if (!resultsDir.exists()) {
        throw new BuildException("Results Directory " + resultsDir.getCanonicalPath() + " does not exist");
      }
    }
    catch (Exception e) {
      throw new BuildException(e);
    }
    
    VelocityEngine ve = new VelocityEngine();

    try {
      ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, templatePath);
      if (logFile != null) {
        File lf = new File(logFile);
        if (lf.exists()) {
          if (!lf.delete()) {
            throw new Exception("Could not delete log file " + lf.getCanonicalPath());
          }
        }

        ve.setProperty(RuntimeConstants.RUNTIME_LOG, logFile);
      }

      ve.init();
      Template t = ve.getTemplate(template);

      // For each of the qFiles generate the test
      VelocityContext ctx = new VelocityContext();
      ctx.put("className", className);
      ctx.put("qfiles", qFiles);
      ctx.put("resultsDir", resultsDir);

      File outFile = new File(outDir, className + ".java");
      FileWriter writer = new FileWriter(outFile);
      t.merge(ctx, writer);
      writer.close();

      System.out.println("Generated " + outFile.getCanonicalPath() + " from template " + template);
    }
    catch(BuildException e) {
      throw e;
    }
    catch(MethodInvocationException e) {
      throw new BuildException("Exception thrown by '" + e.getReferenceName() + "." +
                               e.getMethodName() +"'",
                               e.getWrappedThrowable());
    }
    catch(ParseErrorException e) {
      throw new BuildException("Velocity syntax error", e);
    }
    catch(ResourceNotFoundException e) {
      throw new BuildException("Resource not found", e);
    }
    catch(Exception e) {
      throw new BuildException("Generation failed", e);
    }
  }
}
