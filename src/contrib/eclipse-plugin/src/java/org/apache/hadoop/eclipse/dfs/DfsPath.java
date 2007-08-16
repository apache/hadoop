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
package org.apache.hadoop.eclipse.dfs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.eclipse.core.runtime.IAdaptable;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.jface.viewers.StructuredViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.widgets.Display;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * DFS Path handling for DFS
 */
public class DfsPath implements IAdaptable {

  private Session shell;

  private HadoopServer location;

  private String path;

  private final Viewer viewer;

  private DfsPath parent;

  static Logger log = Logger.getLogger(DfsPath.class.getName());

  public DfsPath(HadoopServer location, String path, Viewer viewer) {
    this.location = location;
    this.path = path;
    this.viewer = viewer;
  }

  protected String getPath() {
    return this.path;
  }

  protected ChannelExec exec(String command) throws JSchException {
    ChannelExec channel = (ChannelExec) getSession().openChannel("exec");
    channel.setCommand(location.getInstallPath() + "/bin/hadoop " + command);
    channel.setErrStream(System.err);
    // channel.connect();

    return channel;
  }

  protected DfsPath(HadoopServer location, String path, Session shell,
      Viewer viewer) {
    this(location, path, viewer);

    this.shell = shell;
  }

  protected DfsPath(DfsPath parent, String path) {
    this(parent.location, path, parent.shell, parent.viewer);
    this.parent = parent;
  }

  protected Session getSession() throws JSchException {
    if (shell == null) {
      // this.shell =
      // JSchUtilities.createJSch().getSession(location.getUser(),
      // location.getHostname());
      this.shell = location.createSession();
    }

    if (!shell.isConnected()) {
      shell.connect();
    }

    return shell;
  }

  protected void dispose() {
    if ((this.shell != null) && this.shell.isConnected()) {
      shell.disconnect();
    }
  }

  @Override
  public String toString() {
    if ((path.length() < 1) || path.equals("/")) {
      return "DFS @ " + location.getName();
    } else {
      String[] parts = path.split("/");
      return parts[parts.length - 1];
    }
  }

  protected void doExec(final String command) {
    org.eclipse.core.runtime.jobs.Job job =
        new org.eclipse.core.runtime.jobs.Job("DFS operation: " + command) {
          @Override
          protected IStatus run(IProgressMonitor monitor) {
            ChannelExec exec = null;
            monitor.beginTask("Execute remote dfs  command", 100);
            try {
              exec = exec(" " + command);
              monitor.worked(33);

              exec.connect();
              monitor.worked(33);

              BufferedReader reader =
                  new BufferedReader(new InputStreamReader(
                      new BufferedInputStream(exec.getInputStream())));
              String response = reader.readLine(); // TIDY(jz)
              monitor.worked(34);

              monitor.done();

              refresh();

              return Status.OK_STATUS;
            } catch (Exception e) {
              e.printStackTrace();
              return new Status(IStatus.ERROR, Activator.PLUGIN_ID, -1,
                  "DFS operation failed: " + e.getLocalizedMessage(), e);
            } finally {
              if (exec != null) {
                exec.disconnect();
              }
            }
          }
        };

    job.setUser(true);
    job.schedule();
  }

  public void delete() throws JSchException {
    doExec("dfs " + DfsFolder.s_whichFS + " -rm " + path);
  }

  public Object getParent() {
    return parent;
  }

  public void refresh() {
    if (parent != null) {
      parent.doRefresh();
    } else {
      doRefresh();
    }
  }

  protected void doRefresh() {
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        ((StructuredViewer) viewer).refresh(DfsPath.this);
      }
    });
  }

  public Object getAdapter(Class type) {
    log.fine(type.toString());
    return null;
  }

  /**
   * Copy the DfsPath to the given local directory
   * 
   * @param directory the local directory
   */
  public void downloadToLocalDirectory(String directory)
      throws InvocationTargetException, JSchException, InterruptedException,
      IOException {

    // Not implemented here; by default, do nothing
  }

}
