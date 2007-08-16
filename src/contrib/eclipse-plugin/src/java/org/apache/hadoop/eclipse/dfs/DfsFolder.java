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
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.rmi.dgc.VMID;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.SubProgressMonitor;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.PlatformUI;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

/**
 * Folder handling methods for the DFS
 */

public class DfsFolder extends DfsPath {

  protected final static String s_whichFS = ""; // "-fs local";

  static Logger log = Logger.getLogger(DfsFolder.class.getName());

  private Object[] children;

  private boolean loading = false;

  protected DfsFolder(HadoopServer location, String path, Viewer viewer) {
    super(location, path, viewer);
  }

  private DfsFolder(DfsPath parent, String path) {
    super(parent, path);
  }

  public Object[] getChildren() {
    ChannelExec channel = null;
    if (children == null) {
      doRefresh();
      return new Object[] { "Loading..." };
    } else {
      return children;
    }
  }

  @Override
  /**
   * Forces a refresh of the items in the current DFS node
   */
  public void doRefresh() {
    new Job("Refresh DFS Children") {
      @Override
      protected IStatus run(IProgressMonitor monitor) {
        try {
          ChannelExec channel =
              exec(" dfs " + s_whichFS + " -ls " + getPath());
          InputStream is = channel.getInputStream();
          BufferedReader in =
              new BufferedReader(new InputStreamReader(
                  new BufferedInputStream(is)));

          if (!channel.isConnected()) {
            channel.connect();
          }

          try {
            // initial "found n items" line ignorable
            if (in.readLine() == null) {
              children =
                  new Object[] { "An error occurred: empty result from dfs -ls" };
            }

            String line;
            List<DfsPath> children = new ArrayList<DfsPath>();
            while ((line = in.readLine()) != null) {
              String[] parts = line.split("\t");

              for (int i = 0; i < parts.length; i++) {
                log.fine(parts[0]);
              }

              if (parts[1].equals("<dir>")) {
                children.add(new DfsFolder(DfsFolder.this, parts[0]));
              } else {
                children.add(new DfsFile(DfsFolder.this, parts[0]));
              }
            }

            DfsFolder.this.children = children.toArray();

            DfsFolder.super.doRefresh();

            return Status.OK_STATUS;
          } finally {
            if (channel.isConnected()) {
              channel.disconnect();
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          return new Status(IStatus.ERROR, Activator.PLUGIN_ID, -1,
              "Refreshing DFS node failed: " + e.getLocalizedMessage(), e);
        }
      }
    }.schedule();
  }

  @Override
  /**
   * Does a recursive delete of the remote directory tree at this node.
   */
  public void delete() throws JSchException {
    doExec("dfs " + s_whichFS + " -rmr " + getPath());
  }

  /**
   * Upload a local directory and its contents to the remote DFS
   * 
   * @param directory source directory to upload
   * @throws SftpException
   * @throws JSchException
   * @throws InvocationTargetException
   * @throws InterruptedException
   */
  public void put(final String directory) throws SftpException,
      JSchException, InvocationTargetException, InterruptedException {
    ProgressMonitorDialog progress =
        new ProgressMonitorDialog((Display.getCurrent() == null) ? null
            : Display.getCurrent().getActiveShell());
    progress.setCancelable(true);

    PlatformUI.getWorkbench().getProgressService().busyCursorWhile(
        new IRunnableWithProgress() {
          public void run(IProgressMonitor monitor)
              throws InvocationTargetException, InterruptedException {
            String guid = new VMID().toString().replace(':', '_');

            monitor.beginTask("Secure Copy", 100);
            scp(directory, "/tmp/hadoop_scp_" + guid,
                new SubProgressMonitor(monitor, 60));

            try {
              SubProgressMonitor sub = new SubProgressMonitor(monitor, 1);
              if (monitor.isCanceled()) {
                return;
              }

              final File dir = new File(directory);

              sub.beginTask("Move files from staging server to DFS", 1);
              ChannelExec exec =
                  exec(" dfs " + s_whichFS
                      + " -moveFromLocal /tmp/hadoop_scp_" + guid + " \""
                      + getPath() + "/" + dir.getName() + "\"");
              BufferedReader reader =
                  new BufferedReader(new InputStreamReader(
                      new BufferedInputStream(exec.getInputStream())));

              if (!monitor.isCanceled()) {
                exec.connect();
                String line = reader.readLine();
                sub.worked(1);
              }

              if (exec.isConnected()) {
                exec.disconnect();
              }

              sub.done();

              monitor.done();
              doRefresh();
            } catch (Exception e) {
              log.log(Level.SEVERE, "", e);
              throw new InvocationTargetException(e);
            }
          }

          public void scp(String from, String to, IProgressMonitor monitor) {
            File file = new File(from);
            ChannelExec channel = null;

            monitor.beginTask("scp from " + from + " to " + to, 100 * (file
                .isDirectory() ? file.list().length + 1 : 1));

            if (monitor.isCanceled()) {
              return;
            }

            if (file.isDirectory()) {
              // mkdir
              try {
                channel = (ChannelExec) getSession().openChannel("exec");
                channel.setCommand(" mkdir " + to);
                InputStream in = channel.getInputStream();
                channel.connect();
                // in.read(); // wait for a response, which
                // we'll then ignore
              } catch (JSchException e) {
                // BUG(jz) abort operation and display error
                throw new RuntimeException(e);
              } catch (IOException e) {
                throw new RuntimeException(e);
              } finally {
                if (channel.isConnected()) {
                  channel.disconnect();
                }
              }

              monitor.worked(100);

              String[] children = file.list();
              for (int i = 0; i < children.length; i++) {
                File child = new File(file, children[i]);

                // recurse
                scp(new File(file, children[i]).getAbsolutePath(), to + "/"
                    + children[i], new SubProgressMonitor(monitor, 100));
              }
            } else {
              InputStream filein = null;

              try {
                channel = (ChannelExec) getSession().openChannel("exec");
                (channel).setCommand("scp -p -t " + to);
                BufferedOutputStream out =
                    new BufferedOutputStream(channel.getOutputStream());
                InputStream in = channel.getInputStream();
                channel.connect();

                if (in.read() == 0) {
                  int step = (int) (100 / new File(from).length());
                  out.write(("C0644 " + new File(from).length() + " "
                      + new File(to).getName() + "\n").getBytes());
                  out.flush();
                  if (in.read() != 0) {
                    throw new RuntimeException("Copy failed");
                  }

                  filein =
                      new BufferedInputStream(new FileInputStream(from));

                  byte[] buffer = new byte[1024];
                  int bytes;
                  while ((bytes = filein.read(buffer)) > -1) {
                    if (monitor.isCanceled()) {
                      return;
                    }

                    out.write(buffer, 0, bytes);
                    monitor.worked(step);
                  }

                  out.write("\0".getBytes());
                  out.flush();

                  if (in.read() != 0) {
                    throw new RuntimeException("Copy failed");
                  }
                  out.close();
                } else {
                  // problems with copy
                  throw new RuntimeException("Copy failed");
                }
              } catch (JSchException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              } catch (IOException e) {
                throw new RuntimeException(e);
              } finally {
                if (channel.isConnected()) {
                  channel.disconnect();
                }
                try {
                  filein.close();
                } catch (IOException e) {
                }
              }
            }

            monitor.done();
          }
        });
  }
}
