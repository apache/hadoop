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

package org.apache.hadoop.eclipse.launch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.rmi.dgc.VMID;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.server.JarModule;
import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.SubProgressMonitor;
import org.eclipse.debug.core.ILaunch;
import org.eclipse.debug.core.ILaunchConfiguration;
import org.eclipse.debug.core.model.ILaunchConfigurationDelegate;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.console.ConsolePlugin;
import org.eclipse.ui.console.IConsole;
import org.eclipse.ui.console.IOConsoleOutputStream;
import org.eclipse.ui.console.MessageConsole;
import org.eclipse.ui.console.MessageConsoleStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * Transfer a jar file and run it on the specified MapReduce server.
 */

public class StartMapReduceServer implements ILaunchConfigurationDelegate {

  private static final Logger log = Logger.getLogger(StartMapReduceServer.class
      .getName());

  private static final int SSH_FAILED_CODE = 999;

  private static final IStatus SSH_FAILED_STATUS1 = new Status(IStatus.ERROR,
      Activator.PLUGIN_ID, SSH_FAILED_CODE,
      "SSH Connection to hadoop server failed", null);

  private static final IStatus SSH_FAILED_STATUS2 = new Status(IStatus.ERROR,
      Activator.PLUGIN_ID, SSH_FAILED_CODE,
      "SSH Connection to start SCP failed", null);

  private static final IStatus SSH_FAILED_STATUS3 = new Status(IStatus.ERROR,
      Activator.PLUGIN_ID, SSH_FAILED_CODE,
      "SCP Connection to hadoop server failed", null);

  private static final int TIMEOUT = 15000;

  private Color black;

  private Color red;

  public StartMapReduceServer() {
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        black = Display.getDefault().getSystemColor(SWT.COLOR_BLACK);
        red = Display.getDefault().getSystemColor(SWT.COLOR_RED);
      }
    });
  }

  static int checkAck(InputStream in) throws IOException {
    int b = in.read();
    // b may be 0 for success,
    // 1 for error,
    // 2 for fatal error,
    // -1
    if (b == 0) {
      return b;
    }
    if (b == -1) {
      log.info("checkAck returned -1");
      return b;
    }

    if ((b == 1) || (b == 2)) {
      StringBuffer sb = new StringBuffer();
      int c;
      do {
        c = in.read();
        sb.append((char) c);
      } while (c != '\n');

      if (b == 1) { // error
        System.out.print(sb.toString());
      }
      if (b == 2) { // fatal error
        System.out.print(sb.toString());
      }
    }
    return b;
  }

  /**
   * Send the file and launch the hadoop job.
   */
  public void launch(ILaunchConfiguration configuration, String mode,
      ILaunch launch, IProgressMonitor monitor) throws CoreException {
    Map attributes = configuration.getAttributes();

    log.log(Level.FINE, "Preparing hadoop launch", configuration);

    String hostname = configuration.getAttribute("hadoop.host", "");
    int serverid = configuration.getAttribute("hadoop.serverid", 0);
    String user = configuration.getAttribute("hadoop.user", "");
    String path = configuration.getAttribute("hadoop.path", "");

    String dir = ensureTrailingSlash(path);

    log.log(Level.FINER, "Computed Server URL", new Object[] { dir, user,
        hostname });

    HadoopServer server = ServerRegistry.getInstance().getServer(serverid);

    try {
      Session session = server.createSession();
      // session.setTimeout(TIMEOUT);

      log.log(Level.FINER, "Connected");

      /*
       * COMMENTED(jz) removing server start/stop support for now if (!
       * attributes.containsKey("hadoop.jar")) { // start or stop server if(
       * server.getServerState() == IServer.STATE_STARTING ) { String command =
       * dir + "bin/start-all.sh"; execInConsole(session, command); } else if(
       * server.getServerState() == IServer.STATE_STOPPING ) { String command =
       * dir + "bin/stop-all.sh"; execInConsole(session, command); } }
       */

      if (false) {
      } else {
        FileInputStream fis = null;
        String jarFile, remoteFile = null;

        if (attributes.containsKey("hadoop.jar")) {
          jarFile = (String) attributes.get("hadoop.jar");
        } else {
          String memento = (String) attributes.get("hadoop.jarrable");
          JarModule fromMemento = JarModule.fromMemento(memento);
          jarFile = fromMemento.buildJar(new SubProgressMonitor(monitor, 100))
              .toString();
        }

        if (jarFile.lastIndexOf('/') > 0) {
          remoteFile = jarFile.substring(jarFile.lastIndexOf('/') + 1);
        } else if (jarFile.lastIndexOf('\\') > 0) {
          remoteFile = jarFile.substring(jarFile.lastIndexOf('\\') + 1);
        }

        // exec 'scp -t -p hadoop.jar' remotely

        String command = "scp -p -t " + remoteFile;
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);

        // get I/O streams for remote scp
        OutputStream out = channel.getOutputStream();
        final InputStream in = channel.getInputStream();

        channel.connect();

        if (checkAck(in) != 0) {
          throw new CoreException(SSH_FAILED_STATUS1);
        }

        // send "C0644 filesize filename", where filename should not
        // include '/'
        long filesize = (new File(jarFile)).length();
        command = "C0644 " + filesize + " ";
        if (jarFile.lastIndexOf('/') > 0) {
          command += jarFile.substring(jarFile.lastIndexOf('/') + 1);
        } else {
          command += jarFile;
        }

        command += "\n";
        out.write(command.getBytes());
        out.flush();
        if (checkAck(in) != 0) {
          throw new CoreException(SSH_FAILED_STATUS2);
        }

        // send a content of jarFile
        fis = new FileInputStream(jarFile);
        byte[] buf = new byte[1024];
        while (true) {
          int len = fis.read(buf, 0, buf.length);
          if (len <= 0) {
            break;
          }
          out.write(buf, 0, len); // out.flush();
        }

        fis.close();
        fis = null;
        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();
        if (checkAck(in) != 0) {
          throw new CoreException(SSH_FAILED_STATUS3);
        }
        out.close();
        channel.disconnect();

        // move the jar file to a temp directory
        String jarDir = "/tmp/hadoopjar"
            + new VMID().toString().replace(':', '_');
        command = "mkdir " + jarDir + ";mv " + remoteFile + " " + jarDir;
        channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);
        channel.connect();
        channel.disconnect();

        session.disconnect();

        // we create a new session with a zero timeout to prevent the
        // console stream
        // from stalling -- eyhung
        final Session session2 = server.createSessionNoTimeout();

        // now remotely execute hadoop with the just sent-over jarfile
        command = dir + "bin/hadoop jar " + jarDir + "/" + remoteFile;
        log.fine("Running command: " + command);
        execInConsole(session2, command, jarDir + "/" + remoteFile);

        // the jar file is not deleted anymore, but placed in a temp dir
        // -- eyhung
      }
    } catch (final JSchException e) {
      e.printStackTrace();
      Display.getDefault().syncExec(new Runnable() {
        public void run() {
          MessageDialog.openError(Display.getDefault().getActiveShell(),
              "Problems connecting to MapReduce Server", 
              e.getLocalizedMessage());
        }
      });
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Show the job output in the console.
   * @param session The SSH session object
   * @param command The command to run remotely
   * @param jarFile The jar file containing the classes for the Hadoop job
   * @throws JSchException
   */
  private void execInConsole(final Session session, final String command,
      final String jarFile) throws JSchException {
    final ChannelExec channel = (ChannelExec) session.openChannel("exec");

    final MessageConsole console = new MessageConsole("Hadoop: " + command,
        null);
    final MessageConsoleStream stream = console.newMessageStream();

    final IOConsoleOutputStream out = console.newOutputStream();
    final IOConsoleOutputStream err = console.newOutputStream();

    out.setColor(black);
    err.setColor(red);

    ConsolePlugin.getDefault().getConsoleManager().addConsoles(
        new IConsole[] { console });
    ConsolePlugin.getDefault().getConsoleManager().showConsoleView(console);

    channel.setCommand(command);
    channel.setInputStream(null);

    channel.connect();
    new Thread() {
      @Override
      public void run() {
        try {

          BufferedReader hadoopOutput = new BufferedReader(
              new InputStreamReader(channel.getInputStream()));

          String stdoutLine;
          while ((stdoutLine = hadoopOutput.readLine()) != null) {
            out.write(stdoutLine);
            out.write('\n');
            continue;
          }

          channel.disconnect();

          // meaningless call meant to prevent console from being
          // garbage collected -- eyhung
          console.getName();
          ChannelExec channel2 = (ChannelExec) session.openChannel("exec");
          channel2.setCommand("rm -rf "
              + jarFile.substring(0, jarFile.lastIndexOf("/")));
          log.fine("Removing temp file "
              + jarFile.substring(0, jarFile.lastIndexOf("/")));
          channel2.connect();
          channel2.disconnect();

        } catch (Exception e) {
        }
      }
    }.start();

    new Thread() {
      @Override
      public void run() {
        try {

          BufferedReader hadoopErr = new BufferedReader(new InputStreamReader(
              channel.getErrStream()));

          // String stdoutLine;
          String stderrLine;
          while ((stderrLine = hadoopErr.readLine()) != null) {
            err.write(stderrLine);
            err.write('\n');
            continue;
          }

        } catch (Exception e) {
        }
      }
    }.start();

  }

  private String ensureTrailingSlash(String dir) {
    if (!dir.endsWith("/")) {
      dir += "/";
    }
    return dir;
  }

}
