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

package org.apache.hadoop.eclipse.servers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.eclipse.server.HadoopServer;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.IMessageProvider;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * Wizard for defining the location of a Hadoop server
 */

public class DefineHadoopServerLocWizardPage extends WizardPage {

  /**
   * User-defined name for the hadoop server
   */
  private Text serverName;

  /**
   * Host name of the Hadoop server
   */
  private Text hostName;

  /**
   * Location of Hadoop jars on the server
   */
  private Text installPath;

  /**
   * User name to use to connect to the Hadoop server
   */
  private Text userName;

  private Button useSSHTunnel;

  /**
   * User name to use to connect to the tunneling machine
   */
  private Text tunnelUserName;

  /**
   * Host name of the tunneling machine
   */
  private Text tunnelHostName;

  /**
   * HadoopServer instance currently edited by this form (null if we create a
   * new one)
   */
  private HadoopServer editedServer;

  /**
   * Constructor to create a new Hadoop server
   */
  public DefineHadoopServerLocWizardPage() {
    super("Hadoop Server", "Define Hadoop Server Location", null);
    this.editedServer = null;
  }

  /**
   * Constructor to edit the parameters of an existing Hadoop server
   * 
   * @param server
   */
  public DefineHadoopServerLocWizardPage(HadoopServer server) {
    super("Hadoop Server", "Edit Hadoop Server Location", null);
    this.editedServer = server;
  }

  /**
   * Fill the server values from the form values
   * 
   * @return
   */
  private HadoopServer defineServerFromValues() {
    String uri =
        userName.getText() + "@" + hostName.getText() + ":"
            + installPath.getText();

    if (editedServer == null) {
      // Create and register the new HadoopServer
      this.editedServer =
          new HadoopServer(uri, serverName.getText(), (useSSHTunnel
              .getSelection()) ? tunnelHostName.getText() : null,
              (useSSHTunnel.getSelection()) ? tunnelUserName.getText()
                  : null);
      ServerRegistry.getInstance().addServer(this.editedServer);

    } else {

      // Update values of the already existing HadoopServer
      editedServer.setName(this.serverName.getText());
      editedServer.setURI(uri);
      if (useSSHTunnel.getSelection())
        editedServer.setTunnel(tunnelHostName.getText(), tunnelUserName
            .getText());
      else
        editedServer.setTunnel(null, null);

      ServerRegistry.getInstance().stateChanged(this.editedServer);
    }

    return this.editedServer;
  }

  /**
   * Fill the form values from the server instance
   */
  private void defineValuesFromServer() {
    if (this.editedServer == null) {
      // Setup values for a new empty instance
      // Do nothing as it may trigger listeners!!!
      /*
       * serverName.setText(""); userName.setText(""); hostName.setText("");
       * installPath.setText(""); useSSHTunnel.setSelection(false);
       * tunnelHostName.setText(""); tunnelUserName.setText("");
       */
    } else {
      // Setup values from the server instance
      serverName.setText(editedServer.getName());
      userName.setText(editedServer.getUserName());
      hostName.setText(editedServer.getHostName());
      installPath.setText(editedServer.getInstallPath());
      if (editedServer.useTunneling()) {
        useSSHTunnel.setSelection(true);
        tunnelHostName.setText(editedServer.getTunnelHostName());
        tunnelUserName.setText(editedServer.getTunnelUserName());
      } else {
        useSSHTunnel.setSelection(false);
        tunnelHostName.setText("");
        tunnelUserName.setText("");
      }
    }
  }

  /**
   * Performs any actions appropriate in response to the user having pressed
   * the Finish button, or refuse if finishing now is not permitted.
   * 
   * @return Object containing information about the Hadoop server
   */
  public HadoopServer performFinish() {
    try {
      return defineServerFromValues();
    } catch (Exception e) {
      e.printStackTrace();
      setMessage("Invalid server location values", IMessageProvider.ERROR);
      return null;
    }
  }

  /**
   * Validates whether Hadoop exists at the specified server location
   * 
   */
  private void testLocation() {
    ProgressMonitorDialog dialog = new ProgressMonitorDialog(getShell());
    dialog.setOpenOnRun(true);

    try {
      final HadoopServer location = defineServerFromValues();

      try {
        dialog.run(true, false, new IRunnableWithProgress() {
          public void run(IProgressMonitor monitor)
              throws InvocationTargetException, InterruptedException {
            Session session = null;
            try {
              session = location.createSession();
              try {
                ChannelExec channel =
                    (ChannelExec) session.openChannel("exec");
                channel.setCommand(location.getInstallPath()
                    + "/bin/hadoop version");
                BufferedReader response =
                    new BufferedReader(new InputStreamReader(channel
                        .getInputStream()));
                channel.connect();
                final String versionLine = response.readLine();

                if ((versionLine != null)
                    && versionLine.startsWith("Hadoop")) {
                  Display.getDefault().syncExec(new Runnable() {
                    public void run() {
                      setMessage("Found " + versionLine,
                          IMessageProvider.INFORMATION);
                    }
                  });
                } else {
                  Display.getDefault().syncExec(new Runnable() {
                    public void run() {
                      setMessage("No Hadoop Found in this location",
                          IMessageProvider.WARNING);
                    }
                  });
                }
              } finally {
                session.disconnect();
                location.dispose();
              }
            } catch (final JSchException e) {
              Display.getDefault().syncExec(new Runnable() {
                public void run() {
                  System.err.println(e.getMessage());
                  setMessage("Problems connecting to server: "
                      + e.getLocalizedMessage(), IMessageProvider.WARNING);
                }
              });
            } catch (final IOException e) {
              Display.getDefault().syncExec(new Runnable() {
                public void run() {
                  setMessage("Problems communicating with server: "
                      + e.getLocalizedMessage(), IMessageProvider.WARNING);
                }
              });
            } catch (final Exception e) {
              Display.getDefault().syncExec(new Runnable() {
                public void run() {
                  setMessage("Errors encountered connecting to server: "
                      + e.getLocalizedMessage(), IMessageProvider.WARNING);
                }
              });
            } finally {
              if (session != null) {
                session.disconnect();
              }
            }
          }

        });
      } catch (InvocationTargetException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (Exception e) {
      setMessage("Invalid server location", IMessageProvider.WARNING);
      return;
    }
  }

  @Override
  /**
   * Location is not complete (and finish button not available) until a
   * hostname is specified.
   */
  public boolean isPageComplete() {
    if (hostName.getText().length() > 0) {
      return true;
    } else {
      return false;
    }
  }

  public boolean canFinish() {
    return isPageComplete();
  }

  /**
   * Create the overall wizard
   */
  public void createControl(Composite parent) {

    setTitle("Define Hadoop Server Location");
    setDescription("Define the location of a Hadoop server for running MapReduce applications.");
    Composite panel = new Composite(parent, SWT.NONE);
    GridLayout layout = new GridLayout();
    layout.numColumns = 3;
    layout.makeColumnsEqualWidth = false;
    panel.setLayout(layout);
    panel.setLayoutData(new GridData(GridData.FILL_BOTH));

    Label serverNameLabel = new Label(panel, SWT.NONE);
    serverNameLabel.setText("&Server name:");

    serverName = new Text(panel, SWT.SINGLE | SWT.BORDER);
    GridData data = new GridData(GridData.FILL_HORIZONTAL);
    serverName.setLayoutData(data);
    serverName.setText("Hadoop Server");

    new Label(panel, SWT.NONE).setText(" ");

    // serverName.addModifyListener(this);

    Label hostNameLabel = new Label(panel, SWT.NONE);
    hostNameLabel.setText("&Hostname:");

    hostName = new Text(panel, SWT.SINGLE | SWT.BORDER);
    hostName.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    hostName.addListener(SWT.Modify, new Listener() {
      public void handleEvent(Event e) {
        refreshButtons();
      }

      public void widgetDefaultSelected(SelectionEvent e) {
      }
    });

    // COMMENTED(jz) seems to cause issues on my version of eclipse
    // hostName.setText(server.getHost());

    // hostName.addModifyListener(this);

    new Label(panel, SWT.NONE).setText(" ");

    Label installationPathLabel = new Label(panel, SWT.NONE);
    installationPathLabel.setText("&Installation directory:");

    installPath = new Text(panel, SWT.SINGLE | SWT.BORDER);
    installPath.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

    // installationPath.addModifyListener(this);

    new Label(panel, SWT.NONE).setText(" ");

    Label usernameLabel = new Label(panel, SWT.NONE);
    usernameLabel.setText("&Username:");

    // new Label(panel, SWT.NONE).setText(" ");

    userName = new Text(panel, SWT.SINGLE | SWT.BORDER);
    userName.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    // username.addModifyListener(this);

    Label spacer = new Label(panel, SWT.NONE);
    spacer.setText(" ");

    Label spacer2 = new Label(panel, SWT.NONE);
    spacer2.setText(" ");

    /*
     * Label label = new Label(panel, SWT.NONE); GridData data2 = new
     * GridData(); data2.horizontalSpan = 2; label.setLayoutData(data2);
     * label.setText("Example: user@host:/path/to/hadoop");
     */

    Group sshTunnelGroup = new Group(panel, SWT.NONE);
    GridData gridData = new GridData(GridData.FILL_HORIZONTAL);
    gridData.horizontalSpan = 3;
    sshTunnelGroup.setLayoutData(gridData);
    sshTunnelGroup.setLayout(new GridLayout(2, false));
    useSSHTunnel = new Button(sshTunnelGroup, SWT.CHECK);
    useSSHTunnel.setText("Tunnel Connections");
    GridData span2 = new GridData(GridData.FILL_HORIZONTAL);
    span2.horizontalSpan = 2;
    useSSHTunnel.setLayoutData(span2);
    Label label = new Label(sshTunnelGroup, SWT.NONE);
    label.setText("Tunnel via");
    tunnelHostName = new Text(sshTunnelGroup, SWT.BORDER | SWT.SINGLE);
    tunnelHostName.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

    Label label2 = new Label(sshTunnelGroup, SWT.NONE);
    label2.setText("Tunnel username");
    tunnelUserName = new Text(sshTunnelGroup, SWT.BORDER | SWT.SINGLE);
    tunnelUserName.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

    Listener refreshButtonsListener = new Listener() {
      public void handleEvent(Event event) {
        refreshButtons();
      }
    };
    useSSHTunnel.addListener(SWT.Selection, refreshButtonsListener);
    tunnelHostName.setEnabled(useSSHTunnel.getSelection());
    tunnelUserName.setEnabled(useSSHTunnel.getSelection());

    ((GridLayout) sshTunnelGroup.getLayout()).marginBottom = 20;

    Label label4 = new Label(panel, SWT.NONE);
    GridData span3 = new GridData(GridData.FILL_HORIZONTAL);
    span3.horizontalSpan = 3;
    label4.setLayoutData(span3);

    final Button validate = new Button(panel, SWT.NONE);
    validate.setText("&Validate location");
    validate.addListener(SWT.Selection, new Listener() {
      public void handleEvent(Event e) {
        testLocation();
      }

      public void widgetDefaultSelected(SelectionEvent e) {
      }
    });

    new Label(panel, SWT.NONE).setText(" ");

    setControl(panel);

    defineValuesFromServer();
  }

  public void refreshButtons() {
    if (useSSHTunnel == null)
      return;

    if (tunnelHostName != null)
      tunnelHostName.setEnabled(useSSHTunnel.getSelection());
    if (tunnelUserName != null)
      tunnelUserName.setEnabled(useSSHTunnel.getSelection());

    getContainer().updateButtons();
  }
}
