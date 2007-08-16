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

package org.apache.hadoop.eclipse.view.servers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.actions.EditServerAction;
import org.apache.hadoop.eclipse.actions.NewServerAction;
import org.apache.hadoop.eclipse.server.HadoopJob;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.server.IJobListener;
import org.apache.hadoop.eclipse.server.JarModule;
import org.apache.hadoop.eclipse.servers.IHadoopServerListener;
import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.debug.internal.ui.DebugPluginImages;
import org.eclipse.debug.ui.IDebugUIConstants;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.viewers.IContentProvider;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.ITreeSelection;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.ui.IViewSite;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.actions.ActionFactory;
import org.eclipse.ui.part.ViewPart;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * Code for displaying/updating the MapReduce Servers view panel
 */
public class ServerView extends ViewPart implements IContentProvider,
    IStructuredContentProvider, ITreeContentProvider, ITableLabelProvider,
    IJobListener, IHadoopServerListener {

  /**
   * This object is the root content for this content provider
   */
  private static final Object CONTENT_ROOT = new Object();

  private final IAction DELETE = new DeleteAction();

  private final IAction PROPERTIES = new EditServerAction(this);

  private final IAction NEWSERVER = new NewServerAction();

  private Map<String, Image> images = new HashMap<String, Image>();

  private TreeViewer viewer;

  public ServerView() {
  }

  /* @inheritDoc */
  @Override
  public void init(IViewSite site) throws PartInitException {
    super.init(site);

    try {
      images.put("hadoop", ImageDescriptor.createFromURL(
          (FileLocator.toFileURL(FileLocator.find(Activator.getDefault()
              .getBundle(), new Path("resources/hadoop_small.gif"), null))))
          .createImage(true));
      images.put("job", ImageDescriptor.createFromURL(
          (FileLocator.toFileURL(FileLocator.find(Activator.getDefault()
              .getBundle(), new Path("resources/job.gif"), null))))
          .createImage(true));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /* @inheritDoc */
  @Override
  public void dispose() {
    for (String key : images.keySet()) {
      if (images.containsKey(key))
        ((Image) images.get(key)).dispose();
    }

    ServerRegistry.getInstance().removeListener(this);

    images.clear();
  }

  /**
   * Creates the columns for the view
   */
  @Override
  public void createPartControl(Composite parent) {
    Tree main =
        new Tree(parent, SWT.SINGLE | SWT.FULL_SELECTION | SWT.H_SCROLL
            | SWT.V_SCROLL);
    main.setHeaderVisible(true);
    main.setLinesVisible(false);
    main.setLayoutData(new GridData(GridData.FILL_BOTH));

    TreeColumn serverCol = new TreeColumn(main, SWT.SINGLE);
    serverCol.setText("Server");
    serverCol.setWidth(185);
    serverCol.setResizable(true);

    TreeColumn locationCol = new TreeColumn(main, SWT.SINGLE);
    locationCol.setText("Location");
    locationCol.setWidth(185);
    locationCol.setResizable(true);

    TreeColumn stateCol = new TreeColumn(main, SWT.SINGLE);
    stateCol.setText("State");
    stateCol.setWidth(95);
    stateCol.setResizable(true);

    TreeColumn statusCol = new TreeColumn(main, SWT.SINGLE);
    statusCol.setText("Status");
    statusCol.setWidth(300);
    statusCol.setResizable(true);

    viewer = new TreeViewer(main);
    viewer.setContentProvider(this);
    viewer.setLabelProvider(this);
    viewer.setInput(CONTENT_ROOT); // dont care

    getViewSite().setSelectionProvider(viewer);
    getViewSite().getActionBars().setGlobalActionHandler(
        ActionFactory.DELETE.getId(), DELETE);

    getViewSite().getActionBars().getToolBarManager().add(PROPERTIES);

    // getViewSite().getActionBars().getToolBarManager().add(new
    // StartAction());
    getViewSite().getActionBars().getToolBarManager().add(NEWSERVER);
  }

  // NewServerAction moved to actions package for cheat sheet access --
  // eyhung

  public class DeleteAction extends Action {
    @Override
    public void run() {
      ISelection selection =
          getViewSite().getSelectionProvider().getSelection();
      if ((selection != null) && (selection instanceof IStructuredSelection)) {
        Object selItem =
            ((IStructuredSelection) selection).getFirstElement();

        if (selItem instanceof HadoopServer) {
          HadoopServer location = (HadoopServer) selItem;
          ServerRegistry.getInstance().removeServer(location);

        } else if (selItem instanceof HadoopJob) {

          // kill the job
          HadoopJob job = (HadoopJob) selItem;
          HadoopServer server = job.getServer();
          String jobId = job.getJobId();

          if (job.isCompleted())
            return;

          try {
            Session session = server.createSession();

            String command =
                server.getInstallPath() + "/bin/hadoop job -kill " + jobId;
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.connect();
            channel.disconnect();

            session.disconnect();
          } catch (JSchException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  public static class StartAction extends Action {
    public StartAction() {
      setText("Start");

      // NOTE(jz) - all below from internal api, worst case no images
      setImageDescriptor(DebugPluginImages
          .getImageDescriptor(IDebugUIConstants.IMG_ACT_RUN));
    }
  }

  /* @inheritDoc */
  @Override
  public void setFocus() {

  }

  /* @inheritDoc */
  public void serverChanged(HadoopServer location, int type) {
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        ServerView.this.viewer.refresh();
      }
    });
  }

  /* @inheritDoc */
  public void inputChanged(final Viewer viewer, Object oldInput,
      Object newInput) {
    if (oldInput == CONTENT_ROOT)
      ServerRegistry.getInstance().removeListener(this);
    if (newInput == CONTENT_ROOT)
      ServerRegistry.getInstance().addListener(this);
  }

  /* @inheritDoc */
  public Object[] getElements(Object inputElement) {
    return ServerRegistry.getInstance().getServers().toArray();
  }

  /* @inheritDoc */
  public Object[] getChildren(Object parentElement) {
    if (parentElement instanceof HadoopServer) {
      ((HadoopServer) parentElement).addJobListener(this);

      return ((HadoopServer) parentElement).getChildren();
    }

    return null;
  }

  /* @inheritDoc */
  public Object getParent(Object element) {
    if (element instanceof HadoopServer) {
      return CONTENT_ROOT;
    } else if (element instanceof HadoopJob) {
      return ((HadoopJob) element).getServer();
    }
    return null;
  }

  /* @inheritDoc */
  public boolean hasChildren(Object element) {
    /* Only server entries have children */
    return (element instanceof HadoopServer);
  }

  /* @inheritDoc */
  public void addListener(ILabelProviderListener listener) {
    // no listeners handling
  }

  public boolean isLabelProperty(Object element, String property) {
    return false;
  }

  /* @inheritDoc */
  public void removeListener(ILabelProviderListener listener) {
    // no listener handling
  }

  /* @inheritDoc */
  public Image getColumnImage(Object element, int columnIndex) {
    if ((columnIndex == 0) && (element instanceof HadoopServer)) {
      return images.get("hadoop");
    } else if ((columnIndex == 0) && (element instanceof HadoopJob)) {
      return images.get("job");
    }
    return null;
  }

  /* @inheritDoc */
  public String getColumnText(Object element, int columnIndex) {
    if (element instanceof HadoopServer) {
      HadoopServer server = (HadoopServer) element;

      switch (columnIndex) {
        case 0:
          return server.getName();
        case 1:
          return server.getHostName().toString();
        case 2:
          return server.getState();
        case 3:
          return "";
      }
    } else if (element instanceof HadoopJob) {
      HadoopJob job = (HadoopJob) element;

      switch (columnIndex) {
        case 0:
          return job.getId();
        case 1:
          return "";
        case 2:
          return job.getState();
        case 3:
          return job.getStatus();
      }
    } else if (element instanceof JarModule) {
      JarModule jar = (JarModule) element;

      switch (columnIndex) {
        case 0:
          return jar.toString();
        case 1:
          return "Publishing jar to server..";
        case 2:
          return "";
      }
    }

    return null;
  }

  public void jobAdded(HadoopJob job) {
    viewer.refresh();
  }

  public void jobChanged(HadoopJob job) {
    viewer.refresh(job);
  }

  public void publishDone(JarModule jar) {
    viewer.refresh();
  }

  public void publishStart(JarModule jar) {
    viewer.refresh();
  }

  /**
   * Return the currently selected server (null if there is no selection or
   * if the selection is not a server)
   * 
   * @return the currently selected server entry
   */
  public HadoopServer getSelectedServer() {
    ITreeSelection selection = (ITreeSelection) viewer.getSelection();
    Object first = selection.getFirstElement();
    if (first instanceof HadoopServer) {
      return (HadoopServer) first;
    }
    return null;
  }

}
