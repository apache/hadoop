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

import java.io.IOException;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.servers.IHadoopServerListener;
import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.IAdaptable;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.viewers.ILabelProvider;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.StructuredViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.ISharedImages;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.model.IWorkbenchAdapter;

/**
 * Handles viewing the DFS
 */
public class DFSContentProvider implements ITreeContentProvider,
    ILabelProvider {

  /**
   * The viewer that displays this Tree content
   */
  private Viewer viewer;

  private ImageDescriptor hadoopImage;

  private ImageDescriptor folderImage;

  private ImageDescriptor fileImage;

  private ImageDescriptor dfsImage;

  public DFSContentProvider() {
    try {
      hadoopImage =
          ImageDescriptor.createFromURL((FileLocator.toFileURL(FileLocator
              .find(Activator.getDefault().getBundle(), new Path(
                  "resources/hadoop_small.gif"), null))));
      dfsImage =
          ImageDescriptor.createFromURL((FileLocator.toFileURL(FileLocator
              .find(Activator.getDefault().getBundle(), new Path(
                  "resources/files.gif"), null))));
    } catch (IOException e) {
      e.printStackTrace();
      // no images, okay, will deal with that
    }
  }

  public Object[] getChildren(Object parentElement) {
    if (parentElement instanceof DummyWorkspace) {
      return ResourcesPlugin.getWorkspace().getRoot().getProjects();
    }
    if (parentElement instanceof DFS) {
      return ServerRegistry.getInstance().getServers().toArray();
    } else if (parentElement instanceof HadoopServer) {
      return new Object[] { new DfsFolder((HadoopServer) parentElement, "/",
          viewer) };
    } else if (parentElement instanceof DfsFolder) {
      return ((DfsFolder) parentElement).getChildren();
    }

    return new Object[0];
  }

  public Object getParent(Object element) {
    if (element instanceof DfsPath) {
      return ((DfsPath) element).getParent();
    } else if (element instanceof HadoopServer) {
      return dfs;
    } else {
      return null;
    }
  }

  public boolean hasChildren(Object element) {
    return (element instanceof HadoopServer)
        || (element instanceof DfsFolder) || (element instanceof DFS)
        || (element instanceof DummyWorkspace);
  }

  public class DFS {
    public DFS() {
      ServerRegistry.getInstance().addListener(new IHadoopServerListener() {
        public void serverChanged(final HadoopServer location, final int type) {
          if (viewer != null) {
            Display.getDefault().syncExec(new Runnable() {
              public void run() {
                if (type == ServerRegistry.SERVER_STATE_CHANGED) {
                  ((StructuredViewer) viewer).refresh(location);
                } else {
                  ((StructuredViewer) viewer).refresh(ResourcesPlugin
                      .getWorkspace().getRoot());
                }
              }
            });
          }
        }
      });
    }

    @Override
    public String toString() {
      return "MapReduce DFS";
    }
  }

  private final DFS dfs = new DFS();

  private final Object workspace = new DummyWorkspace();

  private static class DummyWorkspace {
    @Override
    public String toString() {
      return "Workspace";
    }
  };

  public Object[] getElements(final Object inputElement) {
    return ServerRegistry.getInstance().getServers().toArray();
  }

  public void dispose() {

  }

  public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
    this.viewer = viewer;
  }

  public Image getImage(Object element) {
    if (element instanceof DummyWorkspace) {
      IWorkbenchAdapter a =
          (IWorkbenchAdapter) ((IAdaptable) ResourcesPlugin.getWorkspace()
              .getRoot()).getAdapter(IWorkbenchAdapter.class);
      return a.getImageDescriptor(ResourcesPlugin.getWorkspace().getRoot())
          .createImage();
    } else if (element instanceof DFS) {
      return dfsImage.createImage(true);
    } else if (element instanceof HadoopServer) {
      return hadoopImage.createImage(true);
    } else if (element instanceof DfsFolder) {
      return PlatformUI.getWorkbench().getSharedImages().getImageDescriptor(
          ISharedImages.IMG_OBJ_FOLDER).createImage();
    } else if (element instanceof DfsFile) {
      return PlatformUI.getWorkbench().getSharedImages().getImageDescriptor(
          ISharedImages.IMG_OBJ_FILE).createImage();
    }

    return null;
  }

  public String getText(Object element) {
    if (element instanceof DummyWorkspace) {
      IWorkbenchAdapter a =
          (IWorkbenchAdapter) ((IAdaptable) ResourcesPlugin.getWorkspace()
              .getRoot()).getAdapter(IWorkbenchAdapter.class);
      return a.getLabel(ResourcesPlugin.getWorkspace().getRoot());
    } else {
      return element.toString();
    }
  }

  public void addListener(ILabelProviderListener listener) {

  }

  public boolean isLabelProperty(Object element, String property) {
    return false;
  }

  public void removeListener(ILabelProviderListener listener) {

  }
}
