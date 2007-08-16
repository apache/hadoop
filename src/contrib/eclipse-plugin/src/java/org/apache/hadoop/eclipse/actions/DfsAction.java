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

package org.apache.hadoop.eclipse.actions;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.eclipse.dfs.DfsFile;
import org.apache.hadoop.eclipse.dfs.DfsFolder;
import org.apache.hadoop.eclipse.dfs.DfsPath;
import org.eclipse.core.internal.runtime.AdapterManager;
import org.eclipse.core.resources.IStorage;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IObjectActionDelegate;
import org.eclipse.ui.IPersistableElement;
import org.eclipse.ui.ISharedImages;
import org.eclipse.ui.IStorageEditorInput;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class DfsAction implements IObjectActionDelegate {

  private ISelection selection;

  private IWorkbenchPart targetPart;

  /* @inheritDoc */
  public void setActivePart(IAction action, IWorkbenchPart targetPart) {
    this.targetPart = targetPart;
  }

  /* @inheritDoc */
  public void run(IAction action) {

    // Ignore non structured selections
    if (!(this.selection instanceof IStructuredSelection))
      return;

    IStructuredSelection ss = (IStructuredSelection) selection;
    String actionId = action.getActionDefinitionId();
    try {
      if (actionId.equals("dfs.delete"))
        delete(ss);
      else if (actionId.equals("dfs.open"))
        open(ss);
      else if (actionId.equals("dfs.put"))
        put(ss);
      else if (actionId.equals("dfs.refresh"))
        refresh(ss);
      else if (actionId.equals("dfs.get"))
        get(ss);

    } catch (Exception e) {
      Shell shell = new Shell();
      e.printStackTrace();
      MessageDialog.openError(shell, "DFS Error",
          "An error occurred while performing DFS operation: "
              + e.getMessage());
    }

  }

  /**
   * Implement the import action (upload files from the current machine to
   * HDFS)
   * 
   * @param object
   * @throws SftpException
   * @throws JSchException
   * @throws InvocationTargetException
   * @throws InterruptedException
   */
  private void put(IStructuredSelection selection) throws SftpException,
      JSchException, InvocationTargetException, InterruptedException {

    // Ask the user which local directory to upload
    DirectoryDialog dialog =
        new DirectoryDialog(Display.getCurrent().getActiveShell());
    dialog.setText("Copy from local directory");
    dialog.setMessage("Copy the local directory"
        + " to the selected directories on the distributed filesystem");
    String directory = dialog.open();

    if (directory == null)
      return;

    for (DfsFolder folder : filterSelection(DfsFolder.class, selection))
      folder.put(directory);
  }

  /**
   * Implements the Download action from HDFS to the current machine
   * 
   * @param object
   * @throws SftpException
   * @throws JSchException
   */
  private void get(IStructuredSelection selection) throws SftpException,
      JSchException {

    // Ask the user where to put the downloaded files
    DirectoryDialog dialog =
        new DirectoryDialog(Display.getCurrent().getActiveShell());
    dialog.setText("Copy to local directory");
    dialog.setMessage("Copy the selected files and directories from the "
        + "distributed filesystem to a local directory");
    String directory = dialog.open();

    if (directory == null)
      return;

    for (DfsPath path : filterSelection(DfsPath.class, selection)) {
      try {
        path.downloadToLocalDirectory(directory);
      } catch (Exception e) {
        // nothing we want to do here, ignore
        e.printStackTrace();
      }
    }

  }

  /**
   * Open the selected DfsPath in the editor window
   * 
   * @param selection
   * @throws JSchException
   * @throws IOException
   * @throws PartInitException
   * @throws InvocationTargetException
   * @throws InterruptedException
   */
  private void open(IStructuredSelection selection) throws JSchException,
      IOException, PartInitException, InvocationTargetException,
      InterruptedException {

    for (final DfsFile path : filterSelection(DfsFile.class, selection)) {

      final InputStream data = path.open();
      if (data == null)
        continue;

      final IStorage storage = new IStorage() {
        public Object getAdapter(Class adapter) {
          return AdapterManager.getDefault().getAdapter(this, adapter);
        }

        public boolean isReadOnly() {
          return true;
        }

        public String getName() {
          return path.toString();
        }

        public IPath getFullPath() {
          return new Path(path.toString());
        }

        public InputStream getContents() throws CoreException {
          return data;
        }
      };

      IStorageEditorInput storageEditorInput = new IStorageEditorInput() {
        public Object getAdapter(Class adapter) {
          return null;
        }

        public String getToolTipText() {
          return "";
        }

        public IPersistableElement getPersistable() {
          return null;
        }

        public String getName() {
          return path.toString();
        }

        public ImageDescriptor getImageDescriptor() {
          return PlatformUI.getWorkbench().getSharedImages()
              .getImageDescriptor(ISharedImages.IMG_OBJ_FILE);
        }

        public boolean exists() {
          return true;
        }

        public IStorage getStorage() throws CoreException {
          return storage;
        }
      };

      targetPart.getSite().getWorkbenchWindow().getActivePage().openEditor(
          storageEditorInput, "org.eclipse.ui.DefaultTextEditor");
    }
  }

  private void refresh(IStructuredSelection selection) throws JSchException {
    for (DfsPath path : filterSelection(DfsPath.class, selection))
      path.refresh();

  }

  private void delete(IStructuredSelection selection) throws JSchException {
    List<DfsPath> list = filterSelection(DfsPath.class, selection);
    if (list.isEmpty())
      return;

    if (MessageDialog.openConfirm(null, "Confirm Delete from DFS",
        "Are you sure you want to delete " + list + " from the DFS?")) {
      for (DfsPath path : list)
        path.delete();
    }
  }

  /* @inheritDoc */
  public void selectionChanged(IAction action, ISelection selection) {
    this.selection = selection;
  }

  /**
   * Extract the list of <T> from the structured selection
   * 
   * @param clazz the class T
   * @param selection the structured selection
   * @return the list of <T> it contains
   */
  private <T> List<T> filterSelection(Class<T> clazz,
      IStructuredSelection selection) {
    List<T> list = new ArrayList<T>();
    for (Object obj : selection.toList()) {
      if (clazz.isAssignableFrom(obj.getClass())) {
        list.add((T) obj);
      }
    }
    return list;
  }

}
