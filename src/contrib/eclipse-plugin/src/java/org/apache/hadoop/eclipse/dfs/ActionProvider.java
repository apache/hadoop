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

import java.util.HashMap;
import java.util.Map;

import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.ui.IActionBars;
import org.eclipse.ui.ISharedImages;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.actions.ActionFactory;
import org.eclipse.ui.navigator.CommonActionProvider;
import org.eclipse.ui.navigator.ICommonActionConstants;
import org.eclipse.ui.navigator.ICommonActionExtensionSite;
import org.eclipse.ui.navigator.ICommonMenuConstants;
import org.eclipse.ui.plugin.AbstractUIPlugin;

/**
 * Allows the user to delete and refresh items in the DFS tree
 */

public class ActionProvider extends CommonActionProvider {

  private ICommonActionExtensionSite site;

  private Map<String, ImageDescriptor> descriptors =
      new HashMap<String, ImageDescriptor>();

  public ActionProvider() {
  }

  /* @inheritDoc */
  @Override
  public void init(ICommonActionExtensionSite site) {
    super.init(site);
    this.site = site;

    descriptors
        .put("dfs.delete", PlatformUI.getWorkbench().getSharedImages()
            .getImageDescriptor(ISharedImages.IMG_TOOL_DELETE));
    descriptors.put("dfs.refresh", AbstractUIPlugin
        .imageDescriptorFromPlugin("org.eclipse.core.tools.resources",
            "icons/refresh.gif"));
    // NOTE(jz)
    // pretty brittle, but worst case no image
    // descriptors.put("dfs.put",
    // NavigatorPlugin.imageDescriptorFromPlugin("org.eclipse.core.tools.resources",
    // "icons/refresh.gif"));
  }

  /* @inheritDoc */
  @Override
  public void fillActionBars(IActionBars actionBars) {
    actionBars.setGlobalActionHandler(ActionFactory.DELETE.getId(),
        new DfsAction("dfs.delete", "Delete"));
    actionBars.setGlobalActionHandler(ActionFactory.REFRESH.getId(),
        new DfsAction("dfs.refresh", "Refresh"));

    if ((this.site != null)
        && (this.site.getStructuredViewer().getSelection() instanceof IStructuredSelection)
        && (((IStructuredSelection) this.site.getStructuredViewer()
            .getSelection()).size() == 1)
        && (((IStructuredSelection) this.site.getStructuredViewer()
            .getSelection()).getFirstElement() instanceof DfsFile)) {
      actionBars.setGlobalActionHandler(ICommonActionConstants.OPEN,
          new DfsAction("dfs.open", "View"));
    }

    actionBars.updateActionBars();
  }

  /* @inheritDoc */
  @Override
  public void fillContextMenu(IMenuManager menu) {
    menu.appendToGroup(ICommonMenuConstants.GROUP_EDIT, new DfsAction(
        "dfs.delete", "Delete"));
    menu.appendToGroup(ICommonMenuConstants.GROUP_EDIT, new DfsAction(
        "dfs.refresh", "Refresh"));

    menu.appendToGroup(ICommonMenuConstants.GROUP_NEW, new DfsAction(
        "dfs.get", "Download to local directory..."));

    if (this.site == null)
      return;

    ISelection isel = this.site.getStructuredViewer().getSelection();
    if (!(isel instanceof IStructuredSelection))
      return;

    IStructuredSelection issel = (IStructuredSelection) isel;
    if (issel.size() != 1)
      return;

    Object element = issel.getFirstElement();

    if (element instanceof DfsFile) {
      menu.appendToGroup(ICommonMenuConstants.GROUP_OPEN, new DfsAction(
          "dfs.open", "View"));

    } else if (element instanceof DfsFolder) {
      menu.appendToGroup(ICommonMenuConstants.GROUP_NEW, new DfsAction(
          "dfs.put", "Import from local directory..."));
    }
  }

  /**
   * 
   */
  public class DfsAction extends Action {

    private final String actionDefinition;

    private final String title;

    public DfsAction(String actionDefinition, String title) {
      this.actionDefinition = actionDefinition;
      this.title = title;

    }

    @Override
    public String getText() {
      return this.title;
    }

    @Override
    public ImageDescriptor getImageDescriptor() {
      if (descriptors.containsKey(getActionDefinitionId())) {
        return (ImageDescriptor) descriptors.get(getActionDefinitionId());
      } else {
        return null;
      }
    }

    @Override
    public String getActionDefinitionId() {
      return actionDefinition;
    }

    @Override
    public void run() {
      org.apache.hadoop.eclipse.actions.DfsAction action =
          new org.apache.hadoop.eclipse.actions.DfsAction();
      action.setActivePart(this, PlatformUI.getWorkbench()
          .getActiveWorkbenchWindow().getActivePage().getActivePart());
      action.selectionChanged(this, site.getStructuredViewer()
          .getSelection());
      action.run(this);
    }

    @Override
    public boolean isEnabled() {
      return true;
    }
  }
}
