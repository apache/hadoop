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

import org.apache.hadoop.eclipse.server.JarModule;
import org.apache.hadoop.eclipse.servers.RunOnHadoopWizard;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.runtime.IAdaptable;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.wizard.IWizard;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.actions.ActionDelegate;

/**
 * Allows a resource to be associated with the "Run on Hadoop" action in the
 * Run menu. Only files, not directories, may be run on Hadoop. The file
 * needs to have a main method. When the "Run on Hadoop" action is called,
 * launch the RunOnHadoop Dialog.
 */

public class RunOnHadoopActionDelegate extends ActionDelegate {

  private ISelection selection;

  @Override
  public void selectionChanged(IAction action, ISelection selection) {
    this.selection = selection;
  }

  @Override
  public void run(IAction action) {
    if ((selection == null)
        || (!(selection instanceof IStructuredSelection)))
      return;

    IStructuredSelection issel = (IStructuredSelection) selection;

    if (issel.size() != 1)
      return;

    Object selected = issel.getFirstElement();
    if (!(selected instanceof IAdaptable))
      return;

    IAdaptable adaptable = (IAdaptable) selected;

    IResource resource = (IResource) adaptable.getAdapter(IResource.class);

    // 63561: only allow run-on on file resources
    if ((resource != null) && (resource.getType() == IResource.FILE)) {
      RunOnHadoopWizard wizard =
          new RunOnHadoopWizard(new JarModule(resource));

      WizardDialog dialog = new Dialog(null, wizard);
      dialog.create();
      dialog.setBlockOnOpen(true);
      dialog.open();

      return;
    }

    MessageDialog
        .openInformation(Display.getDefault().getActiveShell(),
            "No Main method found",
            "Please select a file with a main method to Run on a MapReduce server");
  }

  static class Dialog extends WizardDialog {
    public Dialog(Shell parentShell, IWizard newWizard) {
      super(parentShell, newWizard);
    }

    @Override
    public void create() {
      super.create();

      ((RunOnHadoopWizard) getWizard())
          .setProgressMonitor(getProgressMonitor());
    }
  }
}
