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

import java.util.logging.Logger;

import org.apache.hadoop.eclipse.actions.RunOnHadoopActionDelegate;
import org.eclipse.core.resources.IResource;
import org.eclipse.debug.ui.ILaunchShortcut;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.ui.IEditorPart;
import org.eclipse.ui.actions.ActionDelegate;


/**
 * Add a shortcut "Run on Hadoop" to the Run menu
 */

public class LaunchShortcut implements ILaunchShortcut {
  static Logger log = Logger.getLogger(LaunchShortcut.class.getName());

  private ActionDelegate delegate = new RunOnHadoopActionDelegate();

  public LaunchShortcut() {
  }

  public void launch(final ISelection selection, String mode) {
    if (selection instanceof IStructuredSelection) {
      delegate.selectionChanged(null, selection);
      delegate.run(null);
    }
  }

  public void launch(final IEditorPart editor, String mode) {
    delegate.selectionChanged(null, new StructuredSelection(editor
        .getEditorInput().getAdapter(IResource.class))); // hmm(jz)
    // :-)
    delegate.run(null);
  }
}
