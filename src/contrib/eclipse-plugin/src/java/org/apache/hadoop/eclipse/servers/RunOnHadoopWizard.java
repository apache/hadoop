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

import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.server.JarModule;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;

/**
 * Wizard for publishing a job to a Hadoop server.
 */

public class RunOnHadoopWizard extends Wizard implements SelectionListener {

  private DefineHadoopServerLocWizardPage createNewPage;

  private MainPage mainPage;

  private final JarModule jar;

  private boolean complete = false;

  private IProgressMonitor progressMonitor;

  public RunOnHadoopWizard(JarModule jar) {
    this.jar = jar;
    setForcePreviousAndNextButtons(true);
    setNeedsProgressMonitor(true);
    setWindowTitle("Run on Hadoop");
  }

  @Override
  public void addPages() {
    super.addPages();
    mainPage = new MainPage();
    addPage(mainPage);
    createNewPage = new DefineHadoopServerLocWizardPage();
    addPage(createNewPage);
  }

  @Override
  /**
   * Performs any actions appropriate in response to the user having pressed
   * the Finish button, or refuse if finishing now is not permitted.
   */
  public boolean performFinish() {
    HadoopServer location = null;
    if (mainPage.createNew.getSelection()) {
      location = createNewPage.performFinish();
    } else if (mainPage.table.getSelection().length == 1) {
      location = (HadoopServer) mainPage.table.getSelection()[0].getData();
    }

    if (location != null) {
      location.runJar(jar, progressMonitor);

      return true;
    }

    return false;
  }

  public void refreshButtons() {
    getContainer().updateButtons();
  }

  @Override
  /**
   * Allows finish when an existing server is selected or when a new server
   * location is defined
   */
  public boolean canFinish() {

    if (mainPage.chooseExisting.getSelection()
        && (mainPage.table.getSelectionCount() > 0)) {
      return true;
    } else {
      return (createNewPage.isPageComplete());
      // check first
    }
  }

  public class MainPage extends WizardPage {

    private Button createNew;

    private Table table;

    public Button chooseExisting;

    public MainPage() {
      super("Select or define server to run on");
      setTitle("Select Hadoop Server");
      setDescription("Select a Hadoop Server to run on.");
    }

    @Override
    public boolean canFlipToNextPage() {
      return createNew.getSelection();
    }

    public void createControl(Composite parent) {
      Composite control = new Composite(parent, SWT.NONE);
      control.setLayout(new GridLayout(4, false));

      Label label = new Label(control, SWT.FILL);
      label.setText("Select a Hadoop Server to run on.");
      GridData data = new GridData(GridData.FILL_BOTH);
      data.grabExcessVerticalSpace = false;
      data.horizontalSpan = 4;
      label.setLayoutData(data);

      createNew = new Button(control, SWT.RADIO);
      createNew.setText("Define a new Hadoop server location");
      createNew.setLayoutData(data);
      createNew.addSelectionListener(RunOnHadoopWizard.this);

      createNew.setSelection(true);

      chooseExisting = new Button(control, SWT.RADIO);
      chooseExisting
          .setText("Choose an existing server from the list below");
      chooseExisting.setLayoutData(data);
      chooseExisting.addSelectionListener(RunOnHadoopWizard.this);

      chooseExisting.addSelectionListener(new SelectionListener() {

        public void widgetSelected(SelectionEvent e) {
          if (chooseExisting.getSelection()
              && (table.getSelectionCount() == 0)) {
            if (table.getItems().length > 0) {
              table.setSelection(0);
            }
          }
        }

        public void widgetDefaultSelected(SelectionEvent e) {
        }

      });

      Composite serverList = new Composite(control, SWT.NONE);
      GridData span = new GridData(GridData.FILL_BOTH);
      span.horizontalSpan = 4;
      serverList.setLayoutData(span);
      GridLayout layout = new GridLayout(4, false);
      layout.marginTop = 12;
      serverList.setLayout(layout);

      table =
          new Table(serverList, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL
              | SWT.FULL_SELECTION);
      table.setHeaderVisible(true);
      table.setLinesVisible(true);
      GridData d = new GridData(GridData.FILL_HORIZONTAL);
      d.horizontalSpan = 4;
      d.heightHint = 300;
      table.setLayoutData(d);

      TableColumn nameColumn = new TableColumn(table, SWT.SINGLE);
      nameColumn.setText("Name");
      nameColumn.setWidth(160);

      TableColumn hostColumn = new TableColumn(table, SWT.SINGLE);
      hostColumn.setText("Location");
      hostColumn.setWidth(200);

      table.addSelectionListener(new SelectionListener() {
        public void widgetSelected(SelectionEvent e) {
          chooseExisting.setSelection(true);
          createNew.setSelection(false); // shouldnt be necessary,
          // but got a visual bug once

          refreshButtons();
        }

        public void widgetDefaultSelected(SelectionEvent e) {

        }
      });

      TableViewer viewer = new TableViewer(table);
      HadoopServerSelectionListContentProvider provider =
          new HadoopServerSelectionListContentProvider();
      viewer.setContentProvider(provider);
      viewer.setLabelProvider(provider);
      viewer.setInput(new Object()); // don't care, get from singleton
      // server registry

      setControl(control);
    }
  }

  public void widgetDefaultSelected(SelectionEvent e) {
    // TODO Auto-generated method stub

  }

  public void widgetSelected(SelectionEvent e) {
    refreshButtons();
  }

  public void setProgressMonitor(IProgressMonitor progressMonitor) {
    this.progressMonitor = progressMonitor;
  }
}
