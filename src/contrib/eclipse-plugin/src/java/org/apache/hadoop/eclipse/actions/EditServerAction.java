package org.apache.hadoop.eclipse.actions;

import java.io.IOException;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.servers.DefineHadoopServerLocWizardPage;
import org.apache.hadoop.eclipse.view.servers.ServerView;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardDialog;

/**
 * Editing server properties action
 */
public class EditServerAction extends Action {

  private ServerView serverView;

  public EditServerAction(ServerView serverView) {
    this.serverView = serverView;

    setText("Edit Hadoop Server");
    try {
      // TODO Edit server icon
      setImageDescriptor(ImageDescriptor.createFromURL((FileLocator
          .toFileURL(FileLocator.find(Activator.getDefault().getBundle(),
              new Path("resources/hadoop_small.gif"), null)))));
    } catch (IOException e) {
      /* Ignore if no image */
      e.printStackTrace();
    }
  }

  @Override
  public void run() {

    final HadoopServer server = serverView.getSelectedServer();
    if (server == null)
      return;

    WizardDialog dialog = new WizardDialog(null, new Wizard() {
      private DefineHadoopServerLocWizardPage page =
          new DefineHadoopServerLocWizardPage(server);

      @Override
      public void addPages() {
        super.addPages();
        setWindowTitle("Edit Hadoop Server Location");
        addPage(page);
      }

      @Override
      public boolean performFinish() {
        return (page.performFinish() != null);
      }
    });

    dialog.create();
    dialog.setBlockOnOpen(true);
    dialog.open();

    super.run();
  }
}
