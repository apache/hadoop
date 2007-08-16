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

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

/**
 * SWT Forms to prompt for passwords or for keyboard interactive
 * authentication
 */
public abstract class SWTUserInfo implements UserInfo, UIKeyboardInteractive {

  /**
   * Password that was last tried; this to prevent sending many times the
   * "saved" password if it does not work
   */
  private String triedPassword = null;

  public SWTUserInfo() {
  }

  /* @inheritDoc */
  public String getPassphrase() {
    return this.getPassword();
  }

  /* @inheritDoc */
  public abstract String getPassword();

  public abstract void setPassword(String pass);

  public void setPassphrase(String pass) {
    this.setPassword(pass);
  }

  /* @inheritDoc */
  public boolean promptPassphrase(final String arg0) {
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        SWTUserInfo.this.setPassword(new PasswordDialog(null).prompt(arg0));
      }
    });

    return this.getPassword() != null;
  }

  public boolean promptPassword(final String arg0) {
    /*
     * Ask the user for the password if we don't know it, or if we now it but
     * already tried it
     */
    if ((this.getPassword() == null)
        || this.getPassword().equals(this.triedPassword)) {
      Display.getDefault().syncExec(new Runnable() {
        public void run() {
          Shell parent = Display.getDefault().getActiveShell();
          String password = new PasswordDialog(parent).prompt(arg0);
          SWTUserInfo.this.triedPassword = password;
          SWTUserInfo.this.setPassword(password);
        }
      });
    }

    return this.getPassword() != null;
  }

  /**
   * To store the result of the Yes/No message dialog
   */
  private boolean yesNoResult;

  /* @inheritDoc */
  public boolean promptYesNo(final String arg0) {

    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        yesNoResult =
            MessageDialog.openQuestion(
                Display.getDefault().getActiveShell(),
                "SSH Question Dialog", arg0);
      }
    });

    return yesNoResult;
  }

  /* @inheritDoc */
  public void showMessage(final String arg0) {
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        MessageDialog.openInformation(null, "SSH Message", arg0);
      }
    });
  }

  /**
   * To store the answers from the keyboard interactive dialog
   */
  private String[] interactiveAnswers;

  /* @inheritDoc */
  public String[] promptKeyboardInteractive(final String destination,
      final String name, final String instruction, final String[] prompt,
      final boolean[] echo) {
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        Shell parent = Display.getDefault().getActiveShell();
        interactiveAnswers =
            new KeyboardInteractiveDialog(parent).prompt(destination, name,
                instruction, prompt, echo);
      }
    });
    return interactiveAnswers;
  }

  /**
   * Simple password prompting dialog
   */
  public static class PasswordDialog extends Dialog {

    private Text text;

    private String password;

    private String message;

    protected PasswordDialog(Shell parentShell) {
      super(parentShell);
    }

    public String prompt(String message) {
      this.setBlockOnOpen(true);
      this.message = message;

      if (this.open() == OK) {
        return password;
      } else {
        return null;
      }
    }

    @Override
    protected void okPressed() {
      this.password = text.getText();
      super.okPressed();
    }

    @Override
    protected Control createDialogArea(Composite parent) {
      Composite panel = (Composite) super.createDialogArea(parent);
      panel.setLayout(new GridLayout(2, false));
      panel.setLayoutData(new GridData(GridData.FILL_BOTH));

      Label title = new Label(panel, SWT.NONE);
      GridData span2 = new GridData(GridData.FILL_HORIZONTAL);
      span2.horizontalSpan = 2;
      title.setLayoutData(span2);
      title.setText(message);

      getShell().setText(message);

      Label label = new Label(panel, SWT.NONE);
      label.setText("password");

      text = new Text(panel, SWT.BORDER | SWT.SINGLE);
      GridData data = new GridData(GridData.FILL_HORIZONTAL);
      data.grabExcessHorizontalSpace = true;
      text.setLayoutData(data);
      text.setEchoChar('*');

      return panel;
    }
  }

  /**
   * Keyboard interactive prompting dialog
   */
  public static class KeyboardInteractiveDialog extends Dialog {

    private String destination;

    private String name;

    private String instruction;

    private String[] prompt;

    private boolean[] echo;

    private Text[] text;

    private String[] answer;

    protected KeyboardInteractiveDialog(Shell parentShell) {
      super(parentShell);
    }

    public String[] prompt(String destination, String name,
        String instruction, String[] prompt, boolean[] echo) {

      this.destination = destination;
      this.name = name;
      this.instruction = instruction;
      this.prompt = prompt;
      this.echo = echo;

      this.setBlockOnOpen(true);

      if (this.open() == OK)
        return answer;
      else
        return null;
    }

    @Override
    protected void okPressed() {
      answer = new String[text.length];
      for (int i = 0; i < text.length; ++i) {
        answer[i] = text[i].getText();
      }
      super.okPressed();
    }

    @Override
    protected Control createDialogArea(Composite parent) {
      Composite panel = (Composite) super.createDialogArea(parent);
      panel.setLayout(new GridLayout(2, false));
      panel.setLayoutData(new GridData(GridData.FILL_BOTH));

      Label title = new Label(panel, SWT.NONE);
      GridData span2 = new GridData(GridData.FILL_HORIZONTAL);
      span2.horizontalSpan = 2;
      title.setLayoutData(span2);
      title.setText(destination + ": " + name);

      getShell().setText(instruction);

      text = new Text[prompt.length];

      for (int i = 0; i < text.length; ++i) {
        Label label = new Label(panel, SWT.NONE);
        label.setText("password");

        text[i] = new Text(panel, SWT.BORDER | SWT.SINGLE);
        GridData data = new GridData(GridData.FILL_HORIZONTAL);
        data.grabExcessHorizontalSpace = true;
        text[i].setLayoutData(data);
        if (!echo[i])
          text[i].setEchoChar('*');
      }

      return panel;
    }
  }

}
