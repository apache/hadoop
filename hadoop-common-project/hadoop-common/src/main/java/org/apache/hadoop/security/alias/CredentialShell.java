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

package org.apache.hadoop.security.alias;

import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program is the CLI utility for the CredentialProvider facilities in 
 * Hadoop.
 */
public class CredentialShell extends Configured implements Tool {
  final static private String USAGE_PREFIX = "Usage: hadoop credential " +
      "[generic options]\n";
  final static private String COMMANDS =
      "   [-help]\n" +
      "   [" + CreateCommand.USAGE + "]\n" +
      "   [" + DeleteCommand.USAGE + "]\n" +
      "   [" + ListCommand.USAGE + "]\n";
  @VisibleForTesting
  public static final String NO_VALID_PROVIDERS =
      "There are no valid (non-transient) providers configured.\n" +
      "No action has been taken. Use the -provider option to specify\n" +
      "a provider. If you want to use a transient provider then you\n" +
      "MUST use the -provider argument.";

  private boolean interactive = true;
  private Command command = null;

  /** If true, fail if the provider requires a password and none is given. */
  private boolean strict = false;

  /** Allows stdout to be captured if necessary. */
  @VisibleForTesting
  public PrintStream out = System.out;
  /** Allows stderr to be captured if necessary. */
  @VisibleForTesting
  public PrintStream err = System.err;

  private boolean userSuppliedProvider = false;
  private String value = null;
  private PasswordReader passwordReader;
  private boolean isHelp = false;

  @Override
  public int run(String[] args) throws Exception {
    int exitCode = 0;
    try {
      exitCode = init(args);
      if (exitCode != 0) {
        return exitCode;
      }
      if (!isHelp) {
        if (command.validate()) {
          command.execute();
        } else {
          exitCode = 1;
        }
      }
    } catch (Exception e) {
      e.printStackTrace(err);
      return 1;
    }
    return exitCode;
  }

  /**
   * Parse the command line arguments and initialize the data.
   * <pre>
   * % hadoop credential create alias [-provider providerPath]
   * % hadoop credential list [-provider providerPath]
   * % hadoop credential delete alias [-provider providerPath] [-f]
   * </pre>
   * @param args
   * @return 0 if the argument(s) were recognized, 1 otherwise
   * @throws IOException
   */
  protected int init(String[] args) throws IOException {
    // no args should print the help message
    if (0 == args.length) {
      printCredShellUsage();
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("create")) {
        if (i == args.length - 1) {
          printCredShellUsage();
          return 1;
        }
        String alias = args[++i];
        command = new CreateCommand(alias);
        if (alias.equals("-help")) {
          printCredShellUsage();
          return 0;
        }
      } else if (args[i].equals("delete")) {
        if (i == args.length - 1) {
          printCredShellUsage();
          return 1;
        }
        String alias = args[++i];
        command = new DeleteCommand(alias);
        if (alias.equals("-help")) {
          printCredShellUsage();
          return 0;
        }
      } else if (args[i].equals("list")) {
        command = new ListCommand();
      } else if (args[i].equals("-provider")) {
        if (i == args.length - 1) {
          printCredShellUsage();
          return 1;
        }
        userSuppliedProvider = true;
        getConf().set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, 
            args[++i]);
      } else if (args[i].equals("-f") || (args[i].equals("-force"))) {
        interactive = false;
      } else if (args[i].equals("-strict")) {
        strict = true;
      } else if (args[i].equals("-v") || (args[i].equals("-value"))) {
        value = args[++i];
      } else if (args[i].equals("-help")) {
        printCredShellUsage();
        return 0;
      } else {
        printCredShellUsage();
        ToolRunner.printGenericCommandUsage(System.err);
        return 1;
      }
    }
    return 0;
  }

  private void printCredShellUsage() {
    isHelp = true;
    out.println(USAGE_PREFIX + COMMANDS);
    if (command != null) {
      out.println(command.getUsage());
    } else {
      out.println("=========================================================" +
          "======");
      out.println(CreateCommand.USAGE + ":\n\n" + CreateCommand.DESC);
      out.println("=========================================================" +
          "======");
      out.println(DeleteCommand.USAGE + ":\n\n" + DeleteCommand.DESC);
      out.println("=========================================================" +
          "======");
      out.println(ListCommand.USAGE + ":\n\n" + ListCommand.DESC);
    }
  }

  private abstract class Command {
    protected CredentialProvider provider = null;

    public boolean validate() {
      return true;
    }

    protected CredentialProvider getCredentialProvider() {
      CredentialProvider prov = null;
      List<CredentialProvider> providers;
      try {
        providers = CredentialProviderFactory.getProviders(getConf());
        if (userSuppliedProvider) {
          prov = providers.get(0);
        } else {
          for (CredentialProvider p : providers) {
            if (!p.isTransient()) {
              prov = p;
              break;
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace(err);
      }
      if (prov == null) {
        out.println(NO_VALID_PROVIDERS);
      }
      return prov;
    }

    protected void printProviderWritten() {
      out.println("Provider " + provider.toString() + " has been updated.");
    }

    protected void warnIfTransientProvider() {
      if (provider.isTransient()) {
        out.println("WARNING: you are modifying a transient provider.");
      }
    }

    public abstract void execute() throws Exception;

    public abstract String getUsage();
  }

  private class ListCommand extends Command {
    public static final String USAGE =
        "list [-provider provider-path] [-strict]";
    public static final String DESC =
        "The list subcommand displays the aliases contained within \n" +
        "a particular provider - as configured in core-site.xml or\n" +
        "indicated through the -provider argument. If -strict is supplied,\n" +
        "fail immediately if the provider requires a password and none is\n" +
        "provided.";

    public boolean validate() {
      provider = getCredentialProvider();
      return (provider != null);
    }

    public void execute() throws IOException {
      List<String> aliases;
      try {
        aliases = provider.getAliases();
        out.println("Listing aliases for CredentialProvider: " +
            provider.toString());
        for (String alias : aliases) {
          out.println(alias);
        }
      } catch (IOException e) {
        out.println("Cannot list aliases for CredentialProvider: " +
            provider.toString()
            + ": " + e.getMessage());
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class DeleteCommand extends Command {
    public static final String USAGE =
        "delete <alias> [-f] [-provider provider-path] [-strict]";
    public static final String DESC =
        "The delete subcommand deletes the credential\n" +
        "specified as the <alias> argument from within the provider\n" +
        "indicated through the -provider argument. The command asks for\n" +
        "confirmation unless the -f option is specified. If -strict is\n" +
        "supplied, fail immediately if the provider requires a password\n" +
        "and none is given.";

    private String alias = null;
    private boolean cont = true;

    public DeleteCommand(String alias) {
      this.alias = alias;
    }

    @Override
    public boolean validate() {
      provider = getCredentialProvider();
      if (provider == null) {
        return false;
      }
      if (alias == null) {
        out.println("There is no alias specified. Please provide the" +
            "mandatory <alias>. See the usage description with -help.");
        return false;
      }
      if (interactive) {
        try {
          cont = ToolRunner
              .confirmPrompt("You are about to DELETE the credential " +
                  alias + " from CredentialProvider " + provider.toString() +
                  ". Continue? ");
          if (!cont) {
            out.println("Nothing has been be deleted.");
          }
          return cont;
        } catch (IOException e) {
          out.println(alias + " will not be deleted.");
          e.printStackTrace(err);
        }
      }
      return true;
    }

    public void execute() throws IOException {
      warnIfTransientProvider();
      out.println("Deleting credential: " + alias +
          " from CredentialProvider: " + provider.toString());
      if (cont) {
        try {
          provider.deleteCredentialEntry(alias);
          out.println("Credential " + alias +
              " has been successfully deleted.");
          provider.flush();
          printProviderWritten();
        } catch (IOException e) {
          out.println("Credential " + alias + " has NOT been deleted.");
          throw e;
        }
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class CreateCommand extends Command {
    public static final String USAGE = "create <alias> [-value alias-value] " +
        "[-provider provider-path] [-strict]";
    public static final String DESC =
        "The create subcommand creates a new credential for the name\n" +
        "specified as the <alias> argument within the provider indicated\n" +
        "through the -provider argument. If -strict is supplied, fail\n" +
        "immediately if the provider requires a password and none is given.\n" +
        "If -value is provided, use that for the value of the credential\n" +
        "instead of prompting the user.";

    private String alias = null;

    public CreateCommand(String alias) {
      this.alias = alias;
    }

    public boolean validate() {
      boolean rc = true;
      try {
        provider = getCredentialProvider();
        if (provider == null) {
          rc = false;
        } else if (provider.needsPassword()) {
          if (strict) {
            out.println(provider.noPasswordError());
            rc = false;
          } else {
            out.println(provider.noPasswordWarning());
          }
        }
      } catch (IOException e) {
        e.printStackTrace(err);
      }
      if (alias == null) {
        out.println("There is no alias specified. Please provide the" +
            "mandatory <alias>. See the usage description with -help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws IOException, NoSuchAlgorithmException {
      warnIfTransientProvider();
      try {
        char[] credential = null;
        if (value != null) {
          // testing only
          credential = value.toCharArray();
        } else {
          credential = promptForCredential();
        }
        provider.createCredentialEntry(alias, credential);
        provider.flush();
        out.println(alias + " has been successfully created.");
        printProviderWritten();
      } catch (InvalidParameterException e) {
        out.println("Credential " + alias + " has NOT been created. " +
            e.getMessage());
        throw e;
      } catch (IOException e) {
        out.println("Credential " + alias + " has NOT been created. " +
            e.getMessage());
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }
  
  protected char[] promptForCredential() throws IOException {
    PasswordReader c = getPasswordReader();
    if (c == null) {
      throw new IOException("No console available for prompting user.");
    }
    
    char[] cred = null;

    boolean noMatch;
    do {
      char[] newPassword1 = c.readPassword("Enter alias password: ");
      char[] newPassword2 = c.readPassword("Enter alias password again: ");
      noMatch = !Arrays.equals(newPassword1, newPassword2);
      if (noMatch) {
        if (newPassword1 != null) {
          Arrays.fill(newPassword1, ' ');
        }
        c.format("Passwords don't match. Try again.%n");
      } else {
        cred = newPassword1;
      }
      if (newPassword2 != null) {
        Arrays.fill(newPassword2, ' ');
      }
    } while (noMatch);
    return cred;
  }
  
  public PasswordReader getPasswordReader() {
    if (passwordReader == null) {
      passwordReader = new PasswordReader();
    }
    return passwordReader;
  }
  
  public void setPasswordReader(PasswordReader reader) {
    passwordReader = reader;
  }
  
  /** To facilitate testing since Console is a final class. */
  public static class PasswordReader {
    public char[] readPassword(String prompt) {
      Console console = System.console();
      char[] pass = console.readPassword(prompt);
      return pass;
    }

    public void format(String message) {
      Console console = System.console();
      console.format(message);
    }
  }
  
  
  /**
   * Main program.
   *
   * @param args
   *          Command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CredentialShell(), args);
    System.exit(res);
  }
}
