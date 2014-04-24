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

package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.io.PrintStream;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.crypto.key.KeyProvider.Metadata;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program is the CLI utility for the KeyProvider facilities in Hadoop.
 */
public class KeyShell extends Configured implements Tool {
  final static private String USAGE_PREFIX = "Usage: hadoop key " +
  		"[generic options]\n";
  final static private String COMMANDS =
      "   [--help]\n" +
      "   [" + CreateCommand.USAGE + "]\n" +
      "   [" + RollCommand.USAGE + "]\n" +
      "   [" + DeleteCommand.USAGE + "]\n" +
      "   [" + ListCommand.USAGE + "]\n";
  private static final String LIST_METADATA = "keyShell.list.metadata";

  private boolean interactive = false;
  private Command command = null;

  /** allows stdout to be captured if necessary */
  public PrintStream out = System.out;
  /** allows stderr to be captured if necessary */
  public PrintStream err = System.err;

  private boolean userSuppliedProvider = false;

  @Override
  public int run(String[] args) throws Exception {
    int exitCode = 0;
    try {
      exitCode = init(args);
      if (exitCode != 0) {
        return exitCode;
      }
      if (command.validate()) {
          command.execute();
      } else {
        exitCode = -1;
      }
    } catch (Exception e) {
      e.printStackTrace(err);
      return -1;
    }
    return exitCode;
  }

  /**
   * Parse the command line arguments and initialize the data
   * <pre>
   * % hadoop key create keyName [--size size] [--cipher algorithm]
   *    [--provider providerPath]
   * % hadoop key roll keyName [--provider providerPath]
   * % hadoop key list [-provider providerPath]
   * % hadoop key delete keyName [--provider providerPath] [-i]
   * </pre>
   * @param args
   * @return
   * @throws IOException
   */
  private int init(String[] args) throws IOException {
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("create")) {
        String keyName = args[++i];
        command = new CreateCommand(keyName);
        if (keyName.equals("--help")) {
          printKeyShellUsage();
          return -1;
        }
      } else if (args[i].equals("delete")) {
        String keyName = args[++i];
        command = new DeleteCommand(keyName);
        if (keyName.equals("--help")) {
          printKeyShellUsage();
          return -1;
        }
      } else if (args[i].equals("roll")) {
        String keyName = args[++i];
        command = new RollCommand(keyName);
        if (keyName.equals("--help")) {
          printKeyShellUsage();
          return -1;
        }
      } else if (args[i].equals("list")) {
        command = new ListCommand();
      } else if (args[i].equals("--size")) {
        getConf().set(KeyProvider.DEFAULT_BITLENGTH_NAME, args[++i]);
      } else if (args[i].equals("--cipher")) {
        getConf().set(KeyProvider.DEFAULT_CIPHER_NAME, args[++i]);
      } else if (args[i].equals("--provider")) {
        userSuppliedProvider = true;
        getConf().set(KeyProviderFactory.KEY_PROVIDER_PATH, args[++i]);
      } else if (args[i].equals("--metadata")) {
        getConf().setBoolean(LIST_METADATA, true);
      } else if (args[i].equals("-i") || (args[i].equals("--interactive"))) {
        interactive = true;
      } else if (args[i].equals("--help")) {
        printKeyShellUsage();
        return -1;
      } else {
        printKeyShellUsage();
        ToolRunner.printGenericCommandUsage(System.err);
        return -1;
      }
    }
    return 0;
  }

  private void printKeyShellUsage() {
    out.println(USAGE_PREFIX + COMMANDS);
    if (command != null) {
      out.println(command.getUsage());
    }
    else {
      out.println("=========================================================" +
      		"======");
      out.println(CreateCommand.USAGE + ":\n\n" + CreateCommand.DESC);
      out.println("=========================================================" +
          "======");
      out.println(RollCommand.USAGE + ":\n\n" + RollCommand.DESC);
      out.println("=========================================================" +
          "======");
      out.println(DeleteCommand.USAGE + ":\n\n" + DeleteCommand.DESC);
      out.println("=========================================================" +
          "======");
      out.println(ListCommand.USAGE + ":\n\n" + ListCommand.DESC);
    }
  }

  private abstract class Command {
    protected KeyProvider provider = null;

    public boolean validate() {
      return true;
    }

    protected KeyProvider getKeyProvider() {
      KeyProvider provider = null;
      List<KeyProvider> providers;
      try {
        providers = KeyProviderFactory.getProviders(getConf());
        if (userSuppliedProvider) {
          provider = providers.get(0);
        }
        else {
          for (KeyProvider p : providers) {
            if (!p.isTransient()) {
              provider = p;
              break;
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace(err);
      }
      return provider;
    }

    protected void printProviderWritten() {
        out.println(provider.getClass().getName() + " has been updated.");
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
        "list [--provider] [--metadata] [--help]";
    public static final String DESC =
        "The list subcommand displays the keynames contained within \n" +
        "a particular provider - as configured in core-site.xml or " +
        "indicated\nthrough the --provider argument.\n" +
        "If the --metadata option is used, the keys metadata will be printed";

    private boolean metadata = false;

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no non-transient KeyProviders configured.\n"
            + "Consider using the --provider option to indicate the provider\n"
            + "to use. If you want to list a transient provider then you\n"
            + "you MUST use the --provider argument.");
        rc = false;
      }
      metadata = getConf().getBoolean(LIST_METADATA, false);
      return rc;
    }

    public void execute() throws IOException {
      try {
        List<String> keys = provider.getKeys();
        out.println("Listing keys for KeyProvider: " + provider.toString());
        if (metadata) {
          Metadata[] meta =
            provider.getKeysMetadata(keys.toArray(new String[keys.size()]));
          for(int i=0; i < meta.length; ++i) {
            out.println(keys.get(i) + " : " + meta[i]);
          }
        } else {
          for (String keyName : keys) {
            out.println(keyName);
          }
        }
      } catch (IOException e) {
        out.println("Cannot list keys for KeyProvider: " + provider.toString()
            + ": " + e.getMessage());
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class RollCommand extends Command {
    public static final String USAGE = "roll <keyname> [--provider] [--help]";
    public static final String DESC =
        "The roll subcommand creates a new version of the key specified\n" +
        "through the <keyname> argument within the provider indicated using\n" +
        "the --provider argument";

    String keyName = null;

    public RollCommand(String keyName) {
      this.keyName = keyName;
    }

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no valid KeyProviders configured.\n"
            + "Key will not be rolled.\n"
            + "Consider using the --provider option to indicate the provider"
            + " to use.");
        rc = false;
      }
      if (keyName == null) {
        out.println("There is no keyName specified. Please provide the" +
            "mandatory <keyname>. See the usage description with --help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws NoSuchAlgorithmException, IOException {
      try {
        Metadata md = provider.getMetadata(keyName);
        warnIfTransientProvider();
        out.println("Rolling key version from KeyProvider: "
            + provider.toString() + " for key name: " + keyName);
        try {
          provider.rollNewVersion(keyName);
          out.println(keyName + " has been successfully rolled.");
          provider.flush();
          printProviderWritten();
        } catch (NoSuchAlgorithmException e) {
          out.println("Cannot roll key: " + keyName + " within KeyProvider: "
              + provider.toString());
          throw e;
        }
      } catch (IOException e1) {
        out.println("Cannot roll key: " + keyName + " within KeyProvider: "
            + provider.toString());
        throw e1;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class DeleteCommand extends Command {
    public static final String USAGE = "delete <keyname> [--provider] [--help]";
    public static final String DESC =
        "The delete subcommand deletes all of the versions of the key\n" +
        "specified as the <keyname> argument from within the provider\n" +
        "indicated through the --provider argument";

    String keyName = null;
    boolean cont = true;

    public DeleteCommand(String keyName) {
      this.keyName = keyName;
    }

    @Override
    public boolean validate() {
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no valid KeyProviders configured.\n"
            + "Nothing will be deleted.\n"
            + "Consider using the --provider option to indicate the provider"
            + " to use.");
        return false;
      }
      if (keyName == null) {
        out.println("There is no keyName specified. Please provide the" +
            "mandatory <keyname>. See the usage description with --help.");
        return false;
      }
      if (interactive) {
        try {
          cont = ToolRunner
              .confirmPrompt("You are about to DELETE all versions of "
                  + "the key: " + keyName + " from KeyProvider "
                  + provider.toString() + ". Continue?:");
          if (!cont) {
            out.println("Nothing has been be deleted.");
          }
          return cont;
        } catch (IOException e) {
          out.println(keyName + " will not be deleted.");
          e.printStackTrace(err);
        }
      }
      return true;
    }

    public void execute() throws IOException {
      warnIfTransientProvider();
      out.println("Deleting key: " + keyName + " from KeyProvider: "
          + provider.toString());
      if (cont) {
        try {
          provider.deleteKey(keyName);
          out.println(keyName + " has been successfully deleted.");
          provider.flush();
          printProviderWritten();
        } catch (IOException e) {
          out.println(keyName + "has NOT been deleted.");
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
    public static final String USAGE = "create <keyname> [--cipher] " +
    		"[--size] [--provider] [--help]";
    public static final String DESC =
        "The create subcommand creates a new key for the name specified\n" +
        "as the <keyname> argument within the provider indicated through\n" +
        "the --provider argument. You may also indicate the specific\n" +
        "cipher through the --cipher argument. The default for cipher is\n" +
        "currently \"AES/CTR/NoPadding\". The default keysize is \"256\".\n" +
        "You may also indicate the requested key length through the --size\n" +
        "argument.";

    String keyName = null;

    public CreateCommand(String keyName) {
      this.keyName = keyName;
    }

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no valid KeyProviders configured.\nKey" +
        		" will not be created.\n"
            + "Consider using the --provider option to indicate the provider" +
            " to use.");
        rc = false;
      }
      if (keyName == null) {
        out.println("There is no keyName specified. Please provide the" +
        		"mandatory <keyname>. See the usage description with --help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws IOException, NoSuchAlgorithmException {
      warnIfTransientProvider();
      try {
        Options options = KeyProvider.options(getConf());
        provider.createKey(keyName, options);
        out.println(keyName + " has been successfully created.");
        provider.flush();
        printProviderWritten();
      } catch (InvalidParameterException e) {
        out.println(keyName + " has NOT been created. " + e.getMessage());
        throw e;
      } catch (IOException e) {
        out.println(keyName + " has NOT been created. " + e.getMessage());
        throw e;
      } catch (NoSuchAlgorithmException e) {
        out.println(keyName + " has NOT been created. " + e.getMessage());
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
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
    int res = ToolRunner.run(new Configuration(), new KeyShell(), args);
    System.exit(res);
  }
}
