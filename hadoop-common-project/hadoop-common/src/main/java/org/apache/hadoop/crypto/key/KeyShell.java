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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
      "   [-help]\n" +
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

  /**
   * Primary entry point for the KeyShell; called via main().
   *
   * @param args Command line arguments.
   * @return 0 on success and 1 on failure.  This value is passed back to
   * the unix shell, so we must follow shell return code conventions:
   * the return code is an unsigned character, and 0 means success, and
   * small positive integers mean failure.
   * @throws Exception
   */
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
        exitCode = 1;
      }
    } catch (Exception e) {
      e.printStackTrace(err);
      return 1;
    }
    return exitCode;
  }

  /**
   * Parse the command line arguments and initialize the data
   * <pre>
   * % hadoop key create keyName [-size size] [-cipher algorithm]
   *    [-provider providerPath]
   * % hadoop key roll keyName [-provider providerPath]
   * % hadoop key list [-provider providerPath]
   * % hadoop key delete keyName [-provider providerPath] [-i]
   * </pre>
   * @param args Command line arguments.
   * @return 0 on success, 1 on failure.
   * @throws IOException
   */
  private int init(String[] args) throws IOException {
    final Options options = KeyProvider.options(getConf());
    final Map<String, String> attributes = new HashMap<String, String>();

    for (int i = 0; i < args.length; i++) { // parse command line
      boolean moreTokens = (i < args.length - 1);
      if (args[i].equals("create")) {
        String keyName = "-help";
        if (moreTokens) {
          keyName = args[++i];
        }

        command = new CreateCommand(keyName, options);
        if ("-help".equals(keyName)) {
          printKeyShellUsage();
          return 1;
        }
      } else if (args[i].equals("delete")) {
        String keyName = "-help";
        if (moreTokens) {
          keyName = args[++i];
        }

        command = new DeleteCommand(keyName);
        if ("-help".equals(keyName)) {
          printKeyShellUsage();
          return 1;
        }
      } else if (args[i].equals("roll")) {
        String keyName = "-help";
        if (moreTokens) {
          keyName = args[++i];
        }

        command = new RollCommand(keyName);
        if ("-help".equals(keyName)) {
          printKeyShellUsage();
          return 1;
        }
      } else if ("list".equals(args[i])) {
        command = new ListCommand();
      } else if ("-size".equals(args[i]) && moreTokens) {
        options.setBitLength(Integer.parseInt(args[++i]));
      } else if ("-cipher".equals(args[i]) && moreTokens) {
        options.setCipher(args[++i]);
      } else if ("-description".equals(args[i]) && moreTokens) {
        options.setDescription(args[++i]);
      } else if ("-attr".equals(args[i]) && moreTokens) {
        final String attrval[] = args[++i].split("=", 2);
        final String attr = attrval[0].trim();
        final String val = attrval[1].trim();
        if (attr.isEmpty() || val.isEmpty()) {
          out.println("\nAttributes must be in attribute=value form, " +
                  "or quoted\nlike \"attribute = value\"\n");
          printKeyShellUsage();
          return 1;
        }
        if (attributes.containsKey(attr)) {
          out.println("\nEach attribute must correspond to only one value:\n" +
                  "atttribute \"" + attr + "\" was repeated\n" );
          printKeyShellUsage();
          return 1;
        }
        attributes.put(attr, val);
      } else if ("-provider".equals(args[i]) && moreTokens) {
        userSuppliedProvider = true;
        getConf().set(KeyProviderFactory.KEY_PROVIDER_PATH, args[++i]);
      } else if ("-metadata".equals(args[i])) {
        getConf().setBoolean(LIST_METADATA, true);
      } else if ("-i".equals(args[i]) || ("-interactive".equals(args[i]))) {
        interactive = true;
      } else if ("-help".equals(args[i])) {
        printKeyShellUsage();
        return 1;
      } else {
        printKeyShellUsage();
        ToolRunner.printGenericCommandUsage(System.err);
        return 1;
      }
    }

    if (command == null) {
      printKeyShellUsage();
      return 1;
    }

    if (!attributes.isEmpty()) {
      options.setAttributes(attributes);
    }

    return 0;
  }

  private void printKeyShellUsage() {
    out.println(USAGE_PREFIX + COMMANDS);
    if (command != null) {
      out.println(command.getUsage());
    } else {
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
        } else {
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
        out.println(provider + " has been updated.");
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
        "list [-provider <provider>] [-metadata] [-help]";
    public static final String DESC =
        "The list subcommand displays the keynames contained within\n" +
        "a particular provider as configured in core-site.xml or\n" +
        "specified with the -provider argument. -metadata displays\n" +
        "the metadata.";

    private boolean metadata = false;

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no non-transient KeyProviders configured.\n"
          + "Use the -provider option to specify a provider. If you\n"
          + "want to list a transient provider then you must use the\n"
          + "-provider argument.");
        rc = false;
      }
      metadata = getConf().getBoolean(LIST_METADATA, false);
      return rc;
    }

    public void execute() throws IOException {
      try {
        final List<String> keys = provider.getKeys();
        out.println("Listing keys for KeyProvider: " + provider);
        if (metadata) {
          final Metadata[] meta =
            provider.getKeysMetadata(keys.toArray(new String[keys.size()]));
          for (int i = 0; i < meta.length; ++i) {
            out.println(keys.get(i) + " : " + meta[i]);
          }
        } else {
          for (String keyName : keys) {
            out.println(keyName);
          }
        }
      } catch (IOException e) {
        out.println("Cannot list keys for KeyProvider: " + provider
            + ": " + e.toString());
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class RollCommand extends Command {
    public static final String USAGE = "roll <keyname> [-provider <provider>] [-help]";
    public static final String DESC =
      "The roll subcommand creates a new version for the specified key\n" +
      "within the provider indicated using the -provider argument\n";

    String keyName = null;

    public RollCommand(String keyName) {
      this.keyName = keyName;
    }

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no valid KeyProviders configured. The key\n" +
          "has not been rolled. Use the -provider option to specify\n" +
          "a provider.");
        rc = false;
      }
      if (keyName == null) {
        out.println("Please provide a <keyname>.\n" +
          "See the usage description by using -help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws NoSuchAlgorithmException, IOException {
      try {
        warnIfTransientProvider();
        out.println("Rolling key version from KeyProvider: "
            + provider + "\n  for key name: " + keyName);
        try {
          provider.rollNewVersion(keyName);
          provider.flush();
          out.println(keyName + " has been successfully rolled.");
          printProviderWritten();
        } catch (NoSuchAlgorithmException e) {
          out.println("Cannot roll key: " + keyName + " within KeyProvider: "
              + provider + ". " + e.toString());
          throw e;
        }
      } catch (IOException e1) {
        out.println("Cannot roll key: " + keyName + " within KeyProvider: "
            + provider + ". " + e1.toString());
        throw e1;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  private class DeleteCommand extends Command {
    public static final String USAGE = "delete <keyname> [-provider <provider>] [-help]";
    public static final String DESC =
        "The delete subcommand deletes all versions of the key\n" +
        "specified by the <keyname> argument from within the\n" +
        "provider specified -provider.";

    String keyName = null;
    boolean cont = true;

    public DeleteCommand(String keyName) {
      this.keyName = keyName;
    }

    @Override
    public boolean validate() {
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no valid KeyProviders configured. Nothing\n"
          + "was deleted. Use the -provider option to specify a provider.");
        return false;
      }
      if (keyName == null) {
        out.println("There is no keyName specified. Please specify a " +
            "<keyname>. See the usage description with -help.");
        return false;
      }
      if (interactive) {
        try {
          cont = ToolRunner
              .confirmPrompt("You are about to DELETE all versions of "
                  + " key: " + keyName + " from KeyProvider "
                  + provider + ". Continue?:");
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
          + provider);
      if (cont) {
        try {
          provider.deleteKey(keyName);
          provider.flush();
          out.println(keyName + " has been successfully deleted.");
          printProviderWritten();
        } catch (IOException e) {
          out.println(keyName + " has not been deleted. " + e.toString());
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
    public static final String USAGE =
      "create <keyname> [-cipher <cipher>] [-size <size>]\n" +
      "                     [-description <description>]\n" +
      "                     [-attr <attribute=value>]\n" +
      "                     [-provider <provider>] [-help]";
    public static final String DESC =
      "The create subcommand creates a new key for the name specified\n" +
      "by the <keyname> argument within the provider specified by the\n" +
      "-provider argument. You may specify a cipher with the -cipher\n" +
      "argument. The default cipher is currently \"AES/CTR/NoPadding\".\n" +
      "The default keysize is 128. You may specify the requested key\n" +
      "length using the -size argument. Arbitrary attribute=value\n" +
      "style attributes may be specified using the -attr argument.\n" +
      "-attr may be specified multiple times, once per attribute.\n";

    final String keyName;
    final Options options;

    public CreateCommand(String keyName, Options options) {
      this.keyName = keyName;
      this.options = options;
    }

    public boolean validate() {
      boolean rc = true;
      provider = getKeyProvider();
      if (provider == null) {
        out.println("There are no valid KeyProviders configured. No key\n" +
          " was created. You can use the -provider option to specify\n" +
          " a provider to use.");
        rc = false;
      }
      if (keyName == null) {
        out.println("Please provide a <keyname>. See the usage description" +
          " with -help.");
        rc = false;
      }
      return rc;
    }

    public void execute() throws IOException, NoSuchAlgorithmException {
      warnIfTransientProvider();
      try {
        provider.createKey(keyName, options);
        provider.flush();
        out.println(keyName + " has been successfully created with options "
            + options.toString() + ".");
        printProviderWritten();
      } catch (InvalidParameterException e) {
        out.println(keyName + " has not been created. " + e.toString());
        throw e;
      } catch (IOException e) {
        out.println(keyName + " has not been created. " + e.toString());
        throw e;
      } catch (NoSuchAlgorithmException e) {
        out.println(keyName + " has not been created. " + e.toString());
        throw e;
      }
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }
  }

  /**
   * main() entry point for the KeyShell.  While strictly speaking the
   * return is void, it will System.exit() with a return code: 0 is for
   * success and 1 for failure.
   *
   * @param args Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new KeyShell(), args);
    System.exit(res);
  }
}
