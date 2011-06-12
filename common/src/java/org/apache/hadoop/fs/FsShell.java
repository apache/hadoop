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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathExceptions.PathNotFoundException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Provide command line access to a FileSystem. */
@InterfaceAudience.Private
public class FsShell extends Configured implements Tool {
  
  static final Log LOG = LogFactory.getLog(FsShell.class);

  private FileSystem fs;
  private Trash trash;
  protected CommandFactory commandFactory;

  /**
   */
  public FsShell() {
    this(null);
  }

  public FsShell(Configuration conf) {
    super(conf);
    fs = null;
    trash = null;
    commandFactory = new CommandFactory();
  }
  
  protected FileSystem getFS() throws IOException {
    if(fs == null)
      fs = FileSystem.get(getConf());
    
    return fs;
  }
  
  protected Trash getTrash() throws IOException {
    if (this.trash == null) {
      this.trash = new Trash(getConf());
    }
    return this.trash;
  }
  
  protected void init() throws IOException {
    getConf().setQuietMode(true);
  }

  /**
   * Returns the Trash object associated with this shell.
   */
  public Path getCurrentTrashDir() throws IOException {
    return getTrash().getCurrentTrashDir();
  }

  /**
   * Return an abbreviated English-language desc of the byte length
   * @deprecated Consider using {@link org.apache.hadoop.util.StringUtils#byteDesc} instead.
   */
  @Deprecated
  public static String byteDesc(long len) {
    return StringUtils.byteDesc(len);
  }

  /**
   * @deprecated Consider using {@link org.apache.hadoop.util.StringUtils#limitDecimalTo2} instead.
   */
  @Deprecated
  public static synchronized String limitDecimalTo2(double d) {
    return StringUtils.limitDecimalTo2(d);
  }

  private void printHelp(String cmd) {
    String summary = "hadoop fs is the command to execute fs commands. " +
      "The full syntax is: \n\n" +
      "hadoop fs [-fs <local | file system URI>] [-conf <configuration file>]\n\t" +
      "[-D <property=value>]\n\t" +
      "[-report]";

    String conf ="-conf <configuration file>:  Specify an application configuration file.";
 
    String D = "-D <property=value>:  Use value for given property.";
  
    String fs = "-fs [local | <file system URI>]: \tSpecify the file system to use.\n" + 
      "\t\tIf not specified, the current configuration is used, \n" +
      "\t\ttaken from the following, in increasing precedence: \n" + 
      "\t\t\tcore-default.xml inside the hadoop jar file \n" +
      "\t\t\tcore-site.xml in $HADOOP_CONF_DIR \n" +
      "\t\t'local' means use the local file system as your DFS. \n" +
      "\t\t<file system URI> specifies a particular file system to \n" +
      "\t\tcontact. This argument is optional but if used must appear\n" +
      "\t\tappear first on the command line.  Exactly one additional\n" +
      "\t\targument must be specified. \n";

    String help = "-help [cmd]: \tDisplays help for given command or all commands if none\n" +
      "\t\tis specified.\n";

    Command instance = commandFactory.getInstance("-" + cmd);
    if (instance != null) {
      printHelp(instance);
    } else if ("fs".equals(cmd)) {
      System.out.println(fs);
    } else if ("conf".equals(cmd)) {
      System.out.println(conf);
    } else if ("D".equals(cmd)) {
      System.out.println(D);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      for (String thisCmdName : commandFactory.getNames()) {
        instance = commandFactory.getInstance(thisCmdName);
        if (!instance.isDeprecated()) {
          System.out.println("\t[" + instance.getUsage() + "]");
        }
      }
      System.out.println("\t[-help [cmd]]\n");
      
      System.out.println(fs);

      for (String thisCmdName : commandFactory.getNames()) {
        instance = commandFactory.getInstance(thisCmdName);
        if (!instance.isDeprecated()) {
          printHelp(instance);
        }
      }
      System.out.println(help);
    }        
  }

  // TODO: will eventually auto-wrap the text, but this matches the expected
  // output for the hdfs tests...
  private void printHelp(Command instance) {
    boolean firstLine = true;
    for (String line : instance.getDescription().split("\n")) {
      String prefix;
      if (firstLine) {
        prefix = instance.getUsage() + ":\t";
        firstLine = false;
      } else {
        prefix = "\t\t";
      }
      System.out.println(prefix + line);
    }    
  }
  
  /**
   * Displays format of commands.
   * 
   */
  private void printUsage(String cmd) {
    String prefix = "Usage: java " + FsShell.class.getSimpleName();

    Command instance = commandFactory.getInstance(cmd);
    if (instance != null) {
      System.err.println(prefix + " [" + instance.getUsage() + "]");
    } else if ("-fs".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-fs <local | file system URI>]");
    } else if ("-conf".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-conf <configuration file>]");
    } else if ("-D".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-D <[property=value>]");
    } else {
      System.err.println("Usage: java FsShell");
      for (String name : commandFactory.getNames()) {
        instance = commandFactory.getInstance(name);
        if (!instance.isDeprecated()) {
          System.err.println("           [" + instance.getUsage() + "]");
        }
      }
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {
    // TODO: This isn't the best place, but this class is being abused with
    // subclasses which of course override this method.  There really needs
    // to be a better base class for all commands
    commandFactory.setConf(getConf());
    commandFactory.registerCommands(FsCommand.class);
    
    if (argv.length < 1) {
      printUsage(""); 
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];
    // initialize FsShell
    try {
      init();
    } catch (RPC.VersionMismatch v) {
      LOG.debug("Version mismatch", v);
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      LOG.debug("Error", e);
      System.err.println("Bad connection to FS. Command aborted. Exception: " +
          e.getLocalizedMessage());
      return exitCode;
    }

    try {
      Command instance = commandFactory.getInstance(cmd);
      if (instance != null) {
        exitCode = instance.run(Arrays.copyOfRange(argv, i, argv.length));
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else {
        System.err.println(cmd + ": Unknown command");
        printUsage("");
      }
    } catch (Exception e) {
      exitCode = 1;
      LOG.debug("Error", e);
      displayError(cmd, e);
      if (e instanceof IllegalArgumentException) {
        exitCode = -1;
        printUsage(cmd);
      }
    }
    return exitCode;
  }

  // TODO: this is a quick workaround to accelerate the integration of
  // redesigned commands.  this will be removed this once all commands are
  // converted.  this change will avoid having to change the hdfs tests
  // every time a command is converted to use path-based exceptions
  private static Pattern[] fnfPatterns = {
    Pattern.compile("File (.*) does not exist\\."),
    Pattern.compile("File does not exist: (.*)"),
    Pattern.compile("`(.*)': specified destination directory doest not exist")
  };
  private void displayError(String cmd, Exception e) {
    String message = e.getLocalizedMessage().split("\n")[0];
    for (Pattern pattern : fnfPatterns) {
      Matcher matcher = pattern.matcher(message);
      if (matcher.matches()) {
        message = new PathNotFoundException(matcher.group(1)).getMessage();
        break;
      }
    }
    System.err.println(cmd.substring(1) + ": " + message);  
  }
  
  public void close() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    FsShell shell = new FsShell();
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }
}
