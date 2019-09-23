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
package org.apache.hadoop.util;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>GenericOptionsParser</code> is a utility to parse command line
 * arguments generic to the Hadoop framework. 
 * 
 * <code>GenericOptionsParser</code> recognizes several standard command
 * line arguments, enabling applications to easily specify a namenode, a 
 * ResourceManager, additional configuration resources etc.
 * 
 * <h3 id="GenericOptions">Generic Options</h3>
 * 
 * <p>The supported generic options are:
 * <p><blockquote><pre>
 *     -conf &lt;configuration file&gt;     specify a configuration file
 *     -D &lt;property=value&gt;            use value for given property
 *     -fs &lt;local|namenode:port&gt;      specify a namenode
 *     -jt &lt;local|resourcemanager:port&gt;    specify a ResourceManager
 *     -files &lt;comma separated list of files&gt;    specify comma separated
 *                            files to be copied to the map reduce cluster
 *     -libjars &lt;comma separated list of jars&gt;   specify comma separated
 *                            jar files to include in the classpath.
 *     -archives &lt;comma separated list of archives&gt;    specify comma
 *             separated archives to be unarchived on the compute machines.

 * </pre></blockquote><p>
 * 
 * <p>The general command line syntax is:</p>
 * <p><pre><code>
 * bin/hadoop command [genericOptions] [commandOptions]
 * </code></pre><p>
 * 
 * <p>Generic command line arguments <strong>might</strong> modify 
 * <code>Configuration </code> objects, given to constructors.</p>
 * 
 * <p>The functionality is implemented using Commons CLI.</p>
 *
 * <p>Examples:</p>
 * <p><blockquote><pre>
 * $ bin/hadoop dfs -fs darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 * 
 * $ bin/hadoop dfs -D fs.default.name=darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 *     
 * $ bin/hadoop dfs -conf core-site.xml -conf hdfs-site.xml -ls /data
 * list /data directory in dfs with multiple conf files specified.
 *
 * $ bin/hadoop job -D yarn.resourcemanager.address=darwin:8032 -submit job.xml
 * submit a job to ResourceManager darwin:8032
 *
 * $ bin/hadoop job -jt darwin:8032 -submit job.xml
 * submit a job to ResourceManager darwin:8032
 *
 * $ bin/hadoop job -jt local -submit job.xml
 * submit a job to local runner
 * 
 * $ bin/hadoop jar -libjars testlib.jar 
 * -archives test.tgz -files file.txt inputjar args
 * job submission with libjars, files and archives
 * </pre></blockquote><p>
 *
 * @see Tool
 * @see ToolRunner
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GenericOptionsParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(GenericOptionsParser.class);
  private Configuration conf;
  private CommandLine commandLine;
  private final boolean parseSuccessful;

  /**
   * Create an options parser with the given options to parse the args.
   * @param opts the options
   * @param args the command line arguments
   * @throws IOException 
   */
  public GenericOptionsParser(Options opts, String[] args) 
      throws IOException {
    this(new Configuration(), opts, args);
  }

  /**
   * Create an options parser to parse the args.
   * @param args the command line arguments
   * @throws IOException 
   */
  public GenericOptionsParser(String[] args) 
      throws IOException {
    this(new Configuration(), new Options(), args);
  }
  
  /** 
   * Create a <code>GenericOptionsParser</code> to parse only the generic
   * Hadoop arguments.
   * 
   * The array of string arguments other than the generic arguments can be 
   * obtained by {@link #getRemainingArgs()}.
   * 
   * @param conf the <code>Configuration</code> to modify.
   * @param args command-line arguments.
   * @throws IOException 
   */
  public GenericOptionsParser(Configuration conf, String[] args) 
      throws IOException {
    this(conf, new Options(), args); 
  }

  /** 
   * Create a <code>GenericOptionsParser</code> to parse given options as well 
   * as generic Hadoop options. 
   * 
   * The resulting <code>CommandLine</code> object can be obtained by 
   * {@link #getCommandLine()}.
   * 
   * @param conf the configuration to modify  
   * @param options options built by the caller 
   * @param args User-specified arguments
   * @throws IOException 
   */
  public GenericOptionsParser(Configuration conf,
      Options options, String[] args) throws IOException {
    this.conf = conf;
    parseSuccessful = parseGeneralOptions(options, args);
  }

  /**
   * Returns an array of Strings containing only application-specific arguments.
   * 
   * @return array of <code>String</code>s containing the un-parsed arguments
   * or <strong>empty array</strong> if commandLine was not defined.
   */
  public String[] getRemainingArgs() {
    return (commandLine == null) ? new String[]{} : commandLine.getArgs();
  }

  /**
   * Get the modified configuration
   * @return the configuration that has the modified parameters.
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Returns the commons-cli <code>CommandLine</code> object 
   * to process the parsed arguments. 
   * 
   * Note: If the object is created with 
   * {@link #GenericOptionsParser(Configuration, String[])}, then returned 
   * object will only contain parsed generic options.
   * 
   * @return <code>CommandLine</code> representing list of arguments 
   *         parsed against Options descriptor.
   */
  public CommandLine getCommandLine() {
    return commandLine;
  }

  /**
   * Query for the parse operation succeeding.
   * @return true if parsing the CLI was successful
   */
  public boolean isParseSuccessful() {
    return parseSuccessful;
  }

  /**
   * Specify properties of each generic option.
   * <i>Important</i>: as {@link OptionBuilder} is not thread safe, subclasses
   * must synchronize use on {@code OptionBuilder.class}
   */
  @SuppressWarnings("static-access")
  protected Options buildGeneralOptions(Options opts) {
    synchronized (OptionBuilder.class) {
      Option fs = OptionBuilder.withArgName("file:///|hdfs://namenode:port")
          .hasArg()
          .withDescription("specify default filesystem URL to use, "
          + "overrides 'fs.defaultFS' property from configurations.")
          .create("fs");
      Option jt = OptionBuilder.withArgName("local|resourcemanager:port")
          .hasArg()
          .withDescription("specify a ResourceManager")
          .create("jt");
      Option oconf = OptionBuilder.withArgName("configuration file")
          .hasArg()
          .withDescription("specify an application configuration file")
          .create("conf");
      Option property = OptionBuilder.withArgName("property=value")
          .hasArg()
          .withDescription("use value for given property")
          .create('D');
      Option libjars = OptionBuilder.withArgName("paths")
          .hasArg()
          .withDescription(
              "comma separated jar files to include in the classpath.")
          .create("libjars");
      Option files = OptionBuilder.withArgName("paths")
          .hasArg()
          .withDescription("comma separated files to be copied to the " +
              "map reduce cluster")
          .create("files");
      Option archives = OptionBuilder.withArgName("paths")
          .hasArg()
          .withDescription("comma separated archives to be unarchived" +
              " on the compute machines.")
          .create("archives");

      // file with security tokens
      Option tokensFile = OptionBuilder.withArgName("tokensFile")
          .hasArg()
          .withDescription("name of the file with the tokens")
          .create("tokenCacheFile");


      opts.addOption(fs);
      opts.addOption(jt);
      opts.addOption(oconf);
      opts.addOption(property);
      opts.addOption(libjars);
      opts.addOption(files);
      opts.addOption(archives);
      opts.addOption(tokensFile);

      return opts;
    }
  }

  /**
   * Modify configuration according user-specified generic options.
   *
   * @param line User-specified generic options
   */
  private void processGeneralOptions(CommandLine line) throws IOException {
    if (line.hasOption("fs")) {
      FileSystem.setDefaultUri(conf, line.getOptionValue("fs"));
    }

    if (line.hasOption("jt")) {
      String optionValue = line.getOptionValue("jt");
      if (optionValue.equalsIgnoreCase("local")) {
        conf.set("mapreduce.framework.name", optionValue);
      }

      conf.set("yarn.resourcemanager.address", optionValue, 
          "from -jt command line option");
    }
    if (line.hasOption("conf")) {
      String[] values = line.getOptionValues("conf");
      for(String value : values) {
        conf.addResource(new Path(value));
      }
    }

    if (line.hasOption('D')) {
      String[] property = line.getOptionValues('D');
      for(String prop : property) {
        String[] keyval = prop.split("=", 2);
        if (keyval.length == 2) {
          conf.set(keyval[0], keyval[1], "from command line");
        }
      }
    }

    if (line.hasOption("libjars")) {
      // for libjars, we allow expansion of wildcards
      conf.set("tmpjars",
               validateFiles(line.getOptionValue("libjars"), true),
               "from -libjars command line option");
      //setting libjars in client classpath
      URL[] libjars = getLibJars(conf);
      if(libjars!=null && libjars.length>0) {
        conf.setClassLoader(new URLClassLoader(libjars, conf.getClassLoader()));
        Thread.currentThread().setContextClassLoader(
            new URLClassLoader(libjars, 
                Thread.currentThread().getContextClassLoader()));
      }
    }
    if (line.hasOption("files")) {
      conf.set("tmpfiles", 
               validateFiles(line.getOptionValue("files")),
               "from -files command line option");
    }
    if (line.hasOption("archives")) {
      conf.set("tmparchives", 
                validateFiles(line.getOptionValue("archives")),
                "from -archives command line option");
    }
    conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
    
    // tokensFile
    if(line.hasOption("tokenCacheFile")) {
      String fileName = line.getOptionValue("tokenCacheFile");
      // check if the local file exists
      FileSystem localFs = FileSystem.getLocal(conf);
      Path p = localFs.makeQualified(new Path(fileName));
      localFs.getFileStatus(p);
      if(LOG.isDebugEnabled()) {
        LOG.debug("setting conf tokensFile: " + fileName);
      }
      UserGroupInformation.getCurrentUser().addCredentials(
          Credentials.readTokenStorageFile(p, conf));
      conf.set("mapreduce.job.credentials.binary", p.toString(),
               "from -tokenCacheFile command line option");

    }
  }
  
  /**
   * If libjars are set in the conf, parse the libjars.
   * @param conf
   * @return libjar urls
   * @throws IOException
   */
  public static URL[] getLibJars(Configuration conf) throws IOException {
    String jars = conf.get("tmpjars");
    if (jars == null || jars.trim().isEmpty()) {
      return null;
    }
    String[] files = jars.split(",");
    List<URL> cp = new ArrayList<URL>();
    for (String file : files) {
      Path tmp = new Path(file);
      if (tmp.getFileSystem(conf).equals(FileSystem.getLocal(conf))) {
        cp.add(FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL());
      } else {
        LOG.warn("The libjars file " + tmp + " is not on the local " +
            "filesystem. It will not be added to the local classpath.");
      }
    }
    return cp.toArray(new URL[0]);
  }

  /**
   * Takes input as a comma separated list of files
   * and verifies if they exist. It defaults for file:///
   * if the files specified do not have a scheme.
   * it returns the paths uri converted defaulting to file:///.
   * So an input of  /home/user/file1,/home/user/file2 would return
   * file:///home/user/file1,file:///home/user/file2.
   *
   * This method does not recognize wildcards.
   *
   * @param files the input files argument
   * @return a comma-separated list of validated and qualified paths, or null
   * if the input files argument is null
   */
  private String validateFiles(String files) throws IOException {
    return validateFiles(files, false);
  }

  /**
   * takes input as a comma separated list of files
   * and verifies if they exist. It defaults for file:///
   * if the files specified do not have a scheme.
   * it returns the paths uri converted defaulting to file:///.
   * So an input of  /home/user/file1,/home/user/file2 would return
   * file:///home/user/file1,file:///home/user/file2.
   *
   * @param files the input files argument
   * @param expandWildcard whether a wildcard entry is allowed and expanded. If
   * true, any directory followed by a wildcard is a valid entry and is replaced
   * with the list of jars in that directory. It is used to support the wildcard
   * notation in a classpath.
   * @return a comma-separated list of validated and qualified paths, or null
   * if the input files argument is null
   */
  private String validateFiles(String files, boolean expandWildcard)
      throws IOException {
    if (files == null) {
      return null;
    }
    String[] fileArr = files.split(",");
    if (fileArr.length == 0) {
      throw new IllegalArgumentException("File name can't be empty string");
    }
    List<String> finalPaths = new ArrayList<>(fileArr.length);
    for (int i =0; i < fileArr.length; i++) {
      String tmp = fileArr[i];
      if (tmp.isEmpty()) {
        throw new IllegalArgumentException("File name can't be empty string");
      }
      URI pathURI;
      final String wildcard = "*";
      boolean isWildcard = tmp.endsWith(wildcard) && expandWildcard;
      try {
        if (isWildcard) {
          // strip the wildcard
          tmp = tmp.substring(0, tmp.length() - 1);
        }
        // handle the case where a wildcard alone ("*") or the wildcard on the
        // current directory ("./*") is specified
        pathURI = matchesCurrentDirectory(tmp) ?
            new File(Path.CUR_DIR).toURI() :
            new URI(tmp);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
      Path path = new Path(pathURI);
      FileSystem localFs = FileSystem.getLocal(conf);
      if (pathURI.getScheme() == null) {
        //default to the local file system
        //check if the file exists or not first
        localFs.getFileStatus(path);
        if (isWildcard) {
          expandWildcard(finalPaths, path, localFs);
        } else {
          finalPaths.add(path.makeQualified(localFs.getUri(),
              localFs.getWorkingDirectory()).toString());
        }
      } else {
        // check if the file exists in this file system
        // we need to recreate this filesystem object to copy
        // these files to the file system ResourceManager is running
        // on.
        FileSystem fs = path.getFileSystem(conf);
        // existence check
        fs.getFileStatus(path);
        if (isWildcard) {
          expandWildcard(finalPaths, path, fs);
        } else {
          finalPaths.add(path.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString());
        }
      }
    }
    if (finalPaths.isEmpty()) {
      throw new IllegalArgumentException("Path " + files + " cannot be empty.");
    }
    return StringUtils.join(",", finalPaths);
  }

  private boolean matchesCurrentDirectory(String path) {
    return path.isEmpty() || path.equals(Path.CUR_DIR) ||
        path.equals(Path.CUR_DIR + File.separator);
  }

  private void expandWildcard(List<String> finalPaths, Path path, FileSystem fs)
      throws IOException {
    FileStatus status = fs.getFileStatus(path);
    if (!status.isDirectory()) {
      throw new FileNotFoundException(path + " is not a directory.");
    }
    // get all the jars in the directory
    List<Path> jars = FileUtil.getJarsInDirectory(path.toString(),
        fs.equals(FileSystem.getLocal(conf)));
    if (jars.isEmpty()) {
      LOG.warn(path + " does not have jars in it. It will be ignored.");
    } else {
      for (Path jar: jars) {
        finalPaths.add(jar.makeQualified(fs.getUri(),
            fs.getWorkingDirectory()).toString());
      }
    }
  }

  /**
   * Windows powershell and cmd can parse key=value themselves, because
   * /pkey=value is same as /pkey value under windows. However this is not
   * compatible with how we get arbitrary key values in -Dkey=value format.
   * Under windows -D key=value or -Dkey=value might be passed as
   * [-Dkey, value] or [-D key, value]. This method does undo these and
   * return a modified args list by manually changing [-D, key, value]
   * into [-D, key=value]
   *
   * @param args command line arguments
   * @return fixed command line arguments that GnuParser can parse
   */
  private String[] preProcessForWindows(String[] args) {
    if (!Shell.WINDOWS) {
      return args;
    }
    if (args == null) {
      return null;
    }
    List<String> newArgs = new ArrayList<String>(args.length);
    for (int i=0; i < args.length; i++) {
      if (args[i] == null) {
        continue;
      }
      String prop = null;
      if (args[i].equals("-D")) {
        newArgs.add(args[i]);
        if (i < args.length - 1) {
          prop = args[++i];
        }
      } else if (args[i].startsWith("-D")) {
        prop = args[i];
      } else {
        newArgs.add(args[i]);
      }
      if (prop != null) {
        if (prop.contains("=")) {
          // everything good
        } else {
          if (i < args.length - 1) {
            prop += "=" + args[++i];
          }
        }
        newArgs.add(prop);
      }
    }

    return newArgs.toArray(new String[newArgs.size()]);
  }

  /**
   * Parse the user-specified options, get the generic options, and modify
   * configuration accordingly.
   *
   * @param opts Options to use for parsing args.
   * @param args User-specified arguments
   * @return true if the parse was successful
   */
  private boolean parseGeneralOptions(Options opts, String[] args)
      throws IOException {
    opts = buildGeneralOptions(opts);
    CommandLineParser parser = new GnuParser();
    boolean parsed = false;
    try {
      commandLine = parser.parse(opts, preProcessForWindows(args), true);
      processGeneralOptions(commandLine);
      parsed = true;
    } catch(ParseException e) {
      LOG.warn("options parsing failed: "+e.getMessage());

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
    }
    return parsed;
  }

  /**
   * Print the usage message for generic command-line options supported.
   * 
   * @param out stream to print the usage message to.
   */
  public static void printGenericCommandUsage(PrintStream out) {
    out.println("Generic options supported are:");
    out.println("-conf <configuration file>        "
        + "specify an application configuration file");
    out.println("-D <property=value>               "
        + "define a value for a given property");
    out.println("-fs <file:///|hdfs://namenode:port> "
        + "specify default filesystem URL to use, overrides "
        + "'fs.defaultFS' property from configurations.");
    out.println("-jt <local|resourcemanager:port>  "
        + "specify a ResourceManager");
    out.println("-files <file1,...>                "
        + "specify a comma-separated list of files to be copied to the map "
        + "reduce cluster");
    out.println("-libjars <jar1,...>               "
        + "specify a comma-separated list of jar files to be included in the "
        + "classpath");
    out.println("-archives <archive1,...>          "
        + "specify a comma-separated list of archives to be unarchived on the "
        + "compute machines");
    out.println();
    out.println("The general command line syntax is:");
    out.println("command [genericOptions] [commandOptions]");
    out.println();
  }
  
}
