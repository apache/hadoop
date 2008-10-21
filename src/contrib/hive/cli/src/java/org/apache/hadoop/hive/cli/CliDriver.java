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

package org.apache.hadoop.hive.cli;

import jline.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class CliDriver {

  public final static String prompt = "hive";
  public final static String prompt2 = "    "; // when ';' is not yet seen

  public static SetProcessor sp;
  public static Driver qp;
  public static FsShell dfs;

  public CliDriver(CliSessionState ss) {
    SessionState.start(ss);
    sp = new SetProcessor();
    qp = new Driver();
  }
  
  public static int processCmd(String cmd) {
    String[] tokens = cmd.split("\\s+");
    String cmd_1 = cmd.substring(tokens[0].length());
    int ret = 0;
    
    if(tokens[0].equals("set")) {
      ret = sp.run(cmd_1);
    } else if (cmd.equals("quit") || cmd.equals("exit")) {
      // if we have come this far - either the previous commands
      // are all successful or this is command line. in either case
      // this counts as a successful run
      System.exit(0);
    } else if (cmd.startsWith("!")) {
      SessionState ss = SessionState.get();
      String shell_cmd = cmd.substring(1);
      if (shell_cmd.endsWith(";")) {
        shell_cmd = shell_cmd.substring(0, shell_cmd.length()-1);
      }

      //shell_cmd = "/bin/bash -c \'" + shell_cmd + "\'";
      try {
        Process executor = Runtime.getRuntime().exec(shell_cmd);
        StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, ss.out);
        StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, ss.err);
        
        outPrinter.start();
        errPrinter.start();
      
        int exitVal = executor.waitFor();
        if (exitVal != 0) {
          ss.err.write((new String("Command failed with exit code = " + exitVal)).getBytes());
        }
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    } else if (cmd.startsWith("dfs")) {
      // dfs shell commands
      SessionState ss = SessionState.get();
      if(dfs == null)
        dfs = new FsShell(ss.getConf());
      String hadoopCmd = cmd.replaceFirst("dfs\\s+", "");
      hadoopCmd = hadoopCmd.trim();
      if (hadoopCmd.endsWith(";")) {
        hadoopCmd = hadoopCmd.substring(0, hadoopCmd.length()-1);
      }
      String[] args = hadoopCmd.split("\\s+");
      try {
        PrintStream oldOut = System.out;
        System.setOut(ss.out);
        int val = dfs.run(args);
        System.setOut(oldOut);
        if (val != 0) {
          ss.err.write((new String("Command failed with exit code = " + val)).getBytes());
        }
      } catch (Exception e) {
        ss.err.println("Exception raised from DFSShell.run " + e.getLocalizedMessage()); 
      }
    } else {
      ret = qp.run(cmd);
      Vector<String> res = new Vector<String>();
      while (qp.getResults(res)) {
      	for (String r:res) {
          SessionState ss  = SessionState.get();
          PrintStream out = ss.out;
          out.println(r);
      	}
        res.clear();
      }

      int cret = qp.close();
      if (ret == 0) {
        ret = cret;
      }
    }

    return ret;
  }

  public static int processLine(String line) {
    int ret = 0;
    for(String oneCmd: line.split(";")) {
      oneCmd = oneCmd.trim();
      if(oneCmd.equals(""))
        continue;
      
      ret = processCmd(oneCmd);
      if(ret != 0) {
        // ignore anything after the first failed command
        return ret;
      }
    }
    return 0;
  }

  public static int processReader(BufferedReader r) throws IOException {
    String line;
    int ret = 0;
    while((line = r.readLine()) != null) {
      ret = processLine(line);
      if(ret != 0) {
        return ret;
      }
    }
    return 0;
  }

  public static void main(String[] args) throws IOException {

    OptionsProcessor oproc = new OptionsProcessor();
    if(! oproc.process_stage1(args)) {
      System.exit(1);
    }

    // NOTE: It is critical to do this here so that log4j is reinitialized before
    // any of the other core hive classes are loaded
    SessionState.initHiveLog4j();

    CliSessionState ss = new CliSessionState (new HiveConf(SessionState.class));
    ss.in = System.in;
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8");
      ss.err = new PrintStream(System.err, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      System.exit(3);
    }

    SessionState.start(ss);

    if(! oproc.process_stage2(ss)) {
      System.exit(2);
    }

    // set all properties specified via command line
    HiveConf conf = ss.getConf();
    for(Map.Entry<Object, Object> item: ss.cmdProperties.entrySet()) {
      conf.set((String) item.getKey(), (String) item.getValue());
    }

    sp = new SetProcessor();
    qp = new Driver();
    dfs = new FsShell(ss.getConf());

    if(ss.execString != null) {
      System.exit(processLine(ss.execString));
    }

    try {
      if(ss.fileName != null) {
        System.exit(processReader(new BufferedReader(new FileReader(ss.fileName))));
      }
    } catch (FileNotFoundException e) {
      System.err.println("Could not open input file for reading. ("+e.getMessage()+")");
      System.exit(3);
    }

    Character mask = null;
    String trigger = null;

    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(false);
    //reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));

    List<SimpleCompletor> completors = new LinkedList<SimpleCompletor>();
    completors.add(new SimpleCompletor(new String[] { "set", "from",
                                                      "create", "load",
                                                      "describe", "quit", "exit" }));
    reader.addCompletor(new ArgumentCompletor(completors));
    
    String line;
    PrintWriter out = new PrintWriter(System.out);
    final String HISTORYFILE = ".hivehistory";
    String historyFile = System.getProperty("user.home") + File.separator  + HISTORYFILE;
    reader.setHistory(new History(new File(historyFile)));
    int ret = 0;
    Log LOG = LogFactory.getLog("CliDriver");
    LogHelper console = new LogHelper(LOG);
    String prefix = "";
    String curPrompt = prompt;
    while ((line = reader.readLine(curPrompt+"> ")) != null) {
      long start = System.currentTimeMillis();
      if(line.trim().endsWith(";")) {
        line = prefix + " " + line;
        ret = processLine(line);
        prefix = "";
        curPrompt = prompt;
      } else {
        prefix = prefix + line;
        curPrompt = prompt2;
        continue;
      }
      long end = System.currentTimeMillis();
      if (end > start) {
        double timeTaken = (double)(end-start)/1000.0;
        console.printInfo("Time taken: " + timeTaken + " seconds", null);
      }
    }

    System.exit(ret);
  }
}
