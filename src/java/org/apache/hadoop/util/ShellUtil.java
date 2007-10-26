package org.apache.hadoop.util;

import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;

/**
 *  Class to execute a shell.
 *
 */

public class ShellUtil {
    private List<String> exec_cmd;
    private File working_dir;
    private Process process;
    private int exit_code;
    private Map<String, String> environment;

    /**
     * @param args list containing command and command line arguments
     * @param dir Current working directory
     * @throws IOException 
     * @throws InterruptedException
	 */
    public ShellUtil (List<String> args, File dir, Map<String, String> env) {
      exec_cmd = args;
      working_dir = dir;
      environment  = env;
    }
	
    /**
     * Executes the command.
     * @throws IOException
     * @throws InterruptedException
     */
    public void execute() throws IOException, InterruptedException {
      // start the process and wait for it to execute 
      ProcessBuilder builder = new ProcessBuilder(exec_cmd);
      builder.directory(working_dir);
      if (environment != null) {
        builder.environment().putAll(environment);
      }
      process = builder.start();
      exit_code = process.waitFor();
      
    }
    /**
     * @return process
     */
    public Process getProcess() {
      return process;
    }

    /**
     * @return exit-code of the process
     */
    public int getExitCode() {
      return exit_code;
   }
}
