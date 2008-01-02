package org.apache.hadoop.util;

import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 *  Class to execute a shell.
 *  @deprecated Use {@link ShellCommandExecutor} instead.
 */
public class ShellUtil {
  
  ShellCommandExecutor shexec; // shell to execute a command
  
    /**
     * @param args list containing command and command line arguments
     * @param dir Current working directory
     * @param env Environment for the command
     */
    public ShellUtil (List<String> args, File dir, Map<String, String> env) {
      shexec = new ShellCommandExecutor(args.toArray(new String[0]), dir, 
                                         env);
    }
	
    /**
     * Executes the command.
     * @throws IOException
     * @throws InterruptedException
     */
    public void execute() throws IOException {
      // start the process and wait for it to execute
      shexec.execute();
    }
    /**
     * @return process
     */
    public Process getProcess() {
      return shexec.getProcess();
    }

    /**
     * @return exit-code of the process
     */
    public int getExitCode() {
      return shexec.getExitCode();
   }
}
