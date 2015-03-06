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
package org.apache.hadoop.yarn.server.nodemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO.Windows;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

/**
 * Windows secure container executor (WSCE).
 * This class offers a secure container executor on Windows, similar to the 
 * LinuxContainerExecutor. As the NM does not run on a high privileged context, 
 * this class delegates elevated operations to the helper hadoopwintuilsvc, 
 * implemented by the winutils.exe running as a service.
 * JNI and LRPC is used to communicate with the privileged service.
 */
public class WindowsSecureContainerExecutor extends DefaultContainerExecutor {
  
  private static final Log LOG = LogFactory
      .getLog(WindowsSecureContainerExecutor.class);
  
  public static final String LOCALIZER_PID_FORMAT = "STAR_LOCALIZER_%s";
  
  
  /**
   * This class is a container for the JNI Win32 native methods used by WSCE.
   */
  private static class Native {

    private static boolean nativeLoaded = false;

    static {
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        try {
          initWsceNative();
          nativeLoaded = true;
        } catch (Throwable t) {
          LOG.info("Unable to initialize WSCE Native libraries", t);
        }
      }
    }

    /** Initialize the JNI method ID and class ID cache */
    private static native void initWsceNative();
    
    
    /**
     * This class contains methods used by the WindowsSecureContainerExecutor
     * file system operations.
     */
    public static class Elevated {
      private static final int MOVE_FILE = 1;
      private static final int COPY_FILE = 2;

      public static void mkdir(Path dirName) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for mkdir");
        }
        elevatedMkDirImpl(dirName.toString());
      }
      
      private static native void elevatedMkDirImpl(String dirName) 
          throws IOException;
      
      public static void chown(Path fileName, String user, String group) 
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for chown");
        }
        elevatedChownImpl(fileName.toString(), user, group);
      }
      
      private static native void elevatedChownImpl(String fileName, String user, 
          String group) throws IOException;
      
      public static void move(Path src, Path dst, boolean replaceExisting) 
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for move");
        }
        elevatedCopyImpl(MOVE_FILE, src.toString(), dst.toString(), 
            replaceExisting);
      }
      
      public static void copy(Path src, Path dst, boolean replaceExisting) 
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for copy");
        }
        elevatedCopyImpl(COPY_FILE, src.toString(), dst.toString(), 
            replaceExisting);
      }
      
      private static native void elevatedCopyImpl(int operation, String src, 
          String dst, boolean replaceExisting) throws IOException;
      
      public static void chmod(Path fileName, int mode) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for chmod");
        }
        elevatedChmodImpl(fileName.toString(), mode);
      }
      
      private static native void elevatedChmodImpl(String path, int mode) 
          throws IOException;
      
      public static void killTask(String containerName) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for killTask");
        }
        elevatedKillTaskImpl(containerName);
      }
      
      private static native void elevatedKillTaskImpl(String containerName) 
          throws IOException;

      public static OutputStream create(Path f, boolean append)  
          throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for create");
        }
        
        long desiredAccess = Windows.GENERIC_WRITE;
        long shareMode = 0L;
        long creationDisposition = append ? 
            Windows.OPEN_ALWAYS : Windows.CREATE_ALWAYS;
        long flags = Windows.FILE_ATTRIBUTE_NORMAL;
        
        String fileName = f.toString();
        fileName = fileName.replace('/', '\\');
        
        long hFile = elevatedCreateImpl(
            fileName, desiredAccess, shareMode, creationDisposition, flags);
        return new FileOutputStream(
            WinutilsProcessStub.getFileDescriptorFromHandle(hFile));
      }
      
      private static native long elevatedCreateImpl(String path, 
          long desiredAccess, long shareMode,
          long creationDisposition, long flags) throws IOException;
      
      
      public static boolean deleteFile(Path path) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for deleteFile");
        }
        
        return elevatedDeletePathImpl(path.toString(), false);
      }

      public static boolean deleteDirectory(Path path) throws IOException {
        if (!nativeLoaded) {
          throw new IOException("Native WSCE libraries are required for deleteDirectory");
        }
        
        return elevatedDeletePathImpl(path.toString(), true);
      }

      public native static boolean elevatedDeletePathImpl(String path, 
          boolean isDir) throws IOException;
    }

    /**
     * Wraps a process started by the winutils service helper.
     *
     */
    public static class WinutilsProcessStub extends Process {
      
      private final long hProcess;
      private final long hThread;
      private boolean disposed = false;
      
      private final InputStream stdErr;
      private final InputStream stdOut;
      private final OutputStream stdIn;
      
      public WinutilsProcessStub(long hProcess, long hThread, long hStdIn, 
          long hStdOut, long hStdErr) {
        this.hProcess = hProcess;
        this.hThread = hThread;
        
        this.stdIn = new FileOutputStream(getFileDescriptorFromHandle(hStdIn));
        this.stdOut = new FileInputStream(getFileDescriptorFromHandle(hStdOut));
        this.stdErr = new FileInputStream(getFileDescriptorFromHandle(hStdErr));
      }
      
      public static native FileDescriptor getFileDescriptorFromHandle(long handle);
      
      @Override
      public native void destroy();
      
      @Override
      public native int exitValue();
      
      @Override
      public InputStream getErrorStream() {
        return stdErr;
      }
      @Override
      public InputStream getInputStream() {
        return stdOut;
      }
      @Override
      public OutputStream getOutputStream() {
        return stdIn;
      }
      @Override
      public native int waitFor() throws InterruptedException;

      public synchronized native void dispose();

      public native void resume() throws NativeIOException;
    }
    
    public synchronized static WinutilsProcessStub createTaskAsUser(
        String cwd, String jobName, String user, String pidFile, String cmdLine)
      throws IOException {
      if (!nativeLoaded) {
        throw new IOException(
            "Native WSCE  libraries are required for createTaskAsUser");
      }
      synchronized(Shell.WindowsProcessLaunchLock) {
        return createTaskAsUser0(cwd, jobName, user, pidFile, cmdLine);
      }
    }

    private static native WinutilsProcessStub createTaskAsUser0(
      String cwd, String jobName, String user, String pidFile, String cmdLine)
      throws NativeIOException;
  }

  /**
   * A shell script wrapper builder for WSCE.  
   * Overwrites the default behavior to remove the creation of the PID file in 
   * the script wrapper. WSCE creates the pid file as part of launching the 
   * task in winutils.
   */
  private class WindowsSecureWrapperScriptBuilder 
    extends LocalWrapperScriptBuilder {

    public WindowsSecureWrapperScriptBuilder(Path containerWorkDir) {
      super(containerWorkDir);
    }

    @Override
    protected void writeLocalWrapperScript(Path launchDst, Path pidFile, PrintStream pout) {
      pout.format("@call \"%s\"", launchDst);
    }
  }

  /**
   * This is a skeleton file system used to elevate certain operations.
   * WSCE has to create container dirs under local/userchache/$user but
   * this dir itself is owned by $user, with chmod 750. As ther NM has no
   * write access, it must delegate the write operations to the privileged
   * hadoopwintuilsvc.
   */
  private static class ElevatedFileSystem extends DelegateToFileSystem {

    /**
     * This overwrites certain RawLocalSystem operations to be performed by a 
     * privileged process.
     * 
     */
    private static class ElevatedRawLocalFilesystem extends RawLocalFileSystem {
      
      @Override
      protected boolean mkOneDirWithMode(Path path, File p2f,
          FsPermission permission) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:mkOneDirWithMode: %s %s", path,
              permission));
        }
        boolean ret = false;

        // File.mkdir returns false, does not throw. Must mimic it.
        try {
          Native.Elevated.mkdir(path);
          setPermission(path, permission);
          ret = true;
        }
        catch(Throwable e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("EFS:mkOneDirWithMode: %s",
                org.apache.hadoop.util.StringUtils.stringifyException(e)));
          }
        }
        return ret;
      }
      
      @Override
      public void setPermission(Path p, FsPermission permission) 
          throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:setPermission: %s %s", p, permission));
        }
        Native.Elevated.chmod(p, permission.toShort());
      }
      
      @Override
      public void setOwner(Path p, String username, String groupname) 
          throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:setOwner: %s %s %s", 
              p, username, groupname));
        }
        Native.Elevated.chown(p, username, groupname);
      }
      
      @Override
      protected OutputStream createOutputStreamWithMode(Path f, boolean append,
          FsPermission permission) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:createOutputStreamWithMode: %s %b %s", f,
              append, permission));
        }
        boolean success = false;
        OutputStream os = Native.Elevated.create(f, append);
        try {
          setPermission(f, permission);
          success = true;
          return os;
        } finally {
          if (!success) {
            IOUtils.cleanup(LOG, os);
          }
        }
      }
      
      @Override
      public boolean delete(Path p, boolean recursive) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:delete: %s %b", p, recursive));
        }
        
        // The super delete uses the FileUtil.fullyDelete, 
        // but we cannot rely on that because we need to use the elevated 
        // operations to remove the files
        //
        File f = pathToFile(p);
        if (!f.exists()) {
          //no path, return false "nothing to delete"
          return false;
        }
        else if (f.isFile()) {
          return Native.Elevated.deleteFile(p);
        } 
        else if (f.isDirectory()) {
          
          // This is a best-effort attempt. There are race conditions in that
          // child files can be created/deleted after we snapped the list. 
          // No need to protect against that case.
          File[] files = FileUtil.listFiles(f);
          int childCount = files.length;
          
          if (recursive) {
            for(File child:files) {
              if (delete(new Path(child.getPath()), recursive)) {
                --childCount;
              }
            }
          }
          if (childCount == 0) {
            return Native.Elevated.deleteDirectory(p);
          } 
          else {
            throw new IOException("Directory " + f.toString() + " is not empty");
          }
        }
        else {
          // This can happen under race conditions if an external agent 
          // is messing with the file type between IFs
          throw new IOException("Path " + f.toString() + 
              " exists, but is neither a file nor a directory");
        }
      }
    }

    protected ElevatedFileSystem() throws IOException, URISyntaxException {
      super(FsConstants.LOCAL_FS_URI,
          new ElevatedRawLocalFilesystem(), 
          new Configuration(),
          FsConstants.LOCAL_FS_URI.getScheme(),
          false);
    }
  }
  
  private static class WintuilsProcessStubExecutor 
  implements Shell.CommandExecutor {
    private Native.WinutilsProcessStub processStub;
    private StringBuilder output = new StringBuilder();
    private int exitCode;
    
    private enum State {
      INIT,
      RUNNING,
      COMPLETE
    };
    
    private State state;;
    
    private final String cwd;
    private final String jobName;
    private final String userName;
    private final String pidFile;
    private final String cmdLine;

    public WintuilsProcessStubExecutor(
        String cwd, 
        String jobName, 
        String userName, 
        String pidFile,
        String cmdLine) {
      this.cwd = cwd;
      this.jobName = jobName;
      this.userName = userName;
      this.pidFile = pidFile;
      this.cmdLine = cmdLine;
      this.state = State.INIT;
    }    
    
    private void assertComplete() throws IOException {
      if (state != State.COMPLETE) {
        throw new IOException("Process is not complete");
      }
    }
    
    public String getOutput () throws IOException {
      assertComplete();
      return output.toString();
    }
    
    public int getExitCode() throws IOException {
      assertComplete();
      return exitCode;
    }
    
    public void validateResult() throws IOException {
      assertComplete();
      if (0 != exitCode) {
        LOG.warn(output.toString());
        throw new IOException("Processs exit code is:" + exitCode);
      }
    }
    
    private Thread startStreamReader(final InputStream stream) 
        throws IOException {
      Thread streamReaderThread = new Thread() {
        
        @Override
        public void run() {
          try
          {
            BufferedReader lines = new BufferedReader(
                new InputStreamReader(stream, Charset.forName("UTF-8")));
            char[] buf = new char[512];
            int nRead;
            while ((nRead = lines.read(buf, 0, buf.length)) > 0) {
              output.append(buf, 0, nRead);
            }
          }
          catch(Throwable t) {
            LOG.error("Error occured reading the process stdout", t);
          }
        }
      };
      streamReaderThread.start();
      return streamReaderThread;
    }

    public void execute() throws IOException {
      if (state != State.INIT) {
        throw new IOException("Process is already started");
      }
      processStub = Native.createTaskAsUser(cwd,
          jobName, userName, pidFile, cmdLine);
      state = State.RUNNING;

      Thread stdOutReader = startStreamReader(processStub.getInputStream());
      Thread stdErrReader = startStreamReader(processStub.getErrorStream());
      
      try {
        processStub.resume();
        processStub.waitFor();
        stdOutReader.join();
        stdErrReader.join();
      }
      catch(InterruptedException ie) {
        throw new IOException(ie);
      }
      
      exitCode = processStub.exitValue();
      state = State.COMPLETE;
    }

    @Override
    public void close() {
      if (processStub != null) {
        processStub.dispose();
      }
    }
  }

  private String nodeManagerGroup;
  
  /** 
   * Permissions for user WSCE dirs.
   */
  static final short DIR_PERM = (short)0750;  
  
  public WindowsSecureContainerExecutor() 
      throws IOException, URISyntaxException {
    super(FileContext.getFileContext(new ElevatedFileSystem(), 
        new Configuration()));
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    nodeManagerGroup = conf.get(
        YarnConfiguration.NM_WINDOWS_SECURE_CONTAINER_GROUP);
  }
  
  @Override
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration conf) {
    File f = new File(command);
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("getRunCommand: %s exists:%b", 
          command, f.exists()));
    }
    return new String[] { Shell.WINUTILS, "task", "createAsUser", groupId, 
        userName, pidFile.toString(), "cmd /c " + command };
  }
  
  @Override
  protected LocalWrapperScriptBuilder getLocalWrapperScriptBuilder(
      String containerIdStr, Path containerWorkDir) {
   return  new WindowsSecureWrapperScriptBuilder(containerWorkDir);
  }
  
  @Override
  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("copyFile: %s -> %s owner:%s", src.toString(), 
          dst.toString(), owner));
    }
    Native.Elevated.copy(src,  dst, true);
    Native.Elevated.chown(dst, owner, nodeManagerGroup);
  }

  @Override
  protected void createDir(Path dirPath, FsPermission perms,
      boolean createParent, String owner) throws IOException {
    
    // WSCE requires dirs to be 750, not 710 as DCE.
    // This is similar to how LCE creates dirs
    //
    perms = new FsPermission(DIR_PERM);
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("createDir: %s perm:%s owner:%s", 
          dirPath.toString(), perms.toString(), owner));
    }
    
    super.createDir(dirPath, perms, createParent, owner);
    lfs.setOwner(dirPath, owner, nodeManagerGroup);
  }

  @Override
  protected void setScriptExecutable(Path script, String owner) 
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("setScriptExecutable: %s owner:%s", 
          script.toString(), owner));
    }
    super.setScriptExecutable(script, owner);
    Native.Elevated.chown(script, owner, nodeManagerGroup);
  }

  @Override
  public Path localizeClasspathJar(Path classPathJar, Path pwd, String owner) 
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("localizeClasspathJar: %s %s o:%s", 
          classPathJar, pwd, owner));
    }
    createDir(pwd,  new FsPermission(DIR_PERM), true, owner);
    String fileName = classPathJar.getName();
    Path dst = new Path(pwd, fileName);
    Native.Elevated.move(classPathJar, dst, true);
    Native.Elevated.chown(dst, owner, nodeManagerGroup);
    return dst;
  }

 @Override
 public void startLocalizer(Path nmPrivateContainerTokens,
     InetSocketAddress nmAddr, String user, String appId, String locId,
     LocalDirsHandlerService dirsHandler) throws IOException,
     InterruptedException {
   
     List<String> localDirs = dirsHandler.getLocalDirs();
     List<String> logDirs = dirsHandler.getLogDirs();
     
     Path classpathJarPrivateDir = dirsHandler.getLocalPathForWrite(
         ResourceLocalizationService.NM_PRIVATE_DIR);
     createUserLocalDirs(localDirs, user);
     createUserCacheDirs(localDirs, user);
     createAppDirs(localDirs, user, appId);
     createAppLogDirs(appId, logDirs, user);

     Path appStorageDir = getWorkingDir(localDirs, user, appId);
     
     String tokenFn = String.format(
         ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId);
     Path tokenDst = new Path(appStorageDir, tokenFn);
     copyFile(nmPrivateContainerTokens, tokenDst, user);

     File cwdApp = new File(appStorageDir.toString());
     if (LOG.isDebugEnabled()) {
       LOG.debug(String.format("cwdApp: %s", cwdApp));
     }
     
     List<String> command ;

     command = new ArrayList<String>();

   //use same jvm as parent
     File jvm = new File(
         new File(System.getProperty("java.home"), "bin"), "java.exe");
     command.add(jvm.toString());
     
     Path cwdPath = new Path(cwdApp.getPath());
     
     // Build a temp classpath jar. See ContainerLaunch.sanitizeEnv().
     // Passing CLASSPATH explicitly is *way* too long for command line.
     String classPath = System.getProperty("java.class.path");
     Map<String, String> env = new HashMap<String, String>(System.getenv());
     String jarCp[] = FileUtil.createJarWithClassPath(classPath, 
         classpathJarPrivateDir, cwdPath, env);
     String classPathJar = localizeClasspathJar(
         new Path(jarCp[0]), cwdPath, user).toString();
     command.add("-classpath");
     command.add(classPathJar + jarCp[1]);

     String javaLibPath = System.getProperty("java.library.path");
     if (javaLibPath != null) {
       command.add("-Djava.library.path=" + javaLibPath);
     }
     
     ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr, 
         localDirs);
     
     String cmdLine = StringUtils.join(command, " ");
     
     String localizerPid = String.format(LOCALIZER_PID_FORMAT, locId);
     
     WintuilsProcessStubExecutor stubExecutor = new WintuilsProcessStubExecutor(
         cwdApp.getAbsolutePath(), 
         localizerPid, user, "nul:", cmdLine);
     try {
       stubExecutor.execute();
       stubExecutor.validateResult();
     }
     finally {
       stubExecutor.close();
       try
       {
         killContainer(localizerPid, Signal.KILL);
       }
       catch(Throwable e) {
         LOG.warn(String.format(
             "An exception occured during the cleanup of localizer job %s:%n%s", 
             localizerPid, 
             org.apache.hadoop.util.StringUtils.stringifyException(e)));
       }
     }
   }
 
   @Override
  protected CommandExecutor buildCommandExecutor(String wrapperScriptPath,
      String containerIdStr, String userName, Path pidFile, Resource resource,
      File wordDir, Map<String, String> environment) throws IOException {
     return new WintuilsProcessStubExecutor(
         wordDir.toString(),
         containerIdStr, userName, pidFile.toString(), 
         "cmd /c " + wrapperScriptPath);
   }
   
   @Override
   protected void killContainer(String pid, Signal signal) throws IOException {
     Native.Elevated.killTask(pid);
   }
}

