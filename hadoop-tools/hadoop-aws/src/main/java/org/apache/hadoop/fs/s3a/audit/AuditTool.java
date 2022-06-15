/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.audit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.*;

/**
 * AuditTool is a Command Line Interface to manage S3 Auditing
 * i.e, it is a functionality which directly takes s3 path of audit log files
 * and merge all those into single audit log file
 */
public class AuditTool extends Configured implements Tool, Closeable {

    private static final Logger LOG = Logger.getLogger(AuditTool.class);

    private final String ENTRY_POINT = "s3audit";

    private PrintWriter out;

    // Exit codes
    private static final int SUCCESS = EXIT_SUCCESS;
    private static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;

    /** Error String when the wrong FS is used for binding: {@value}. **/
    @VisibleForTesting
    public static final String WRONG_FILESYSTEM = "Wrong filesystem for ";

    private final String USAGE = ENTRY_POINT + "  s3a://BUCKET\n";

    private final File s3aLogsDirectory = new File("S3AAuditLogsDirectory");

    public AuditTool() {
    }

    /**
     * tells us the usage of the AuditTool by commands
     * @return the string USAGE
     */
    public String getUsage() {
        return USAGE;
    }

    /**
     * this run method in AuditTool takes S3 bucket path
     * which contains audit log files from command line arguments
     * and merge the audit log files present in that path into single file in local system
     * @param args command specific arguments.
     * @return SUCCESS i.e, '0', which is an exit code
     * @throws Exception on any failure
     */
    @Override
    public int run(String[] args) throws Exception {
        List<String> argv = new ArrayList<>(Arrays.asList(args));
        println("argv: " + argv);
        if (argv.isEmpty()) {
            errorln(getUsage());
            throw invalidArgs("No bucket specified");
        }
        //path of audit log files in s3 bucket
        Path s3LogsPath = new Path(argv.get(0));

        //setting the file system
        URI fsURI = toUri(String.valueOf(s3LogsPath));
        S3AFileSystem s3AFileSystem = bindFilesystem(FileSystem.newInstance(fsURI, getConf()));
        RemoteIterator<LocatedFileStatus> listOfS3LogFiles = s3AFileSystem.listFiles(s3LogsPath, true);

        //creating local audit log files directory and copying audit log files into local files from s3 bucket
        //so that it will be easy for us to implement merging and parsing classes
        if (!s3aLogsDirectory.exists()) {
            boolean s3aLogsDirectoryCreation = s3aLogsDirectory.mkdir();
        }
        File s3aLogsSubDir = new File(s3aLogsDirectory, s3LogsPath.getName());
        boolean s3aLogsSubDirCreation = false;
        if (!s3aLogsSubDir.exists()) {
            s3aLogsSubDirCreation = s3aLogsSubDir.mkdir();
        }
        if (s3aLogsSubDirCreation) {
            while (listOfS3LogFiles.hasNext()) {
                Path s3LogFilePath = listOfS3LogFiles.next().getPath();
                File s3LogLocalFilePath = new File(s3aLogsSubDir, s3LogFilePath.getName());
                boolean localFileCreation = s3LogLocalFilePath.createNewFile();
                if (localFileCreation) {
                    FileStatus fileStatus = s3AFileSystem.getFileStatus(s3LogFilePath);
                    long s3LogFileLength = fileStatus.getLen();
                    //reads s3 file data into byte buffer
                    byte[] s3LogDataBuffer = readDataset(s3AFileSystem, s3LogFilePath, (int) s3LogFileLength);
                    //writes byte array into local file
                    FileUtils.writeByteArrayToFile(s3LogLocalFilePath, s3LogDataBuffer);
                }
            }
        }

        //calls S3AAuditLogMerger for implementing merging code
        //by passing local audit log files directory which are copied from s3 bucket
        S3AAuditLogMerger s3AAuditLogMerger = new S3AAuditLogMerger();
        s3AAuditLogMerger.mergeFiles(s3aLogsSubDir.getPath());

        //deleting the local log files
        if(s3aLogsDirectory.exists()) {
            FileUtils.forceDeleteOnExit(s3aLogsDirectory);
        }
        return SUCCESS;
    }

    /**
     * Read the file and convert to a byte dataset.
     * This implements readfully internally, so that it will read
     * in the file without ever having to seek()
     * @param s3AFileSystem filesystem
     * @param s3LogFilePath path to read from
     * @param s3LogFileLength length of data to read
     * @return the bytes
     * @throws IOException IO problems
     */
    private byte[] readDataset(FileSystem s3AFileSystem, Path s3LogFilePath, int s3LogFileLength) throws IOException {
        byte[] s3LogDataBuffer = new byte[s3LogFileLength];
        int offset = 0;
        int nread = 0;
        try(FSDataInputStream fsDataInputStream = s3AFileSystem.open(s3LogFilePath)) {
            while (nread < s3LogFileLength) {
                int nbytes = fsDataInputStream.read(s3LogDataBuffer, offset + nread, s3LogFileLength - nread);
                if (nbytes < 0) {
                    throw new EOFException("End of file reached before reading fully.");
                }
                nread += nbytes;
            }
        }
        return s3LogDataBuffer;
    }

    /**
     * Build an exception to throw with a formatted message.
     * @param exitCode exit code to use
     * @param format string format
     * @param args optional arguments for the string
     * @return a new exception to throw
     */
    protected static ExitUtil.ExitException exitException(
            final int exitCode,
            final String format,
            final Object... args) {
        return new ExitUtil.ExitException(exitCode,
                String.format(format, args));
    }

    /**
     * Build the exception to raise on invalid arguments.
     * @param format string format
     * @param args optional arguments for the string
     * @return a new exception to throw
     */
    protected static ExitUtil.ExitException invalidArgs(
            String format, Object... args) {
        return exitException(INVALID_ARGUMENT, format, args);
    }

    /**
     * Sets the filesystem; it must be an S3A FS instance, or a FilterFS
     * around an S3A Filesystem.
     * @param bindingFS filesystem to bind to
     * @return the bound FS.
     * @throws ExitUtil.ExitException if the FS is not an S3 FS
     */
    protected S3AFileSystem bindFilesystem(FileSystem bindingFS) {
        FileSystem fs = bindingFS;
        while (fs instanceof FilterFileSystem) {
            fs = ((FilterFileSystem) fs).getRawFileSystem();
        }
        if (!(fs instanceof S3AFileSystem)) {
            throw new ExitUtil.ExitException(EXIT_SERVICE_UNAVAILABLE,
                    WRONG_FILESYSTEM + "URI " + fs.getUri() + " : "
                            + fs.getClass().getName());
        }
        S3AFileSystem filesystem = (S3AFileSystem) fs;
        return filesystem;
    }

    /**
     * Convert a path to a URI, catching any {@code URISyntaxException}
     * and converting to an invalid args exception.
     * @param s3Path path to convert to a URI
     * @return a URI of the path
     * @throws ExitUtil.ExitException INVALID_ARGUMENT if the URI is invalid
     */
    protected static URI toUri(String s3Path) {
        URI uri;
        try {
            uri = new URI(s3Path);
        } catch (URISyntaxException e) {
            throw invalidArgs("Not a valid fileystem path: %s", s3Path);
        }
        return uri;
    }

    protected static void errorln(String x) {
        System.err.println(x);
    }

    /**
     * Flush all active output channels, including {@Code System.err},
     * as to stay in sync with any JRE log messages.
     */
    private void flush() {
        if (out != null) {
            out.flush();
        } else {
            System.out.flush();
        }
        System.err.flush();
    }

    /**
     * Print a line of output. This goes to any output file, or
     * is logged at info. The output is flushed before and after, to
     * try and stay in sync with JRE logging.
     *
     * @param format format string
     * @param args any arguments
     */
    private void println(String format, Object... args) {
        flush();
        String msg = String.format(format, args);
        if (out != null) {
            out.println(msg);
        } else {
            System.out.println(msg);
        }
        flush();
    }

    /**
     * Inner entry point, with no logging or system exits.
     *
     * @param conf configuration
     * @param argv argument list
     * @return an exception
     * @throws Exception Exception.
     */
    public static int exec(Configuration conf, String... argv) throws Exception {
        try(AuditTool auditTool = new AuditTool()) {
            return ToolRunner.run(conf, auditTool, argv);
        }
    }

    /**
     * Main entry point.
     * @param argv args list
     */
    public static void main(String[] argv) {
        try {
            ExitUtil.terminate(exec(new Configuration(), argv));
        } catch (ExitUtil.ExitException e) {
            LOG.error(e.toString());
            System.exit(e.status);
        } catch (Exception e) {
            LOG.error(e.toString(), e);
            ExitUtil.halt(-1, e);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        if (out != null) {
            out.close();
        }
    }
}
