/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.*;

import org.apache.hadoop.conf.Configuration;

/**
 * A collection of file-processing util methods
 */
public class FileUtil {
    /**
     * Delete a directory and all its contents.  If
     * we return false, the directory may be partially-deleted.
     */
    public static boolean fullyDelete(File dir, Configuration conf) throws IOException {
        return fullyDelete(new LocalFileSystem(conf), dir);
    }
    public static boolean fullyDelete(FileSystem fs, File dir) throws IOException {
        // 20041022, xing.
        // Currently fs.detele(File) means fully delete for both
        // LocalFileSystem.java and DistributedFileSystem.java. So we are okay now.
        // If implementation changes in future, it should be modified too.
        return fs.delete(dir);
    }

    /**
     * Copy a file's contents to a new location.
     * Returns whether a target file was overwritten
     */
    public static boolean copyContents(FileSystem fs, File src, File dst, boolean overwrite, Configuration conf) throws IOException {
        if (fs.exists(dst) && !overwrite) {
            return false;
        }

        File dstParent = dst.getParentFile();
        if ((dstParent != null) && (!fs.exists(dstParent))) {
            fs.mkdirs(dstParent);
        }

        if (fs.isFile(src)) {
            FSInputStream in = fs.openRaw(src);
            try {
                FSOutputStream out = fs.createRaw(dst, true);
                byte buf[] = new byte[conf.getInt("io.file.buffer.size", 4096)];
                try {
                    int readBytes = in.read(buf);

                    while (readBytes >= 0) {
                        out.write(buf, 0, readBytes);
                        readBytes = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            }
        } else {
            fs.mkdirs(dst);
            File contents[] = fs.listFilesRaw(src);
            if (contents != null) {
                for (int i = 0; i < contents.length; i++) {
                    File newDst = new File(dst, contents[i].getName());
                    if (! copyContents(fs, contents[i], newDst, overwrite, conf)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Copy a file and/or directory and all its contents (whether
     * data or other files/dirs)
     */
    public static void recursiveCopy(FileSystem fs, File src, File dst, Configuration conf) throws IOException {
        //
        // Resolve the real target.
        //
        if (fs.exists(dst) && fs.isDirectory(dst)) {
            dst = new File(dst, src.getName());
        } else if (fs.exists(dst)) {
            throw new IOException("Destination " + dst + " already exists");
        }

        //
        // Copy the items
        //
        if (! fs.isDirectory(src)) {
            //
            // If the source is a file, then just copy the contents
            //
            copyContents(fs, src, dst, true, conf);
        } else {
            //
            // If the source is a dir, then we need to copy all the subfiles.
            //
            fs.mkdirs(dst);
            File contents[] = fs.listFiles(src);
            for (int i = 0; i < contents.length; i++) {
                recursiveCopy(fs, contents[i], new File(dst, contents[i].getName()), conf);
            }
        }
    }
}
