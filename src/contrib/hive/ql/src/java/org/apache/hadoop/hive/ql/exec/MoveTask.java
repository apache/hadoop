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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * MoveTask implementation
 **/
public class MoveTask extends Task<moveWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  private void cleanseSource(FileSystem fs, Path sourcePath) throws IOException {
    if(sourcePath == null)
      return;

    FileStatus [] srcs = fs.globStatus(sourcePath);
    if(srcs != null) {
      for(FileStatus one: srcs) {
        if(Hive.needsDeletion(one)) {
          fs.delete(one.getPath(), true);
        }
      }
    }
  }

  public int execute() {

    try {
      // Create a file system handle
      FileSystem fs = FileSystem.get(conf);

      // Do any hive related operations like moving tables and files
      // to appropriate locations
      for(loadFileDesc lfd: work.getLoadFileWork()) {
        Path targetPath = new Path(lfd.getTargetDir());
        Path sourcePath = new Path(lfd.getSourceDir());
        cleanseSource(fs, sourcePath);
        if (lfd.getIsDfsDir()) {
          // Just do a rename on the URIs
          String mesg = "Moving data to: " + lfd.getTargetDir();
          String mesg_detail = " from " +  lfd.getSourceDir();
          console.printInfo(mesg, mesg_detail);

          // delete the output directory if it already exists
          fs.delete(targetPath, true);
          // if source exists, rename. Otherwise, create a empty directory
          if (fs.exists(sourcePath))
            fs.rename(sourcePath, targetPath);
          else
            fs.mkdirs(targetPath);
        } else {
          // This is a local file
          String mesg = "Copying data to local directory " + lfd.getTargetDir();
          String mesg_detail =  " from " + lfd.getSourceDir();
          console.printInfo(mesg, mesg_detail);

          // delete the existing dest directory
          LocalFileSystem dstFs = FileSystem.getLocal(fs.getConf());

          if(dstFs.delete(targetPath, true) || !dstFs.exists(targetPath)) {
            console.printInfo(mesg, mesg_detail);
          // if source exists, rename. Otherwise, create a empty directory
          if (fs.exists(sourcePath))
            fs.copyToLocalFile(sourcePath, targetPath);
          else
            dstFs.mkdirs(targetPath);
          } else {
            console.printInfo("Unable to delete the existing destination directory: " + targetPath);
          }
        }
      }

      // Next we do this for tables and partitions
      for(loadTableDesc tbd: work.getLoadTableWork()) {
        String mesg = "Loading data to table " + tbd.getTable().getTableName() +
        ((tbd.getPartitionSpec().size() > 0) ? 
            " partition " + tbd.getPartitionSpec().toString() : "");
        String mesg_detail = " from " + tbd.getSourceDir();
        console.printInfo(mesg, mesg_detail);

        // Get the file format of the table
        boolean tableIsSequenceFile = tbd.getTable().getInputFileFormatClass().equals(SequenceFileInputFormat.class);
        // Get all files from the src directory
        FileStatus [] dirs;
        ArrayList<FileStatus> files;
        try {
          fs = FileSystem.get(db.getTable(tbd.getTable().getTableName()).getDataLocation(),
              Hive.get().getConf());
          dirs = fs.globStatus(new Path(tbd.getSourceDir()));
          files = new ArrayList<FileStatus>();
          for (int i=0; i<dirs.length; i++) {
            files.addAll(Arrays.asList(fs.listStatus(dirs[i].getPath())));
            // We only check one file, so exit the loop when we have at least one.
            if (files.size()>0) break;
          }
        } catch (IOException e) {
          throw new HiveException("addFiles: filesystem error in check phase", e);
        }
        // Check if the file format of the file matches that of the table.
        if (files.size() > 0) {
          int fileId = 0;
          boolean fileIsSequenceFile = true;   
          try {
            SequenceFile.Reader reader = new SequenceFile.Reader(
              fs, files.get(fileId).getPath(), conf);
            reader.close();
          } catch (IOException e) {
            fileIsSequenceFile = false;
          }
          if (!fileIsSequenceFile && tableIsSequenceFile) {
            throw new HiveException("Cannot load text files into a table stored as SequenceFile.");
          }
          if (fileIsSequenceFile && !tableIsSequenceFile) {
            throw new HiveException("Cannot load SequenceFiles into a table stored as TextFile.");
          }
        }
         

        if(tbd.getPartitionSpec().size() == 0) {
          db.loadTable(new Path(tbd.getSourceDir()), tbd.getTable().getTableName(), tbd.getReplace());
        } else {
          LOG.info("Partition is: " + tbd.getPartitionSpec().toString());
          db.loadPartition(new Path(tbd.getSourceDir()), tbd.getTable().getTableName(),
              tbd.getPartitionSpec(), tbd.getReplace());
        }
      }

      return 0;
    }
    catch (Exception e) {
      console.printError("Failed with exception " +   e.getMessage(), "\n" + StringUtils.stringifyException(e));
      return (1);
    }
  }
}
