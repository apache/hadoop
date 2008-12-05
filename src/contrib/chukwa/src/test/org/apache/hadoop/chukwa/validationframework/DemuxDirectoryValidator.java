/*
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

package org.apache.hadoop.chukwa.validationframework;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.validationframework.util.DataOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DemuxDirectoryValidator
{

  static Configuration conf = null;
  static FileSystem fs = null;
  
  public static void usage()
  {
    System.out.println("Usage ...");
    System.exit(-1);
  }
  
  public static void validate(boolean isLocal,FileSystem fs,Configuration conf,String[] directories)
  {
    DemuxDirectoryValidator.fs = fs;
    DemuxDirectoryValidator.conf = conf;
    try
    {
      if (isLocal)
      {
        compareLocalDirectory(directories[0],directories[1]);
      }
      else
      {
        DemuxDirectoryValidator.fs = fs;
        compareHDFSDirectory(directories[0],directories[1]);
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException ("Validation failed! [" 
          + directories[0] +"][" + directories[1] + "]" ,e);
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args)
  {

    if (args.length != 3)
    {
      usage();
    }
    
    String demuxGoldDirectory = args[1];
    String demuxTestDirectory = args[2];
    boolean isLocal = true;
    
    if ("-local".equalsIgnoreCase(args[0]))
    {
      compareLocalDirectory(demuxGoldDirectory,demuxTestDirectory);
    }
    else if ("-hdfs".equalsIgnoreCase(args[0]))
    {
      isLocal = false;
      conf = new ChukwaConfiguration();
      String fsName = conf.get("writer.hdfs.filesystem");
      try
      {
        fs = FileSystem.get(new URI(fsName), conf);
      } catch (Exception e)
      {
        e.printStackTrace();
        throw new RuntimeException(e);
      } 
    }
    else
    {
      System.out.println("Wrong first argument");
      usage();
    }
    
    String[] dirs = { demuxGoldDirectory,demuxTestDirectory};
    validate(isLocal,fs,conf,dirs);
    
    System.out.println("Gold and test directories are equivalent");
    System.exit(10);
  }
  
  public static void compareHDFSDirectory(String gold,String test)
  {
    try
    {
      Path goldDirectory = new Path(gold);
      FileStatus[] goldFiles = fs.listStatus(goldDirectory);

      //      Path testDirectory = new Path(test);
//      FileStatus[] testFiles = fs.listStatus(testDirectory);
//      
      
      for(int i=0;i<goldFiles.length;i++)
      {
        
        //Skip the crc files
        if (goldFiles[i].getPath().getName().endsWith(".crc")  )
        { continue; }
        
        System.out.println("Testing [" + goldFiles[i].getPath().getName().intern() + "]" );
        
//        if (goldFiles[i].getPath().getName().intern() != testFiles[i].getPath().getName().intern())
//        {
//          throw new RuntimeException("Gold & test dirrectories [" + gold +"/" +goldFiles[i].getPath().getName() +"] are not the same");
//        }
    
        if (goldFiles[i].isDir())
        {
          //Skip the _logs directory
          if (goldFiles[i].getPath().getName().equalsIgnoreCase("_logs"))
          { continue; }
          
          compareHDFSDirectory(gold +"/" +goldFiles[i].getPath().getName(),test +"/" +goldFiles[i].getPath().getName());
        }
        else
        {
          boolean isTheSme = DataOperations.validateChukwaRecords(fs,conf,goldFiles[i].getPath(), new Path(test + "/" +goldFiles[i].getPath().getName()));
          if (!isTheSme)
          {
//            System.out.println("MD5 failed on [" + gold +"/" +goldFiles[i] +"]");
            throw new RuntimeException("ChukwaRecords validation error: for Gold & test [" + gold +"/" +goldFiles[i].getPath().getName()+"] [" 
                + test +"/" +goldFiles[i].getPath().getName()+ "] are not the same");
          }
        }
      }
    } catch (Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  public static void compareLocalDirectory(String gold,String test)
  {
    File goldDirectory = new File(gold);
    String[] goldFiles = goldDirectory.list();
    File testDirectory = new File(test);
    String[] testFiles = testDirectory.list();
    
    for(int i=0;i<goldFiles.length;i++)
    {
      if (goldFiles[i].intern() != testFiles[i].intern())
      {
        throw new RuntimeException("Gold & test dirrectories [" + gold +"/" +goldFiles[i] +"] are not the same");
      }
      File g = new File(gold +"/" +goldFiles[i]);
      if (g.isDirectory())
      {
        //Skip the _logs directory
        if (goldFiles[i].equalsIgnoreCase("_logs"))
        { continue; }
       
        compareLocalDirectory(gold +"/" +goldFiles[i],test +"/" +goldFiles[i]);
      }
      else
      {
        boolean md5 = DataOperations.validateMD5(gold +"/" +goldFiles[i], test +"/" +goldFiles[i]);
        if (!md5)
        {
//          System.out.println("MD5 failed on [" + gold +"/" +goldFiles[i] +"]");
          throw new RuntimeException("MD5 for Gold & test [" + gold +"/" +goldFiles[i]+"] are not the same");
        }
      }
    }
    
    
  }
}
