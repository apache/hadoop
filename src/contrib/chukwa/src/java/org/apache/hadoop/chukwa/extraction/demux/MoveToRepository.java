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

package org.apache.hadoop.chukwa.extraction.demux;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

// TODO
// First version of the Spill
// need some polishing

public class MoveToRepository
{
	static Logger log = Logger.getLogger(MoveToRepository.class);
	
	static ChukwaConfiguration conf = null;
	static FileSystem fs = null;
	static final String HadoopLogDir = "_logs";
	static final String hadoopTempDir = "_temporary";
	static SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd");
	static Calendar calendar = Calendar.getInstance();
	
	static void processClutserDirectory(Path srcDir,String destDir) throws Exception
	{
		log.info("processClutserDirectory (" + srcDir.getName() + "," + destDir +")");
		FileStatus fstat = fs.getFileStatus(srcDir);
		
		if (!fstat.isDir())
		{
			throw new IOException(srcDir + " is not a directory!");
		}
		else
		{
			FileStatus[] datasourceDirectories = fs.listStatus(srcDir);
			
			for(FileStatus datasourceDirectory : datasourceDirectories)
			{
				log.info(datasourceDirectory.getPath() + " isDir?" +datasourceDirectory.isDir());
				if (!datasourceDirectory.isDir())
				{
					throw new IOException("Top level datasource directory should be a directory :" + datasourceDirectory.getPath());
				}
				
				String dirName = datasourceDirectory.getPath().getName();
				Path destPath = new Path(destDir + "/" + dirName);
				log.info("dest directory path: " + destPath);
				log.info("processClutserDirectory processing Datasource: (" + dirName +")");
				processDatasourceDirectory(srcDir.getName(),datasourceDirectory.getPath(),destDir + "/" + dirName);
			}
		}
	}
	
	static void processDatasourceDirectory(String cluster,Path srcDir,String destDir) throws Exception
	{
		String fileName = null;
		int fileDay  = 0;
		int fileHour = 0;
		int fileMin  = 0;
		
		FileStatus[] recordFiles = fs.listStatus(srcDir);
		for(FileStatus recordFile : recordFiles)
		{
			//   dataSource_20080915_18_15.1.evt
			// <datasource>_<yyyyMMdd_HH_mm>.1.evt
			
			fileName = recordFile.getPath().getName();
			log.info("processDatasourceDirectory processing RecordFile: (" + fileName +")");
			log.info("fileName: " + fileName);
			
			
			int l = fileName.length();
			String dataSource = srcDir.getName();
			log.info("Datasource: " + dataSource);
			
			if (fileName.endsWith(".D.evt"))
			{
				// Hadoop_dfs_datanode_20080919.D.evt
				
				fileDay = Integer.parseInt(fileName.substring(l-14,l-6));
				writeRecordFile(destDir + "/" + fileDay + "/", recordFile.getPath(),dataSource + "_" +fileDay);
				// mark this directory for Daily rotate (re-process)
				addDirectory4Rolling( true,fileDay , fileHour, cluster , dataSource);
			}
			else if (fileName.endsWith(".H.evt"))
			{ 
				// Hadoop_dfs_datanode_20080925_1.H.evt
				// Hadoop_dfs_datanode_20080925_12.H.evt
				
				String day = null;
			    String hour = null;
			    if (fileName.charAt(l-8) == '_')
			    {
			    	day = fileName.substring(l-16,l-8);
			    	log.info("day->" + day);
			    	hour = "" +fileName.charAt(l-7);
			    	log.info("hour->" +hour);
			    }
			    else
			    {
			    	day = fileName.substring(l-17,l-9);
			    	log.info("day->" +day);
			    	hour = fileName.substring(l-8,l-6);
			    	log.info("hour->" +hour);
			    }
			    fileDay = Integer.parseInt(day);
			    fileHour = Integer.parseInt(hour);
			    // rotate there so spill
				writeRecordFile(destDir + "/" + fileDay + "/" + fileHour + "/", recordFile.getPath(),dataSource + "_"  +fileDay+ "_" + fileHour );
				// mark this directory for daily rotate
				addDirectory4Rolling( true,fileDay , fileHour, cluster , dataSource);
			}
			else if (fileName.endsWith(".R.evt"))
			{
				if (fileName.charAt(l-11) == '_')
				{
					fileDay = Integer.parseInt(fileName.substring(l-19,l-11));
					fileHour = Integer.parseInt(""+fileName.charAt(l-10));
					fileMin = Integer.parseInt(fileName.substring(l-8,l-6));
				}
				else
				{
					fileDay = Integer.parseInt(fileName.substring(l-20,l-12));
					fileHour = Integer.parseInt(fileName.substring(l-11,l-9));
					fileMin = Integer.parseInt(fileName.substring(l-8,l-6));
				}

				log.info("fileDay: " + fileDay);
				log.info("fileHour: " + fileHour);
				log.info("fileMin: " + fileMin);
				writeRecordFile(destDir + "/" + fileDay + "/" + fileHour + "/" + fileMin, recordFile.getPath(),dataSource + "_" +fileDay+ "_" + fileHour +"_" +fileMin);
				// mark this directory for hourly rotate
				addDirectory4Rolling( false,fileDay , fileHour, cluster , dataSource);
			}
			else
			{
				throw new RuntimeException("Wrong fileName format! [" + fileName+"]");
			}
		}
	}
			
	static void addDirectory4Rolling(boolean isDailyOnly, int day,int hour,String cluster, String dataSource) throws IOException
	{
		// TODO get root directory from config
		String rollingDirectory = "/chukwa/rolling/";
		
		Path path = new Path(rollingDirectory + "/daily/" + day + "/" + cluster +"/" + dataSource);
		if (!fs.exists(path))
			{ fs.mkdirs(path);}
		
		if (!isDailyOnly)
		{
			path = new Path(rollingDirectory + "/hourly/" + day + "/"  + hour + "/" + cluster +"/" + dataSource);
			if (!fs.exists(path))
				{ fs.mkdirs(path);}
		}
	}
	
	static void writeRecordFile(String destDir,Path recordFile,String fileName) throws IOException
	{
		boolean done = false;
		int count = 1;
		do
		{
			Path destDirPath = new Path(destDir );
			Path destFilePath = new Path(destDir + "/" + fileName + "." + count + ".evt" );
			
			if (!fs.exists(destDirPath))
			{
				fs.mkdirs(destDirPath);
				log.info(">>>>>>>>>>>> create Dir" + destDirPath);
			}
			
			if (!fs.exists(destFilePath))
			{
				log.info(">>>>>>>>>>>> Before Rename" + recordFile + " -- "+ destFilePath);
				//fs.rename(recordFile,destFilePath);
				FileUtil.copy(fs,recordFile,fs,destFilePath,false,false,conf);
				//FileUtil.replaceFile(new File(recordFile.toUri()), new File(destFilePath.toUri()));
				done = true;
				log.info(">>>>>>>>>>>> after Rename" + destFilePath);
			}
			else
			{
				log.info("Start MoveToRepository main()");
			}
			count ++;
			// Just put a limit here
			// TODO read from config
			if (count > 1000)
			{
				throw new IOException("too many files in this directory: " + destDir);
			}
		} while (!done);
	}
	
	static boolean checkRotate(String directoryAsString, boolean createDirectoryIfNotExist) throws IOException
	{
		Path directory = new Path(directoryAsString);
		boolean exist = fs.exists(directory);
	
		if (! exist )
		{
			if (createDirectoryIfNotExist== true)
				{ fs.mkdirs(directory); }
			return false;
		}
		else
		{
			return fs.exists(new Path(directoryAsString + "/rotateDone"));
		}
	}
				
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception
	{
		conf = new ChukwaConfiguration();
		String fsName = conf.get("writer.hdfs.filesystem");
		fs = FileSystem.get(new URI(fsName), conf);
		
		Path srcDir = new Path(args[0]);
		String destDir = args[1];
		
		log.info("Start MoveToRepository main()");
		
		FileStatus fstat = fs.getFileStatus(srcDir);
		
		if (!fstat.isDir())
		{
			throw new IOException(srcDir + " is not a directory!");
		}
		else
		{
			FileStatus[] clusters = fs.listStatus(srcDir);
			// Run a moveOrMerge on all clusters
			String name = null;
			for(FileStatus cluster : clusters)
			{
				name = cluster.getPath().getName();
				// Skip hadoop M/R outputDir
				if ( (name.intern() == HadoopLogDir.intern() ) || (name.intern() == hadoopTempDir.intern()) )
				{
					continue;
				}
				log.info("main procesing Cluster (" + cluster.getPath().getName() +")");
				processClutserDirectory(cluster.getPath(),destDir + "/" + cluster.getPath().getName());
				
				// Delete the demux's cluster dir 
				FileUtil.fullyDelete(fs,cluster.getPath());
			}
		}
		
		log.info("Done with MoveToRepository main()");

	}

}
