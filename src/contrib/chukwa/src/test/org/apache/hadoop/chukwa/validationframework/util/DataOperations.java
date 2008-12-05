package org.apache.hadoop.chukwa.validationframework.util;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

public class DataOperations
{
  static Logger log = Logger.getLogger(DataOperations.class);

  public static void copyFile(String fromFileName, String toFileName)
      throws IOException
  {
    File fromFile = new File(fromFileName);
    File toFile = new File(toFileName);

    FileInputStream from = null;
    FileOutputStream to = null;
    try
    {
      from = new FileInputStream(fromFile);
      to = new FileOutputStream(toFile);
      byte[] buffer = new byte[4096];
      int bytesRead;

      while ((bytesRead = from.read(buffer)) != -1)
        to.write(buffer, 0, bytesRead); // write
    } finally
    {
      if (from != null)
        try
        {
          from.close();
        } catch (IOException e)
        {
          ;
        }
      if (to != null)
        try
        {
          to.close();
        } catch (IOException e)
        {
          // ;
        }
    }
  }

  public static boolean validateMD5(String inputFile, String testFile)
  {
    // System.out.println("validateMD5 [" + inputFile + "] [" + testFile
    // + "]");
    String md5_1 = MD5.checksum(new File(inputFile));
    String md5_2 = MD5.checksum(new File(testFile));
    // System.out.println("MD5 [" + md5_1 + "] [" + md5_2 + "]");
    return md5_1.intern() == md5_2.intern();
  }

  public static boolean validateMD5(FileSystem fs, Path inputFile, Path testFile)
  {
    // System.out.println("validateMD5 [" + inputFile + "] [" + testFile
    // + "]");
    String md5_1 = MD5.checksum(fs, inputFile);
    String md5_2 = MD5.checksum(fs, testFile);
    // System.out.println("MD5 [" + md5_1 + "] [" + md5_2 + "]");
    return md5_1.intern() == md5_2.intern();
  }

  public static boolean validateChukwaRecords(FileSystem fs,
      Configuration conf, Path inputFile, Path testFile)
  {
    SequenceFile.Reader goldReader = null;
    SequenceFile.Reader testReader = null;
    try
    {
      // log.info(">>>>>>>>>>>>>> Openning records [" + inputFile.getName()
      // +"][" + testFile.getName() +"]");
      goldReader = new SequenceFile.Reader(fs, inputFile, conf);
      testReader = new SequenceFile.Reader(fs, testFile, conf);

      ChukwaRecordKey goldKey = new ChukwaRecordKey();
      ChukwaRecord goldRecord = new ChukwaRecord();

      ChukwaRecordKey testKey = new ChukwaRecordKey();
      ChukwaRecord testRecord = new ChukwaRecord();

      // log.info(">>>>>>>>>>>>>> Start reading");
      while (goldReader.next(goldKey, goldRecord))
      {
        testReader.next(testKey, testRecord);

        if (goldKey.compareTo(testKey) != 0)
        {
          log.info(">>>>>>>>>>>>>> Not the same Key");
          log.info(">>>>>>>>>>>>>> Record [" + goldKey.getKey() + "] ["
              + goldKey.getReduceType() + "]");
          log.info(">>>>>>>>>>>>>> Record [" + testKey.getKey() + "] ["
              + testKey.getReduceType() + "]");
          return false;
        }

        if (goldRecord.compareTo(testRecord) != 0)
        {
          log.info(">>>>>>>>>>>>>> Not the same Value");
          log.info(">>>>>>>>>>>>>> Record [" + goldKey.getKey() + "] ["
              + goldKey.getReduceType() + "]");
          log.info(">>>>>>>>>>>>>> Record [" + testKey.getKey() + "] ["
              + testKey.getReduceType() + "]");
          log.info(">>>>>>>>>>>>>> Gold Value [" + goldRecord.toString() + "]");
          log.info(">>>>>>>>>>>>>> Test value [" + testRecord.toString() + "]");
          
          return false;
        }
      }
      // log.info(">>>>>>>>>>>>>> Same File");
      return true;
    } catch (IOException e)
    {
      e.printStackTrace();
      return false;
    } finally
    {
      try
      {
        goldReader.close();
        testReader.close();
      } catch (IOException e)
      {
      }

    }
  }

  public static void extractRawLogFromdataSink(String directory, String fileName)
      throws Exception
  {
    ChukwaConfiguration conf = new ChukwaConfiguration();
    String fsName = conf.get("writer.hdfs.filesystem");
    FileSystem fs = FileSystem.get(new URI(fsName), conf);

    SequenceFile.Reader r = new SequenceFile.Reader(fs, new Path(directory
        + fileName + ".done"), conf);

    File outputFile = new File(directory + fileName + ".raw");

    ChukwaArchiveKey key = new ChukwaArchiveKey();
    ChunkImpl chunk = ChunkImpl.getBlankChunk();
    FileWriter out = new FileWriter(outputFile);
    try
    {
      while (r.next(key, chunk))
      {
        out.write(new String(chunk.getData()));
      }
    } finally
    {
      out.close();
      r.close();
    }

  }

  public static void extractRawLogFromDump(String directory, String fileName)
      throws Exception
  {
    File inputFile = new File(directory + fileName + ".bin");
    File outputFile = new File(directory + fileName + ".raw");
    DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
    Chunk chunk = null;
    FileWriter out = new FileWriter(outputFile);
    boolean eof = false;
    do
    {
      try
      {
        chunk = ChunkImpl.read(dis);
        out.write(new String(chunk.getData()));
      } catch (EOFException e)
      {
        eof = true;
      }

    } while (!eof);

    dis.close();
    out.close();
  }
}
