package org.apache.hadoop.chukwa.validationframework;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent.AlreadyRunningException;
import org.apache.hadoop.chukwa.datacollection.collector.CollectorStub;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.validationframework.interceptor.ChunkDumper;
import org.apache.hadoop.chukwa.validationframework.interceptor.SetupTestClasses;
import org.apache.hadoop.chukwa.validationframework.util.DataOperations;

public class ChukwaAgentToCollectorValidator
{
	public static final int ADD = 100;
	public static final int VALIDATE = 200;
	
	
	private static void usage()
	{
		System.out.println("usage ...");
		System.exit(-1);
	}
	/**
	 * @param args
	 * @throws Throwable 
	 * @throws AlreadyRunningException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Throwable
	{
		if (args.length != 2)
		{
			usage();
		}
		
		
		int command = -1;
		
		if ("-add".equalsIgnoreCase(args[0]))
		{
			command = ChukwaAgentToCollectorValidator.ADD;
		}
		else if ("-validate".equalsIgnoreCase(args[0]))
		{
			command = ChukwaAgentToCollectorValidator.VALIDATE;
		}
		else
		{
			usage();
		}
		
		String chukwaTestRepository = System.getenv("chukwaTestRepository");
		if (chukwaTestRepository == null)
		{
		  chukwaTestRepository = "/tmp/chukwaTestRepository/";
		}
		
		if (!chukwaTestRepository.endsWith("/"))
		{
		  chukwaTestRepository += "/";
		}
		
		String fileName = args[1];
		
		String name = null;
		if (fileName.indexOf("/") >= 0)
		{
		  name = fileName.substring(fileName.lastIndexOf("/"));
		}
		else
		{
		  name = fileName;
		}
		  
		String chukwaTestDirectory =  chukwaTestRepository + name ;
		String inputFile = chukwaTestDirectory + "/input/" + name;
		String outputDir = null;
		
		if (command == ChukwaAgentToCollectorValidator.ADD)
		{
			File dir = new File(chukwaTestDirectory + "/input/");
			if (dir.exists())
			{
				throw new RuntimeException("a test with the same input file is already there, remove it first");
			}
			dir.mkdirs();
			DataOperations.copyFile(fileName, inputFile);
			outputDir = "/gold";
		}
		else
		{
		  outputDir = "/" + System.currentTimeMillis();
		}
		
		System.out.println("chukwaTestDirectory [" + chukwaTestDirectory + "]");
		System.out.println("command [" + ( (command == ChukwaAgentToCollectorValidator.ADD)?"ADD":"VALIDATE") + "]");
		System.out.println("fileName [" + inputFile + "]");
		

		 ChukwaConfiguration conf = new ChukwaConfiguration(true);
	   String collectorOutputDir = conf.get("chukwaCollector.outputDir");
		
	   
		prepareAndSendData(chukwaTestDirectory+ outputDir,inputFile,collectorOutputDir);
		extractRawLog(chukwaTestDirectory+ outputDir,name,collectorOutputDir);
		boolean rawLogTestResult = validateRawLogs(chukwaTestDirectory+outputDir,name) ;
		
		
		boolean binLogTestResult = true;
		
	  if (command == ChukwaAgentToCollectorValidator.VALIDATE)
	  {
	    binLogTestResult = validateOutputs(chukwaTestDirectory+outputDir,name);
	  }
		
		
		if (rawLogTestResult == true && binLogTestResult == true)
		{
		  System.out.println("test OK");
		  System.exit(10);
		}
		else
		{
		  System.out.println("test KO");
		  throw new RuntimeException("test failed for file [" + name +"]" );
		}
	}

	public static void prepareAndSendData(String dataRootFolder,String inputFile,String dataSinkDirectory) throws Throwable
	{

	  ChunkDumper.testRepositoryDumpDir = dataRootFolder + "/";

	  SetupTestClasses.setupClasses();

	  // clean up the collector outputDir.
	  File collectorDir = new File(dataSinkDirectory);
	  String[] files = collectorDir.list();
	  for(String f: files)
	  {
	    File file = new File(dataSinkDirectory+ File.separator +f);
	    file.delete();
	    System.out.println("Deleting previous collectors files: " + f);
	  }
	  
	  System.out.println("Starting agent");
    String[] agentArgs = new String[0];
    ChukwaAgent.main(agentArgs);
    
    // Start the collector
    System.out.println("Starting collector");
    CollectorStub.main(new String[0]);
    
    // Start the agent
    ChukwaAgent agent = ChukwaAgent.getAgent();
    
     
    int portno = 9093; // Default
    ChukwaAgentController cli = new ChukwaAgentController("localhost",portno);
    // ADD 
    // org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped 
    // SysLog 
    // 0 /var/log/messages 
    // 0
    System.out.println("Adding adaptor");
    long adaptor = cli.add("org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped",
        "AutomatedTestType", "0 " + inputFile, 0);
    
    cli.remove(adaptor);
    System.out.println("Adaptor removed");
    agent.shutdown();
    System.out.println("Shutting down agent");
    CollectorStub.jettyServer.stop();
    System.out.println("Shutting down collector");
    Thread.sleep(2000);
	}
	
	public static void extractRawLog(String dataRootFolder,String fileName,String dataSinkDirectory) throws Exception
	{
	  // Adaptor output
	  DataOperations.extractRawLogFromDump(dataRootFolder + "/adaptor/", fileName);
	  // Sender output
	  DataOperations.extractRawLogFromDump(dataRootFolder + "/sender/", fileName);
	  
	  // Collector output
	  File dir = new File(dataRootFolder  + "/collector/");
	  dir.mkdirs();
	  
	  File dataSinkDir = new File(dataSinkDirectory);
	  String[] doneFiles = dataSinkDir.list();
    // Move done file to the final directory
    for(String f: doneFiles)
    {
      String outputFile = null;
      if (f.endsWith(".done"))
      {
        outputFile = fileName +".done";
      }
      else
      {
        outputFile = fileName + ".crc";
      }
      System.out.println("Moving that file ["  + dataSinkDirectory+ File.separator +f + "] to ["  +dataRootFolder  + "/collector/" + outputFile +"]");
     DataOperations.copyFile(dataSinkDirectory+ File.separator +f, dataRootFolder  + "/collector/" + outputFile);
     }
  
    DataOperations.extractRawLogFromdataSink(ChunkDumper.testRepositoryDumpDir  + "/collector/",fileName);
	}
	
	
	public static boolean validateRawLogs(String dataRootFolder,String fileName) 
	{
	  boolean result = true;
	  // Validate Adaptor
	  boolean adaptorMD5 = DataOperations.validateMD5(dataRootFolder + "/../input/" + fileName, dataRootFolder + "/adaptor/" + fileName + ".raw");
	  if (!adaptorMD5)
	  {
	    System.out.println("Adaptor validation failed");
	    result = false;
	  }
	  // Validate Sender
	  boolean senderMD5 = DataOperations.validateMD5(dataRootFolder + "/../input/" + fileName, dataRootFolder + "/sender/" + fileName + ".raw");
	  if (!senderMD5)
	  {
	    System.out.println("Sender validation failed");
	    result = false;
	  }
	  // Validate DataSink
	  boolean collectorMD5 = DataOperations.validateMD5(dataRootFolder + "/../input/" + fileName , dataRootFolder + "/collector/" + fileName + ".raw");
	  if (!collectorMD5)
	  {
	    System.out.println("collector validation failed");
	    result = false;
	  }
	
	  return result;
	}

	public static boolean validateOutputs(String dataRootFolder,String fileName)
	{
	   boolean result = true;
	    // Validate Adaptor
	    boolean adaptorMD5 = DataOperations.validateMD5(dataRootFolder + "/../gold/adaptor/" + fileName + ".bin", dataRootFolder + "/adaptor/" + fileName + ".bin");
	    if (!adaptorMD5)
	    {
	      System.out.println("Adaptor bin validation failed");
	      result = false;
	    }
	    // Validate Sender
	    boolean senderMD5 = DataOperations.validateMD5(dataRootFolder + "/../gold/sender/" + fileName+ ".bin", dataRootFolder + "/sender/" + fileName + ".bin");
	    if (!senderMD5)
	    {
	      System.out.println("Sender bin validation failed");
	      result = false;
	    }
	    // Validate DataSink
//	    boolean collectorMD5 = DataOperations.validateRawLog(dataRootFolder + "/../gold/collector/" + fileName + ".done", dataRootFolder + "/collector/" + fileName + ".done");
//	    if (!collectorMD5)
//	    {
//	      System.out.println("collector bin validation failed");
//	      result = false;
//	    }
	  
	    return result;
	}
}