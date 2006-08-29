package org.apache.hadoop.benchmarks.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Runs a job multiple times and takesaverage of all runs. 
 * @author sanjaydahiya
 *
 */
public class MultiJobRunner {
  
  private String jarFile = null ; // "MRBenchmark.jar" ;
  private String input ; 
  private String output ; 
  private int numJobs = 1 ; // default value
  private static final Log LOG = LogFactory.getLog(MultiJobRunner.class);
  private int numMaps = 2; 
  private int numReduces = 1;
  private int dataLines = 1; 
  private boolean ignoreOutput = false ; 
  private boolean verbose = false ; 
  
  // just to print in the end
  ArrayList execTimes = new ArrayList(); 
  
  private static String context = "/mapred/benchmark"; 
  
  /**
   * Input is a local file. 
   * @param input
   * @param output
   * @param jarFile
   */
  public MultiJobRunner(String input, String output, String jarFile){
    this.input = input ; 
    this.output = output ; 
    this.jarFile = jarFile ; 
  }
  
  public String getInput() {
    return input;
  }
  
  public void setInput(String input) {
    this.input = input;
  } 
  
  public String getJarFile() {
    return jarFile;
  }
  
  public void setJarFile(String jarFile) {
    this.jarFile = jarFile;
  }
  
  public String getOutput() {
    return output;
  }
  
  public void setOutput(String output) {
    this.output = output;
  }
  
  public int getNumJobs() {
    return numJobs;
  }
  
  public void setNumJobs(int numJobs) {
    this.numJobs = numJobs;
  }
  
  
  public int getDataLines() {
    return dataLines;
  }
  
  public void setDataLines(int dataLines) {
    this.dataLines = dataLines;
  }
  
  public boolean isIgnoreOutput(){
    return this.ignoreOutput ; 
  }
  
  public void setIgnoreOutput(boolean ignore){
    this.ignoreOutput = ignore ; 
  }
  
  public void setVerbose(boolean verbose){
    this.verbose = verbose ; 
  }
  public boolean getVerbose(){
    return this.verbose; 
  }
  
  /**
   * Prepare the jobConf.
   * @return
   */
  private JobConf setupJob(){
    JobConf job = new JobConf() ;
    
    job.addInputPath(new Path(context + "/input"));
    
    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    job.setInputKeyClass(LongWritable.class);
    job.setOutputValueClass(UTF8.class);
    
    job.setMapOutputKeyClass(UTF8.class);
    job.setMapOutputValueClass(UTF8.class);
    
    job.setOutputPath(new Path(output));
    
    if( null != jarFile ){
      job.setJar(jarFile);
    }
    job.setMapperClass(BenchmarkMapper.class);
    job.setReducerClass(BenchmarkReducer.class);
    
    job.setNumMapTasks(this.numMaps);
    job.setNumReduceTasks(this.numReduces);
    
    return job ; 
  }
  
  /**
   * Runs a MapReduce task, given number of times. The input to each task is the same file. 
   * @param job
   * @param times
   * @throws IOException
   */
  private void runJobInSequence(int times) throws IOException{
    Path intrimData = null ; 
    Random rand = new Random();
    
    for( int i= 0;i<times;i++){
      // create a new job conf every time, reusing same object doesnt seem to work. 
      JobConf job = setupJob();
      
      // give a new random name to output of the mapred tasks
      // TODO: see if something better can be done
      intrimData = new Path(context+"/temp/multiMapRedOutput_" + 
          rand.nextInt() );
      job.setOutputPath(intrimData);
      
      // run the mapred task now 
      LOG.info("Running job, Input : " + job.getInputPaths()[0] + 
          " Output : " + job.getOutputPath());
      long curTime = System.currentTimeMillis();
      JobClient.runJob(job);
      execTimes.add(new Long(System.currentTimeMillis() - curTime));
      
      // pull the output out of DFS for validation
      File localOutputFile = File.createTempFile("MROutput" + 
          new Random().nextInt(), ".txt" );
      String localOutputPath = localOutputFile.getAbsolutePath() ; 
      localOutputFile.delete(); 
      
      if( ! ignoreOutput ){
        copyFromDFS(intrimData, localOutputPath);
      }
      
      // diff(input, localOutputPath);
    }
  }
  
  /**
   * Not using it. 
   */
  private boolean diff(String path1, String path2) throws IOException{
    boolean ret = false ; 
    
    return ret ; 
  }
  
  /**
   * Runs a sequence of map reduce tasks, output of each reduce is input 
   * to next map. input should be a pre configured array of JobConfs. 
   * 
   */
  public Path runJobsInSequence(JobConf[] jobs) throws IOException{
    
    // input location = jobs[0] input loc
    Path finalOutput = null ; 
    
    for( int i=0;i<jobs.length; i++){
      if( 0 != i ) {
        // run the first job in sequence. 
        jobs[i].addInputPath(finalOutput) ; 
      }
      
      JobClient.runJob(jobs[i]);
      finalOutput = jobs[i].getOutputPath(); 
    }
    
    return finalOutput; 
  }
  
  /**
   * 
   * Copy the input file from local disk to DFS. 
   * @param localFile
   * @param remotePath
   * @return
   * @throws IOException
   */
  private Path copyToDFS(String localFile, Path remotePath) throws IOException{
    if( null == remotePath){ 
      // use temp path under /mapred in DFS
      remotePath =  new Path( context+"/input/MRBenchmark_" + 
          new Random().nextInt()) ;
    }
    //new File(localPath).
    Configuration conf = new Configuration();
    FileSystem localFS = FileSystem.getNamed("local", conf);
    FileSystem remoteFS = FileSystem.get(conf);
    
    FileUtil.copy(localFS, new Path(localFile), remoteFS, 
        remotePath, false, conf);
    
    if( ignoreOutput) {
      // delete local copy 
      new File(localFile).delete();
    }
    
    return remotePath; 
  }
  
  private void copyFromDFS(Path remotePath, String localPath)
  throws IOException{
    
    Configuration conf = new Configuration();
    FileSystem localFS = FileSystem.getNamed("local", conf);
    FileSystem remoteFS = FileSystem.get(conf);
    
    FileUtil.copy(remoteFS, remotePath, 
        localFS, new Path(localPath), false, conf);
  }
  
  private void setupContext() throws IOException{
    FileSystem.get(new Configuration()).mkdirs(new Path(context));
  }
  private void clearContext() throws IOException{
    FileSystem.get(new Configuration()).delete(new Path(context));
  }
  /**
   * Run the benchmark. 
   * @throws IOException
   */
  public void run() throws IOException{
    
    setupContext(); 
    Path path = copyToDFS(input, null);
    
    long time = System.currentTimeMillis();
    
    try{
      runJobInSequence(numJobs);
    }finally{
      clearContext(); 
    }
    
    if( verbose ) {
      // Print out a report 
      System.out.println("Total MapReduce tasks executed: " + this.numJobs);
      System.out.println("Total lines of data : " + this.dataLines);
      System.out.println("Maps : " + this.numMaps + 
          " ,  Reduces : " + this.numReduces);
    }
    int i =0 ; 
    long totalTime = 0 ; 
    for( Iterator iter = execTimes.iterator() ; iter.hasNext();){
      totalTime +=  ((Long)iter.next()).longValue() ; 
      if( verbose ) {
        System.out.println("Total time for task : " + ++i + 
            " , =  " +  (Long)iter.next());
      }
    }
    
    long avgTime = totalTime / numJobs ;
    if( verbose ) {
      System.out.println("Avg time : " + avgTime);
    }
    
    System.out.println("DataLines  Maps    Reduces    AvgTime");
    System.out.println(this.dataLines + ", " + this.numMaps + ", " + 
        this.numReduces + ", " + avgTime);
    
  }
  
  public int getNumMaps() {
    return numMaps;
  }
  
  public void setNumMaps(int numMaps) {
    this.numMaps = numMaps;
  }
  
  public int getNumReduces() {
    return numReduces;
  }
  
  public void setNumReduces(int numReduces) {
    this.numReduces = numReduces;
  }
  
  public static void main (String[] args) throws IOException{
    
    String version = "MRBenchmark.0.0.1";
    String usage = 
      "Usage: MultiJobRunner -inputLines noOfLines -jar jarFilePath " + 
      "[-output dfsPath] [-times numJobs] -workDir dfsPath" +  
      "[-inputType (ascending, descending, random)]" + 
      " -maps numMaps -reduces numReduces -ignoreOutput -verbose" ;
    
    System.out.println(version);
    
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    
    String output = "";
    String jarFile = null; //"MRBenchmark.jar" ; 
    int numJobs = 1 ; 
    int numMaps = 2; 
    int numReduces = 1 ; 
    int dataLines = 1 ; 
    int inputType = GenData.ASCENDING ; 
    boolean ignoreOutput = false ; 
    boolean verbose = false ; 
    
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-output")) {
        output = args[++i];
      }else if (args[i].equals("-jar")) {
        jarFile = args[++i];
      }else if (args[i].equals("-times")) {
        numJobs = Integer.parseInt(args[++i]);
      }else if(args[i].equals("-workDir")) {
        context = args[++i];
      }else if(args[i].equals("-maps")) {
        numMaps = Integer.parseInt(args[++i]);
      }else if(args[i].equals("-reduces")) {
        numReduces = Integer.parseInt(args[++i]);
      }else if(args[i].equals("-inputLines")) {
        dataLines = Integer.parseInt(args[++i]);
      }else if(args[i].equals("-inputType")) {
        String s = args[++i] ; 
        if( s.equals("ascending")){
          inputType = GenData.ASCENDING ;
        }else if(s.equals("descending")){
          inputType = GenData.DESCENDING ; 
        }else if(s.equals("random")){
          inputType = GenData.RANDOM ;
        }
      }else if(args[i].equals("-ignoreOutput")) {
        ignoreOutput = true ;
      }else if(args[i].equals("-verbose")) {
        verbose = true ;
      }
    }
    
    File inputFile = File.createTempFile("SortedInput_" + 
        new Random().nextInt(),".txt" );
    GenData.generateText(dataLines, inputFile, inputType);
    
    MultiJobRunner runner = new MultiJobRunner(inputFile.getAbsolutePath(), 
        output, jarFile );
    runner.setInput(inputFile.getAbsolutePath());
    runner.setNumMaps(numMaps);
    runner.setNumReduces(numReduces);
    runner.setDataLines(dataLines);
    runner.setIgnoreOutput(ignoreOutput);
    runner.setVerbose(verbose);
    
    if( 0 != numJobs ){
      runner.setNumJobs(numJobs);
    }
    
    try{
      runner.run(); 
    }catch(IOException e){
      e.printStackTrace();
    }
  }
  
}
