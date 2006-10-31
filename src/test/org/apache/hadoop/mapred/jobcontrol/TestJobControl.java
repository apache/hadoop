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

package org.apache.hadoop.mapred.jobcontrol;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class performs unit test for Job/JobControl classes.
 *  
 * @author runping
 *
 */
public class TestJobControl extends junit.framework.TestCase {

    private static NumberFormat idFormat = NumberFormat.getInstance();
    static {
        idFormat.setMinimumIntegerDigits(4);
        idFormat.setGroupingUsed(false);
    }

    static private Random rand = new Random();

    private static void cleanData(FileSystem fs, Path dirPath)
            throws IOException {
        fs.delete(dirPath);
    }

    private static String generateRandomWord() {
        return idFormat.format(rand.nextLong());
    }

    private static String generateRandomLine() {
        long r = rand.nextLong() % 7;
        long n = r + 20;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < n; i++) {
            sb.append(generateRandomWord()).append(" ");
        }
        sb.append("\n");
        return sb.toString();
    }

    private static void generateData(FileSystem fs, Path dirPath)
            throws IOException {
        FSDataOutputStream out = fs.create(new Path(dirPath, "data.txt"));
        for (int i = 0; i < 100000; i++) {
            String line = TestJobControl.generateRandomLine();
            out.write(line.getBytes("UTF-8"));
        }
        out.close();
    }

    public static class DataCopy extends MapReduceBase implements Mapper,
            Reducer {
        public void map(WritableComparable key, Writable value,
                OutputCollector output, Reporter reporter) throws IOException {
            output.collect(new Text(key.toString()), value);
        }

        public void reduce(WritableComparable key, Iterator values,
                OutputCollector output, Reporter reporter) throws IOException {
            Text dumbKey = new Text("");
            while (values.hasNext()) {
                Text data = (Text) values.next();
                output.collect(dumbKey, data);
            }
        }
    }

    private static JobConf createCopyJob(ArrayList indirs, Path outdir)
            throws Exception {

        Configuration defaults = new Configuration();
        JobConf theJob = new JobConf(defaults, TestJobControl.class);
        theJob.setJobName("DataMoveJob");

        theJob.setInputPath((Path) indirs.get(0));
        if (indirs.size() > 1) {
            for (int i = 1; i < indirs.size(); i++) {
                theJob.addInputPath((Path) indirs.get(i));
            }
        }
        theJob.setMapperClass(DataCopy.class);
        theJob.setOutputPath(outdir);
        theJob.setOutputKeyClass(Text.class);
        theJob.setOutputValueClass(Text.class);
        theJob.setReducerClass(DataCopy.class);
        theJob.setNumMapTasks(12);
        theJob.setNumReduceTasks(4);
        return theJob;
    }

    /**
     * This is a main function for testing JobControl class.
     * It first cleans all the dirs it will use. Then it generates some random text
     * data in TestJobControlData/indir. Then it creates 4 jobs: 
     *      Job 1: copy data from indir to outdir_1
     *      Job 2: copy data from indir to outdir_2
     *      Job 3: copy data from outdir_1 and outdir_2 to outdir_3
     *      Job 4: copy data from outdir to outdir_4
     * The jobs 1 and 2 have no dependency. The job 3 depends on jobs 1 and 2.
     * The job 4 depends on job 3.
     * 
     * Then it creates a JobControl object and add the 4 jobs to the JobControl object.
     * Finally, it creates a thread to run the JobControl object and monitors/reports
     * the job states.
     * 
     * @param args
     */
    public static void doJobControlTest() throws Exception {
        
        Configuration defaults = new Configuration();
        FileSystem fs = FileSystem.get(defaults);
        Path rootDataDir = new Path(System.getProperty("test.build.data", "."), "TestJobControlData");
        Path indir = new Path(rootDataDir, "indir");
        Path outdir_1 = new Path(rootDataDir, "outdir_1");
        Path outdir_2 = new Path(rootDataDir, "outdir_2");
        Path outdir_3 = new Path(rootDataDir, "outdir_3");
        Path outdir_4 = new Path(rootDataDir, "outdir_4");

        cleanData(fs, indir);
        generateData(fs, indir);

        cleanData(fs, outdir_1);
        cleanData(fs, outdir_2);
        cleanData(fs, outdir_3);
        cleanData(fs, outdir_4);

        ArrayList dependingJobs = null;

        ArrayList inPaths_1 = new ArrayList();
        inPaths_1.add(indir);
        JobConf jobConf_1 = createCopyJob(inPaths_1, outdir_1);
        Job job_1 = new Job(jobConf_1, dependingJobs);
        ArrayList inPaths_2 = new ArrayList();
        inPaths_2.add(indir);
        JobConf jobConf_2 = createCopyJob(inPaths_2, outdir_2);
        Job job_2 = new Job(jobConf_2, dependingJobs);

        ArrayList inPaths_3 = new ArrayList();
        inPaths_3.add(outdir_1);
        inPaths_3.add(outdir_2);
        JobConf jobConf_3 = createCopyJob(inPaths_3, outdir_3);
        dependingJobs = new ArrayList();
        dependingJobs.add(job_1);
        dependingJobs.add(job_2);
        Job job_3 = new Job(jobConf_3, dependingJobs);

        ArrayList inPaths_4 = new ArrayList();
        inPaths_4.add(outdir_3);
        JobConf jobConf_4 = createCopyJob(inPaths_4, outdir_4);
        dependingJobs = new ArrayList();
        dependingJobs.add(job_3);
        Job job_4 = new Job(jobConf_4, dependingJobs);

        JobControl theControl = new JobControl("Test");
        theControl.addJob(job_1);
        theControl.addJob(job_2);
        theControl.addJob(job_3);
        theControl.addJob(job_4);

        Thread theController = new Thread(theControl);
        theController.start();
        while (!theControl.allFinished()) {

            System.out.println("Jobs in waiting state: "
                    + theControl.getWaitingJobs().size());
            System.out.println("Jobs in ready state: "
                    + theControl.getReadyJobs().size());
            System.out.println("Jobs in running state: "
                    + theControl.getRunningJobs().size());
            System.out.println("Jobs in success state: "
                    + theControl.getSuccessfulJobs().size());
            System.out.println("Jobs in failed state: "
                    + theControl.getFailedJobs().size());
            System.out.println("\n");

            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }
        }
        System.out.println("Jobs are all done???");
        System.out.println("Jobs in waiting state: "
                + theControl.getWaitingJobs().size());
        System.out.println("Jobs in ready state: "
                + theControl.getReadyJobs().size());
        System.out.println("Jobs in running state: "
                + theControl.getRunningJobs().size());
        System.out.println("Jobs in success state: "
                + theControl.getSuccessfulJobs().size());
        System.out.println("Jobs in failed state: "
                + theControl.getFailedJobs().size());
        System.out.println("\n");
        
        if (job_1.getState() != Job.FAILED && 
                job_1.getState() != Job.DEPENDENT_FAILED && 
                job_1.getState() != Job.SUCCESS) {
           
                String states = "job_1:  " + job_1.getState() + "\n";
                throw new Exception("The state of job_1 is not in a complete state\n" + states);
        }
        
        if (job_2.getState() != Job.FAILED &&
                job_2.getState() != Job.DEPENDENT_FAILED && 
                job_2.getState() != Job.SUCCESS) {
           
                String states = "job_2:  " + job_2.getState() + "\n";
                throw new Exception("The state of job_2 is not in a complete state\n" + states);
        }
        
        if (job_3.getState() != Job.FAILED && 
                job_3.getState() != Job.DEPENDENT_FAILED && 
                job_3.getState() != Job.SUCCESS) {
           
                String states = "job_3:  " + job_3.getState() + "\n";
                throw new Exception("The state of job_3 is not in a complete state\n" + states);
        }
        if (job_4.getState() != Job.FAILED && 
                job_4.getState() != Job.DEPENDENT_FAILED && 
                job_4.getState() != Job.SUCCESS) {
           
                String states = "job_4:  " + job_4.getState() + "\n";
                throw new Exception("The state of job_4 is not in a complete state\n" + states);
        }
        
        if (job_1.getState() == Job.FAILED || 
                job_2.getState() == Job.FAILED ||
                job_1.getState() == Job.DEPENDENT_FAILED || 
                job_2.getState() == Job.DEPENDENT_FAILED) {
            if (job_3.getState() != Job.DEPENDENT_FAILED) {
                String states = "job_1:  " + job_1.getState() + "\n";
                states = "job_2:  " + job_2.getState() + "\n";
                states = "job_3:  " + job_3.getState() + "\n";
                states = "job_4:  " + job_4.getState() + "\n";
                throw new Exception("The states of jobs 1, 2, 3, 4 are not consistent\n" + states);
            }
        }
        if (job_3.getState() == Job.FAILED || 
                job_3.getState() == Job.DEPENDENT_FAILED) {
            if (job_4.getState() != Job.DEPENDENT_FAILED) {
                String states = "job_3:  " + job_3.getState() + "\n";
                states = "job_4:  " + job_4.getState() + "\n";
                throw new Exception("The states of jobs 3, 4 are not consistent\n" + states);
            }
        }
        
        theControl.stop();
    }

    public void testJobControl() throws Exception {
        doJobControlTest();
    }
    
    public static void main(String[] args) {
        TestJobControl test = new TestJobControl();
        try {
            test.testJobControl();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
