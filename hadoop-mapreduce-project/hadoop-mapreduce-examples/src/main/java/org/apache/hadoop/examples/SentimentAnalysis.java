package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.lang.Math;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


public class SentimentAnalysis {

    public static class SentimentMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private final Text user = new Text();

        Map<String, Integer> wordList = new HashMap<>();

//        function to split dictionary words and their sentiment scores which are comma separated
        public static String[] split(final String line, final char delimiter)
        {
            String[] result = new String[2]; //length of the array will be 2 for a word and a score
            int j = line.indexOf(delimiter, 0); // first substring (i.e, the dictionary word before the comma)
            result[0] = line.substring(0,j);
            result[1] = line.substring(j+1);

            return result;
        }

//        function to split username and tweets in a way that tweets dont get split if they contain a comma
        public static String[] splitTweets(final String line, final char delimiter)
        {
            int count = 0;
            int i = line.length()-1;
            StringBuilder s = new StringBuilder();
            String[] data = new String[2];
            char ch;
            while(i>=0){ //iterating the string from the end
                ch = line.charAt(i); //char at ith position
                s.insert(0, ch); //adding the char to the string
                if (ch == delimiter){
                    if(line.charAt(i+1) == '\"' && line.charAt(i-1) == '\"'){   //if the char is a delimiter(i.e, comma here) and it is surrounded by quotes only then split
                        count++;
                        if (count == 1){ //the first value will be a tweet
                            data[1] = s.substring(2, s.length()-1);
                            s = new StringBuilder();
                        }
                        if (count == 2){ //the second value will be an username
                            data[0] = s.substring(2, s.length()-1);
                            break;
                        }

                    }
                }
                i--;
            }
            return data;

        }

//        function to read the dictionary word list from file and create a hashmap of word, sentiment score
        public void getList(Context context
        ) throws IOException {

            URI[] files = context.getCacheFiles();
            String line = "";
            if (files != null && files.length>0){
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(files[0].toString());
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    while ((line = reader.readLine()) != null) {
                        String[] words = split(line, ','); //splits the words and creates a hashmap of word and its sentiment score
                        wordList.put(words[0], Integer.parseInt((words[1])));
                    }
                } catch (Exception e) {
                    System.out.println("Unable to read the words.txt file");
                    e.printStackTrace();
                    System.exit(1);
                }
            }

        }

//        function to calculate the sentiment score and map each user with the sentiment score
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            getList(context);

            String [] data = splitTweets(value.toString(), ',');
            user.set(data[0]);

            StringTokenizer tweetLines = new StringTokenizer(data[1],".");

            //calculating the sentiment score
            while (tweetLines.hasMoreTokens()) {
                IntWritable sentiment = new IntWritable();
                sentiment.set(0);
                boolean negationExists = false;
                int negationIndex = -1;
                int currIndex = -1;
                String line = tweetLines.nextToken();


                StringTokenizer words = new StringTokenizer(line);
                while(words.hasMoreTokens()){
                    String word = words.nextToken();
                    currIndex = words.countTokens() + 1;
                    if(wordList.containsKey(word.toLowerCase())) { //if the wordlist contains a word from the tweet
                        if (wordList.get(word.toLowerCase()) == 0) { //checking if it's a negation word
                            negationExists = true;
                            negationIndex = words.countTokens() + 1;
                        }
                        else if (wordList.get(word.toLowerCase()) == 1) {
                            sentiment.set(sentiment.get()+1); //increasing the score for a positive word
                            if(negationExists && Math.abs(currIndex - negationIndex) <= 3){ //checking if negation word present and if its 3 positions before or after a word
                                sentiment.set(sentiment.get()-2); //if negation exists then its a negative sentiment so decreasing twice, once for the previous increase in score and one for the negative sentiment
                            }
                        }
                        else if(wordList.get(word.toLowerCase()) == -1){
                            sentiment.set(sentiment.get()-1); //decreasing the score for a negative word
                            if(negationExists && Math.abs(currIndex - negationIndex) <= 3){ //checking if negation word exists and if its 3 positions before or after a word
                                sentiment.set(sentiment.get()+2); //if negation exists then its a positive sentiment so increasing twice, once for the previous decrease in score and one for the positive sentiment
                            }
                        }
                    }
                }
                context.write(user, sentiment);

            }
        }
    }

//    function to sum up the sentiment score for each user by performing the reduce operation on the <user,sentiment> pairs.
    public static class SentimentReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable sentiment = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            sentiment.set(sum);
            context.write(key, sentiment);
        }
    }

    public static void main(String[] args) throws Exception {

        String maxSize = "33554432"; //optional argument
        int numMaps = 100; //optional argument

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Incorrect number of arguments");
            System.exit(2);
        }

        if (otherArgs.length >= 4) {
            maxSize = otherArgs[3];
        }
        if (otherArgs.length >= 5) {
            numMaps = Integer.parseInt(otherArgs[4]);
        }

        Job job = Job.getInstance(conf, "sentiment analysis");
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentMapper.class);
        job.setCombinerClass(SentimentReducer.class);
        job.setReducerClass(SentimentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", maxSize);
        conf.setInt(JobContext.NUM_MAPS, numMaps);

        try {
            job.addCacheFile(new URI(otherArgs[2])); //make it configurable
        } catch (Exception e) {
            System.out.println("Unable to open words.txt");
            e.printStackTrace();
            System.exit(1);
        }

        TextInputFormat.addInputPath(job, new Path(otherArgs[0])); //first argument is the input dataset path
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //second argument is the output file path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}