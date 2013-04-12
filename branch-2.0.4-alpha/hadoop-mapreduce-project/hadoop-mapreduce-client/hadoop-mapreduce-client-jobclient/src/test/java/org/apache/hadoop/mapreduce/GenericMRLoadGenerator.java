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

package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenericMRLoadGenerator extends Configured implements Tool {
  public static String MAP_PRESERVE_PERCENT = 
    "mapreduce.loadgen.sort.map.preserve.percent";
  public static String REDUCE_PRESERVE_PERCENT = 
    "mapreduce.loadgen.sort.reduce.preserve.percent";
  public static String INDIRECT_INPUT_FORMAT = 
    "mapreduce.loadgen.indirect.input.format";
  public static String INDIRECT_INPUT_FILE = 
    "mapreduce.loadgen.indirect.input.file";
  
  protected static int printUsage() {
    System.err.println(
    "Usage: [-m <maps>] [-r <reduces>]\n" +
    "       [-keepmap <percent>] [-keepred <percent>]\n" +
    "       [-indir <path>] [-outdir <path]\n" +
    "       [-inFormat[Indirect] <InputFormat>] [-outFormat <OutputFormat>]\n" +
    "       [-outKey <WritableComparable>] [-outValue <Writable>]\n");
    GenericOptionsParser.printGenericCommandUsage(System.err);
    return -1;
  }


  /**
   * Configure a job given argv.
   */
  public static boolean parseArgs(String[] argv, Job job) throws IOException {
    if (argv.length < 1) {
      return 0 == printUsage();
    }
    for(int i=0; i < argv.length; ++i) {
      if (argv.length == i + 1) {
        System.out.println("ERROR: Required parameter missing from " +
            argv[i]);
        return 0 == printUsage();
      }
      try {
        if ("-r".equals(argv[i])) {
          job.setNumReduceTasks(Integer.parseInt(argv[++i]));
        } else if ("-inFormat".equals(argv[i])) {
          job.setInputFormatClass(
              Class.forName(argv[++i]).asSubclass(InputFormat.class));
        } else if ("-outFormat".equals(argv[i])) {
          job.setOutputFormatClass(
              Class.forName(argv[++i]).asSubclass(OutputFormat.class));
        } else if ("-outKey".equals(argv[i])) {
          job.setOutputKeyClass(
            Class.forName(argv[++i]).asSubclass(WritableComparable.class));
        } else if ("-outValue".equals(argv[i])) {
          job.setOutputValueClass(
            Class.forName(argv[++i]).asSubclass(Writable.class));
        } else if ("-keepmap".equals(argv[i])) {
          job.getConfiguration().set(MAP_PRESERVE_PERCENT, argv[++i]);
        } else if ("-keepred".equals(argv[i])) {
          job.getConfiguration().set(REDUCE_PRESERVE_PERCENT, argv[++i]);
        } else if ("-outdir".equals(argv[i])) {
          FileOutputFormat.setOutputPath(job, new Path(argv[++i]));
        } else if ("-indir".equals(argv[i])) {
          FileInputFormat.addInputPaths(job, argv[++i]);
        } else if ("-inFormatIndirect".equals(argv[i])) {
          job.getConfiguration().setClass(INDIRECT_INPUT_FORMAT,
              Class.forName(argv[++i]).asSubclass(InputFormat.class),
              InputFormat.class);
          job.setInputFormatClass(IndirectInputFormat.class);
        } else {
          System.out.println("Unexpected argument: " + argv[i]);
          return 0 == printUsage();
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + argv[i]);
        return 0 == printUsage();
      } catch (Exception e) {
        throw (IOException)new IOException().initCause(e);
      }
    }
    return true;
  }

  public int run(String [] argv) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJarByClass(GenericMRLoadGenerator.class);
    job.setMapperClass(SampleMapper.class);
    job.setReducerClass(SampleReducer.class);
    if (!parseArgs(argv, job)) {
      return -1;
    }

    Configuration conf = job.getConfiguration();
    if (null == FileOutputFormat.getOutputPath(job)) {
      // No output dir? No writes
      job.setOutputFormatClass(NullOutputFormat.class);
    }

    if (0 == FileInputFormat.getInputPaths(job).length) {
      // No input dir? Generate random data
      System.err.println("No input path; ignoring InputFormat");
      confRandom(job);
    } else if (null != conf.getClass(INDIRECT_INPUT_FORMAT, null)) {
      // specified IndirectInputFormat? Build src list
      JobClient jClient = new JobClient(conf);  
      Path tmpDir = new Path("/tmp");
      Random r = new Random();
      Path indirInputFile = new Path(tmpDir,
          Integer.toString(r.nextInt(Integer.MAX_VALUE), 36) + "_files");
      conf.set(INDIRECT_INPUT_FILE, indirInputFile.toString());
      SequenceFile.Writer writer = SequenceFile.createWriter(
          tmpDir.getFileSystem(conf), conf, indirInputFile,
          LongWritable.class, Text.class,
          SequenceFile.CompressionType.NONE);
      try {
        for (Path p : FileInputFormat.getInputPaths(job)) {
          FileSystem fs = p.getFileSystem(conf);
          Stack<Path> pathstack = new Stack<Path>();
          pathstack.push(p);
          while (!pathstack.empty()) {
            for (FileStatus stat : fs.listStatus(pathstack.pop())) {
              if (stat.isDirectory()) {
                if (!stat.getPath().getName().startsWith("_")) {
                  pathstack.push(stat.getPath());
                }
              } else {
                writer.sync();
                writer.append(new LongWritable(stat.getLen()),
                    new Text(stat.getPath().toUri().toString()));
              }
            }
          }
        }
      } finally {
        writer.close();
      }
    }

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    int ret = job.waitForCompletion(true) ? 0 : 1;
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " +
                       (endTime.getTime() - startTime.getTime()) /1000 +
                       " seconds.");

    return ret;
  }

  /**
   * Main driver/hook into ToolRunner.
   */
  public static void main(String[] argv) throws Exception {
    int res =
      ToolRunner.run(new Configuration(), new GenericMRLoadGenerator(), argv);
    System.exit(res);
  }

  static class RandomInputFormat extends InputFormat<Text, Text> {

    public List<InputSplit> getSplits(JobContext job) {
      int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
      List<InputSplit> splits = new ArrayList<InputSplit>();
      for (int i = 0; i < numSplits; ++i) {
        splits.add(new IndirectInputFormat.IndirectSplit(
            new Path("ignore" + i), 1));
      }
      return splits;
    }

    public RecordReader<Text,Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException {
      final IndirectInputFormat.IndirectSplit clSplit =
        (IndirectInputFormat.IndirectSplit)split;
      return new RecordReader<Text,Text>() {
        boolean once = true;
        Text key = new Text();
        Text value = new Text();
        public boolean nextKeyValue() {
          if (once) {
            key.set(clSplit.getPath().toString());
            once = false;
            return true;
          }
          return false;
        }
        public void initialize(InputSplit split, TaskAttemptContext context) 
            throws IOException, InterruptedException {}
        public Text getCurrentKey() { return key; }
        public Text getCurrentValue() { return value; }
        public void close() { }
        public float getProgress() { return 0.0f; }
      };
    }
  }

  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }

  static class RandomMapOutput extends Mapper<Text,Text,Text,Text> {
    StringBuilder sentence = new StringBuilder();
    int keymin;
    int keymax;
    int valmin;
    int valmax;
    long bytesToWrite;
    Random r = new Random();

    private int generateSentence(Text t, int noWords) {
      sentence.setLength(0);
      --noWords;
      for (int i = 0; i < noWords; ++i) {
        sentence.append(words[r.nextInt(words.length)]);
        sentence.append(" ");
      }
      if (noWords >= 0) sentence.append(words[r.nextInt(words.length)]);
      t.set(sentence.toString());
      return sentence.length();
    }

    public void setup(Context context) {
      Configuration conf = new Configuration();
      bytesToWrite = conf.getLong(RandomTextWriter.BYTES_PER_MAP,
                                    1*1024*1024*1024);
      keymin = conf.getInt(RandomTextWriter.MIN_KEY, 5);
      keymax = conf.getInt(RandomTextWriter.MAX_KEY, 10);
      valmin = conf.getInt(RandomTextWriter.MIN_VALUE, 5);
      valmax = conf.getInt(RandomTextWriter.MAX_VALUE, 10);
    }

    public void map(Text key, Text val, Context context) 
        throws IOException, InterruptedException {
      long acc = 0L;
      long recs = 0;
      final int keydiff = keymax - keymin;
      final int valdiff = valmax - valmin;
      for (long i = 0L; acc < bytesToWrite; ++i) {
        int recacc = 0;
        recacc += generateSentence(key, keymin +
            (0 == keydiff ? 0 : r.nextInt(keydiff)));
        recacc += generateSentence(val, valmin +
            (0 == valdiff ? 0 : r.nextInt(valdiff)));
        context.write(key, val);
        ++recs;
        acc += recacc;
        context.getCounter(Counters.BYTES_WRITTEN).increment(recacc);
        context.getCounter(Counters.RECORDS_WRITTEN).increment(1);
        context.setStatus(acc + "/" + (bytesToWrite - acc) + " bytes");
      }
      context.setStatus("Wrote " + recs + " records");
    }

  }

  /**
   * When no input dir is specified, generate random data.
   */
  protected static void confRandom(Job job)
      throws IOException {
    // from RandomWriter
    job.setInputFormatClass(RandomInputFormat.class);
    job.setMapperClass(RandomMapOutput.class);

    Configuration conf = job.getConfiguration();
    final ClusterStatus cluster = new JobClient(conf).getClusterStatus();
    int numMapsPerHost = conf.getInt(RandomTextWriter.MAPS_PER_HOST, 10);
    long numBytesToWritePerMap =
      conf.getLong(RandomTextWriter.BYTES_PER_MAP, 1*1024*1024*1024);
    if (numBytesToWritePerMap == 0) {
      throw new IOException(
          "Cannot have " + RandomTextWriter.BYTES_PER_MAP + " set to 0");
    }
    long totalBytesToWrite = conf.getLong(RandomTextWriter.TOTAL_BYTES,
         numMapsPerHost * numBytesToWritePerMap * cluster.getTaskTrackers());
    int numMaps = (int)(totalBytesToWrite / numBytesToWritePerMap);
    if (numMaps == 0 && totalBytesToWrite > 0) {
      numMaps = 1;
      conf.setLong(RandomTextWriter.BYTES_PER_MAP, totalBytesToWrite);
    }
    conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
  }


  // Sampling //
  static abstract class SampleMapBase<K extends WritableComparable<?>,
      V extends Writable> extends Mapper<K, V, K, V> {

    private long total;
    private long kept = 0;
    private float keep;

    public void setup(Context context) {
      this.keep = context.getConfiguration().
        getFloat(MAP_PRESERVE_PERCENT, (float)100.0) / (float)100.0;
    }
    
    protected void emit(K key, V val, Context context)
        throws IOException, InterruptedException {
      ++total;
      while((float) kept / total < keep) {
        ++kept;
        context.write(key, val);
      }
    }
  }

  static abstract class SampleReduceBase<K extends WritableComparable<?>,
      V extends Writable> extends Reducer<K, V, K, V> {
    private long total;
    private long kept = 0;
    private float keep;

    public void setup(Context context) {
      this.keep = context.getConfiguration().getFloat(
        REDUCE_PRESERVE_PERCENT, (float)100.0) / (float)100.0;
    }

    protected void emit(K key, V val, Context context)
        throws IOException, InterruptedException {
      ++total;
      while((float) kept / total < keep) {
        ++kept;
        context.write(key, val);
      }
    }
  }
  
  public static class SampleMapper<K extends WritableComparable<?>, 
                                   V extends Writable>
      extends SampleMapBase<K,V> {

    public void map(K key, V val, Context context)
        throws IOException, InterruptedException {
      emit(key, val, context);
    }

  }

  public static class SampleReducer<K extends WritableComparable<?>, 
                                    V extends Writable>
      extends SampleReduceBase<K,V> {

    public void reduce(K key, Iterable<V> values, Context context)
        throws IOException, InterruptedException {
      for (V value : values) {
        emit(key, value, context);
      }
    }

  }

  // Indirect reads //

  /**
   * Obscures the InputFormat and location information to simulate maps
   * reading input from arbitrary locations (&quot;indirect&quot; reads).
   */
  static class IndirectInputFormat<K, V> extends InputFormat<K, V> {

    static class IndirectSplit extends InputSplit {
      Path file;
      long len;
      public IndirectSplit() { }
      public IndirectSplit(Path file, long len) {
        this.file = file;
        this.len = len;
      }
      public Path getPath() { return file; }
      public long getLength() { return len; }
      public String[] getLocations() throws IOException {
        return new String[]{};
      }
      public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, file.toString());
        WritableUtils.writeVLong(out, len);
      }
      public void readFields(DataInput in) throws IOException {
        file = new Path(WritableUtils.readString(in));
        len = WritableUtils.readVLong(in);
      }
    }

    public List<InputSplit> getSplits(JobContext job)
        throws IOException {

      Configuration conf = job.getConfiguration();
      Path src = new Path(conf.get(INDIRECT_INPUT_FILE, null));
      FileSystem fs = src.getFileSystem(conf);

      List<InputSplit> splits = new ArrayList<InputSplit>();
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (SequenceFile.Reader sl = new SequenceFile.Reader(fs, src, conf);
           sl.next(key, value);) {
        splits.add(new IndirectSplit(new Path(value.toString()), key.get()));
      }

      return splits;
    }

	@SuppressWarnings("unchecked")
	public RecordReader<K, V> createRecordReader(InputSplit split, 
        TaskAttemptContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      InputFormat<K, V> indirIF = (InputFormat)ReflectionUtils.newInstance(
          conf.getClass(INDIRECT_INPUT_FORMAT,
            SequenceFileInputFormat.class), conf);
      IndirectSplit is = ((IndirectSplit)split);
      return indirIF.createRecordReader(new FileSplit(is.getPath(), 0,
            is.getLength(), (String[])null), context);
    }
  }

  /**
   * A random list of 1000 words from /usr/share/dict/words
   */
  private static final String[] words = {
    "diurnalness", "Homoiousian", "spiranthic", "tetragynian",
    "silverhead", "ungreat", "lithograph", "exploiter",
    "physiologian", "by", "hellbender", "Filipendula",
    "undeterring", "antiscolic", "pentagamist", "hypoid",
    "cacuminal", "sertularian", "schoolmasterism", "nonuple",
    "gallybeggar", "phytonic", "swearingly", "nebular",
    "Confervales", "thermochemically", "characinoid", "cocksuredom",
    "fallacious", "feasibleness", "debromination", "playfellowship",
    "tramplike", "testa", "participatingly", "unaccessible",
    "bromate", "experientialist", "roughcast", "docimastical",
    "choralcelo", "blightbird", "peptonate", "sombreroed",
    "unschematized", "antiabolitionist", "besagne", "mastication",
    "bromic", "sviatonosite", "cattimandoo", "metaphrastical",
    "endotheliomyoma", "hysterolysis", "unfulminated", "Hester",
    "oblongly", "blurredness", "authorling", "chasmy",
    "Scorpaenidae", "toxihaemia", "Dictograph", "Quakerishly",
    "deaf", "timbermonger", "strammel", "Thraupidae",
    "seditious", "plerome", "Arneb", "eristically",
    "serpentinic", "glaumrie", "socioromantic", "apocalypst",
    "tartrous", "Bassaris", "angiolymphoma", "horsefly",
    "kenno", "astronomize", "euphemious", "arsenide",
    "untongued", "parabolicness", "uvanite", "helpless",
    "gemmeous", "stormy", "templar", "erythrodextrin",
    "comism", "interfraternal", "preparative", "parastas",
    "frontoorbital", "Ophiosaurus", "diopside", "serosanguineous",
    "ununiformly", "karyological", "collegian", "allotropic",
    "depravity", "amylogenesis", "reformatory", "epidymides",
    "pleurotropous", "trillium", "dastardliness", "coadvice",
    "embryotic", "benthonic", "pomiferous", "figureheadship",
    "Megaluridae", "Harpa", "frenal", "commotion",
    "abthainry", "cobeliever", "manilla", "spiciferous",
    "nativeness", "obispo", "monilioid", "biopsic",
    "valvula", "enterostomy", "planosubulate", "pterostigma",
    "lifter", "triradiated", "venialness", "tum",
    "archistome", "tautness", "unswanlike", "antivenin",
    "Lentibulariaceae", "Triphora", "angiopathy", "anta",
    "Dawsonia", "becomma", "Yannigan", "winterproof",
    "antalgol", "harr", "underogating", "ineunt",
    "cornberry", "flippantness", "scyphostoma", "approbation",
    "Ghent", "Macraucheniidae", "scabbiness", "unanatomized",
    "photoelasticity", "eurythermal", "enation", "prepavement",
    "flushgate", "subsequentially", "Edo", "antihero",
    "Isokontae", "unforkedness", "porriginous", "daytime",
    "nonexecutive", "trisilicic", "morphiomania", "paranephros",
    "botchedly", "impugnation", "Dodecatheon", "obolus",
    "unburnt", "provedore", "Aktistetae", "superindifference",
    "Alethea", "Joachimite", "cyanophilous", "chorograph",
    "brooky", "figured", "periclitation", "quintette",
    "hondo", "ornithodelphous", "unefficient", "pondside",
    "bogydom", "laurinoxylon", "Shiah", "unharmed",
    "cartful", "noncrystallized", "abusiveness", "cromlech",
    "japanned", "rizzomed", "underskin", "adscendent",
    "allectory", "gelatinousness", "volcano", "uncompromisingly",
    "cubit", "idiotize", "unfurbelowed", "undinted",
    "magnetooptics", "Savitar", "diwata", "ramosopalmate",
    "Pishquow", "tomorn", "apopenptic", "Haversian",
    "Hysterocarpus", "ten", "outhue", "Bertat",
    "mechanist", "asparaginic", "velaric", "tonsure",
    "bubble", "Pyrales", "regardful", "glyphography",
    "calabazilla", "shellworker", "stradametrical", "havoc",
    "theologicopolitical", "sawdust", "diatomaceous", "jajman",
    "temporomastoid", "Serrifera", "Ochnaceae", "aspersor",
    "trailmaking", "Bishareen", "digitule", "octogynous",
    "epididymitis", "smokefarthings", "bacillite", "overcrown",
    "mangonism", "sirrah", "undecorated", "psychofugal",
    "bismuthiferous", "rechar", "Lemuridae", "frameable",
    "thiodiazole", "Scanic", "sportswomanship", "interruptedness",
    "admissory", "osteopaedion", "tingly", "tomorrowness",
    "ethnocracy", "trabecular", "vitally", "fossilism",
    "adz", "metopon", "prefatorial", "expiscate",
    "diathermacy", "chronist", "nigh", "generalizable",
    "hysterogen", "aurothiosulphuric", "whitlowwort", "downthrust",
    "Protestantize", "monander", "Itea", "chronographic",
    "silicize", "Dunlop", "eer", "componental",
    "spot", "pamphlet", "antineuritic", "paradisean",
    "interruptor", "debellator", "overcultured", "Florissant",
    "hyocholic", "pneumatotherapy", "tailoress", "rave",
    "unpeople", "Sebastian", "thermanesthesia", "Coniferae",
    "swacking", "posterishness", "ethmopalatal", "whittle",
    "analgize", "scabbardless", "naught", "symbiogenetically",
    "trip", "parodist", "columniform", "trunnel",
    "yawler", "goodwill", "pseudohalogen", "swangy",
    "cervisial", "mediateness", "genii", "imprescribable",
    "pony", "consumptional", "carposporangial", "poleax",
    "bestill", "subfebrile", "sapphiric", "arrowworm",
    "qualminess", "ultraobscure", "thorite", "Fouquieria",
    "Bermudian", "prescriber", "elemicin", "warlike",
    "semiangle", "rotular", "misthread", "returnability",
    "seraphism", "precostal", "quarried", "Babylonism",
    "sangaree", "seelful", "placatory", "pachydermous",
    "bozal", "galbulus", "spermaphyte", "cumbrousness",
    "pope", "signifier", "Endomycetaceae", "shallowish",
    "sequacity", "periarthritis", "bathysphere", "pentosuria",
    "Dadaism", "spookdom", "Consolamentum", "afterpressure",
    "mutter", "louse", "ovoviviparous", "corbel",
    "metastoma", "biventer", "Hydrangea", "hogmace",
    "seizing", "nonsuppressed", "oratorize", "uncarefully",
    "benzothiofuran", "penult", "balanocele", "macropterous",
    "dishpan", "marten", "absvolt", "jirble",
    "parmelioid", "airfreighter", "acocotl", "archesporial",
    "hypoplastral", "preoral", "quailberry", "cinque",
    "terrestrially", "stroking", "limpet", "moodishness",
    "canicule", "archididascalian", "pompiloid", "overstaid",
    "introducer", "Italical", "Christianopaganism", "prescriptible",
    "subofficer", "danseuse", "cloy", "saguran",
    "frictionlessly", "deindividualization", "Bulanda", "ventricous",
    "subfoliar", "basto", "scapuloradial", "suspend",
    "stiffish", "Sphenodontidae", "eternal", "verbid",
    "mammonish", "upcushion", "barkometer", "concretion",
    "preagitate", "incomprehensible", "tristich", "visceral",
    "hemimelus", "patroller", "stentorophonic", "pinulus",
    "kerykeion", "brutism", "monstership", "merciful",
    "overinstruct", "defensibly", "bettermost", "splenauxe",
    "Mormyrus", "unreprimanded", "taver", "ell",
    "proacquittal", "infestation", "overwoven", "Lincolnlike",
    "chacona", "Tamil", "classificational", "lebensraum",
    "reeveland", "intuition", "Whilkut", "focaloid",
    "Eleusinian", "micromembrane", "byroad", "nonrepetition",
    "bacterioblast", "brag", "ribaldrous", "phytoma",
    "counteralliance", "pelvimetry", "pelf", "relaster",
    "thermoresistant", "aneurism", "molossic", "euphonym",
    "upswell", "ladhood", "phallaceous", "inertly",
    "gunshop", "stereotypography", "laryngic", "refasten",
    "twinling", "oflete", "hepatorrhaphy", "electrotechnics",
    "cockal", "guitarist", "topsail", "Cimmerianism",
    "larklike", "Llandovery", "pyrocatechol", "immatchable",
    "chooser", "metrocratic", "craglike", "quadrennial",
    "nonpoisonous", "undercolored", "knob", "ultratense",
    "balladmonger", "slait", "sialadenitis", "bucketer",
    "magnificently", "unstipulated", "unscourged", "unsupercilious",
    "packsack", "pansophism", "soorkee", "percent",
    "subirrigate", "champer", "metapolitics", "spherulitic",
    "involatile", "metaphonical", "stachyuraceous", "speckedness",
    "bespin", "proboscidiform", "gul", "squit",
    "yeelaman", "peristeropode", "opacousness", "shibuichi",
    "retinize", "yote", "misexposition", "devilwise",
    "pumpkinification", "vinny", "bonze", "glossing",
    "decardinalize", "transcortical", "serphoid", "deepmost",
    "guanajuatite", "wemless", "arval", "lammy",
    "Effie", "Saponaria", "tetrahedral", "prolificy",
    "excerpt", "dunkadoo", "Spencerism", "insatiately",
    "Gilaki", "oratorship", "arduousness", "unbashfulness",
    "Pithecolobium", "unisexuality", "veterinarian", "detractive",
    "liquidity", "acidophile", "proauction", "sural",
    "totaquina", "Vichyite", "uninhabitedness", "allegedly",
    "Gothish", "manny", "Inger", "flutist",
    "ticktick", "Ludgatian", "homotransplant", "orthopedical",
    "diminutively", "monogoneutic", "Kenipsim", "sarcologist",
    "drome", "stronghearted", "Fameuse", "Swaziland",
    "alen", "chilblain", "beatable", "agglomeratic",
    "constitutor", "tendomucoid", "porencephalous", "arteriasis",
    "boser", "tantivy", "rede", "lineamental",
    "uncontradictableness", "homeotypical", "masa", "folious",
    "dosseret", "neurodegenerative", "subtransverse", "Chiasmodontidae",
    "palaeotheriodont", "unstressedly", "chalcites", "piquantness",
    "lampyrine", "Aplacentalia", "projecting", "elastivity",
    "isopelletierin", "bladderwort", "strander", "almud",
    "iniquitously", "theologal", "bugre", "chargeably",
    "imperceptivity", "meriquinoidal", "mesophyte", "divinator",
    "perfunctory", "counterappellant", "synovial", "charioteer",
    "crystallographical", "comprovincial", "infrastapedial", "pleasurehood",
    "inventurous", "ultrasystematic", "subangulated", "supraoesophageal",
    "Vaishnavism", "transude", "chrysochrous", "ungrave",
    "reconciliable", "uninterpleaded", "erlking", "wherefrom",
    "aprosopia", "antiadiaphorist", "metoxazine", "incalculable",
    "umbellic", "predebit", "foursquare", "unimmortal",
    "nonmanufacture", "slangy", "predisputant", "familist",
    "preaffiliate", "friarhood", "corelysis", "zoonitic",
    "halloo", "paunchy", "neuromimesis", "aconitine",
    "hackneyed", "unfeeble", "cubby", "autoschediastical",
    "naprapath", "lyrebird", "inexistency", "leucophoenicite",
    "ferrogoslarite", "reperuse", "uncombable", "tambo",
    "propodiale", "diplomatize", "Russifier", "clanned",
    "corona", "michigan", "nonutilitarian", "transcorporeal",
    "bought", "Cercosporella", "stapedius", "glandularly",
    "pictorially", "weism", "disilane", "rainproof",
    "Caphtor", "scrubbed", "oinomancy", "pseudoxanthine",
    "nonlustrous", "redesertion", "Oryzorictinae", "gala",
    "Mycogone", "reappreciate", "cyanoguanidine", "seeingness",
    "breadwinner", "noreast", "furacious", "epauliere",
    "omniscribent", "Passiflorales", "uninductive", "inductivity",
    "Orbitolina", "Semecarpus", "migrainoid", "steprelationship",
    "phlogisticate", "mesymnion", "sloped", "edificator",
    "beneficent", "culm", "paleornithology", "unurban",
    "throbless", "amplexifoliate", "sesquiquintile", "sapience",
    "astucious", "dithery", "boor", "ambitus",
    "scotching", "uloid", "uncompromisingness", "hoove",
    "waird", "marshiness", "Jerusalem", "mericarp",
    "unevoked", "benzoperoxide", "outguess", "pyxie",
    "hymnic", "euphemize", "mendacity", "erythremia",
    "rosaniline", "unchatteled", "lienteria", "Bushongo",
    "dialoguer", "unrepealably", "rivethead", "antideflation",
    "vinegarish", "manganosiderite", "doubtingness", "ovopyriform",
    "Cephalodiscus", "Muscicapa", "Animalivora", "angina",
    "planispheric", "ipomoein", "cuproiodargyrite", "sandbox",
    "scrat", "Munnopsidae", "shola", "pentafid",
    "overstudiousness", "times", "nonprofession", "appetible",
    "valvulotomy", "goladar", "uniarticular", "oxyterpene",
    "unlapsing", "omega", "trophonema", "seminonflammable",
    "circumzenithal", "starer", "depthwise", "liberatress",
    "unleavened", "unrevolting", "groundneedle", "topline",
    "wandoo", "umangite", "ordinant", "unachievable",
    "oversand", "snare", "avengeful", "unexplicit",
    "mustafina", "sonable", "rehabilitative", "eulogization",
    "papery", "technopsychology", "impressor", "cresylite",
    "entame", "transudatory", "scotale", "pachydermatoid",
    "imaginary", "yeat", "slipped", "stewardship",
    "adatom", "cockstone", "skyshine", "heavenful",
    "comparability", "exprobratory", "dermorhynchous", "parquet",
    "cretaceous", "vesperal", "raphis", "undangered",
    "Glecoma", "engrain", "counteractively", "Zuludom",
    "orchiocatabasis", "Auriculariales", "warriorwise", "extraorganismal",
    "overbuilt", "alveolite", "tetchy", "terrificness",
    "widdle", "unpremonished", "rebilling", "sequestrum",
    "equiconvex", "heliocentricism", "catabaptist", "okonite",
    "propheticism", "helminthagogic", "calycular", "giantly",
    "wingable", "golem", "unprovided", "commandingness",
    "greave", "haply", "doina", "depressingly",
    "subdentate", "impairment", "decidable", "neurotrophic",
    "unpredict", "bicorporeal", "pendulant", "flatman",
    "intrabred", "toplike", "Prosobranchiata", "farrantly",
    "toxoplasmosis", "gorilloid", "dipsomaniacal", "aquiline",
    "atlantite", "ascitic", "perculsive", "prospectiveness",
    "saponaceous", "centrifugalization", "dinical", "infravaginal",
    "beadroll", "affaite", "Helvidian", "tickleproof",
    "abstractionism", "enhedge", "outwealth", "overcontribute",
    "coldfinch", "gymnastic", "Pincian", "Munychian",
    "codisjunct", "quad", "coracomandibular", "phoenicochroite",
    "amender", "selectivity", "putative", "semantician",
    "lophotrichic", "Spatangoidea", "saccharogenic", "inferent",
    "Triconodonta", "arrendation", "sheepskin", "taurocolla",
    "bunghole", "Machiavel", "triakistetrahedral", "dehairer",
    "prezygapophysial", "cylindric", "pneumonalgia", "sleigher",
    "emir", "Socraticism", "licitness", "massedly",
    "instructiveness", "sturdied", "redecrease", "starosta",
    "evictor", "orgiastic", "squdge", "meloplasty",
    "Tsonecan", "repealableness", "swoony", "myesthesia",
    "molecule", "autobiographist", "reciprocation", "refective",
    "unobservantness", "tricae", "ungouged", "floatability",
    "Mesua", "fetlocked", "chordacentrum", "sedentariness",
    "various", "laubanite", "nectopod", "zenick",
    "sequentially", "analgic", "biodynamics", "posttraumatic",
    "nummi", "pyroacetic", "bot", "redescend",
    "dispermy", "undiffusive", "circular", "trillion",
    "Uraniidae", "ploration", "discipular", "potentness",
    "sud", "Hu", "Eryon", "plugger",
    "subdrainage", "jharal", "abscission", "supermarket",
    "countergabion", "glacierist", "lithotresis", "minniebush",
    "zanyism", "eucalypteol", "sterilely", "unrealize",
    "unpatched", "hypochondriacism", "critically", "cheesecutter",
  };
}
