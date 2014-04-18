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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program uses map/reduce to just run a distributed job where there is
 * no interaction between the tasks and each task writes a large unsorted
 * random sequence of words.
 * In order for this program to generate data for terasort with a 5-10 words
 * per key and 20-100 words per value, have the following config:
 * <xmp>
 * <?xml version="1.0"?>
 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 * <configuration>
 *   <property>
 *     <name>mapreduce.randomtextwriter.minwordskey</name>
 *     <value>5</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.maxwordskey</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.minwordsvalue</name>
 *     <value>20</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.maxwordsvalue</name>
 *     <value>100</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.totalbytes</name>
 *     <value>1099511627776</value>
 *   </property>
 * </configuration></xmp>
 * 
 * Equivalently, {@link RandomTextWriter} also supports all the above options
 * and ones supported by {@link Tool} via the command-line.
 * 
 * To run: bin/hadoop jar hadoop-${version}-examples.jar randomtextwriter
 *            [-outFormat <i>output format class</i>] <i>output</i> 
 */
public class RandomTextWriter extends Configured implements Tool {
  public static final String TOTAL_BYTES = 
    "mapreduce.randomtextwriter.totalbytes";
  public static final String BYTES_PER_MAP = 
    "mapreduce.randomtextwriter.bytespermap";
  public static final String MAPS_PER_HOST = 
    "mapreduce.randomtextwriter.mapsperhost";
  public static final String MAX_VALUE = "mapreduce.randomtextwriter.maxwordsvalue";
  public static final String MIN_VALUE = "mapreduce.randomtextwriter.minwordsvalue";
  public static final String MIN_KEY = "mapreduce.randomtextwriter.minwordskey";
  public static final String MAX_KEY = "mapreduce.randomtextwriter.maxwordskey";
  
  static int printUsage() {
    System.out.println("randomtextwriter " +
                       "[-outFormat <output format class>] " + 
                       "<output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return 2;
  }
  
  /**
   * User counters
   */
  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }

  static class RandomTextMapper extends Mapper<Text, Text, Text, Text> {
    
    private long numBytesToWrite;
    private int minWordsInKey;
    private int wordsInKeyRange;
    private int minWordsInValue;
    private int wordsInValueRange;
    private Random random = new Random();
    
    /**
     * Save the configuration value that we need to write the data.
     */
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      numBytesToWrite = conf.getLong(BYTES_PER_MAP,
                                    1*1024*1024*1024);
      minWordsInKey = conf.getInt(MIN_KEY, 5);
      wordsInKeyRange = (conf.getInt(MAX_KEY, 10) - minWordsInKey);
      minWordsInValue = conf.getInt(MIN_VALUE, 10);
      wordsInValueRange = (conf.getInt(MAX_VALUE, 100) - minWordsInValue);
    }
    
    /**
     * Given an output filename, write a bunch of random records to it.
     */
    public void map(Text key, Text value,
                    Context context) throws IOException,InterruptedException {
      int itemCount = 0;
      while (numBytesToWrite > 0) {
        // Generate the key/value 
        int noWordsKey = minWordsInKey + 
          (wordsInKeyRange != 0 ? random.nextInt(wordsInKeyRange) : 0);
        int noWordsValue = minWordsInValue + 
          (wordsInValueRange != 0 ? random.nextInt(wordsInValueRange) : 0);
        Text keyWords = generateSentence(noWordsKey);
        Text valueWords = generateSentence(noWordsValue);
        
        // Write the sentence 
        context.write(keyWords, valueWords);
        
        numBytesToWrite -= (keyWords.getLength() + valueWords.getLength());
        
        // Update counters, progress etc.
        context.getCounter(Counters.BYTES_WRITTEN).increment(
                  keyWords.getLength() + valueWords.getLength());
        context.getCounter(Counters.RECORDS_WRITTEN).increment(1);
        if (++itemCount % 200 == 0) {
          context.setStatus("wrote record " + itemCount + ". " + 
                             numBytesToWrite + " bytes left.");
        }
      }
      context.setStatus("done with " + itemCount + " records.");
    }
    
    private Text generateSentence(int noWords) {
      StringBuffer sentence = new StringBuffer();
      String space = " ";
      for (int i=0; i < noWords; ++i) {
        sentence.append(words[random.nextInt(words.length)]);
        sentence.append(space);
      }
      return new Text(sentence.toString());
    }
  }
  
  /**
   * This is the main routine for launching a distributed random write job.
   * It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
   * The reduce doesn't do anything.
   * 
   * @throws IOException 
   */
  public int run(String[] args) throws Exception {    
    if (args.length == 0) {
      return printUsage();    
    }
    
    Configuration conf = getConf();
    JobClient client = new JobClient(conf);
    ClusterStatus cluster = client.getClusterStatus();
    int numMapsPerHost = conf.getInt(MAPS_PER_HOST, 10);
    long numBytesToWritePerMap = conf.getLong(BYTES_PER_MAP,
                                             1*1024*1024*1024);
    if (numBytesToWritePerMap == 0) {
      System.err.println("Cannot have " + BYTES_PER_MAP +" set to 0");
      return -2;
    }
    long totalBytesToWrite = conf.getLong(TOTAL_BYTES, 
         numMapsPerHost*numBytesToWritePerMap*cluster.getTaskTrackers());
    int numMaps = (int) (totalBytesToWrite / numBytesToWritePerMap);
    if (numMaps == 0 && totalBytesToWrite > 0) {
      numMaps = 1;
      conf.setLong(BYTES_PER_MAP, totalBytesToWrite);
    }
    conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
    
    Job job = new Job(conf);
    
    job.setJarByClass(RandomTextWriter.class);
    job.setJobName("random-text-writer");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setInputFormatClass(RandomWriter.RandomInputFormat.class);
    job.setMapperClass(RandomTextMapper.class);        
    
    Class<? extends OutputFormat> outputFormatClass = 
      SequenceFileOutputFormat.class;
    List<String> otherArgs = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-outFormat".equals(args[i])) {
          outputFormatClass = 
            Class.forName(args[++i]).asSubclass(OutputFormat.class);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage(); // exits
      }
    }

    job.setOutputFormatClass(outputFormatClass);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(0)));
    
    System.out.println("Running " + numMaps + " maps.");
    
    // reducer NONE
    job.setNumReduceTasks(0);
    
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
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RandomTextWriter(), args);
    System.exit(res);
  }

  /**
   * A random list of 1000 words from /usr/share/dict/words
   */
  private static String[] words = {
                                   "diurnalness", "Homoiousian",
                                   "spiranthic", "tetragynian",
                                   "silverhead", "ungreat",
                                   "lithograph", "exploiter",
                                   "physiologian", "by",
                                   "hellbender", "Filipendula",
                                   "undeterring", "antiscolic",
                                   "pentagamist", "hypoid",
                                   "cacuminal", "sertularian",
                                   "schoolmasterism", "nonuple",
                                   "gallybeggar", "phytonic",
                                   "swearingly", "nebular",
                                   "Confervales", "thermochemically",
                                   "characinoid", "cocksuredom",
                                   "fallacious", "feasibleness",
                                   "debromination", "playfellowship",
                                   "tramplike", "testa",
                                   "participatingly", "unaccessible",
                                   "bromate", "experientialist",
                                   "roughcast", "docimastical",
                                   "choralcelo", "blightbird",
                                   "peptonate", "sombreroed",
                                   "unschematized", "antiabolitionist",
                                   "besagne", "mastication",
                                   "bromic", "sviatonosite",
                                   "cattimandoo", "metaphrastical",
                                   "endotheliomyoma", "hysterolysis",
                                   "unfulminated", "Hester",
                                   "oblongly", "blurredness",
                                   "authorling", "chasmy",
                                   "Scorpaenidae", "toxihaemia",
                                   "Dictograph", "Quakerishly",
                                   "deaf", "timbermonger",
                                   "strammel", "Thraupidae",
                                   "seditious", "plerome",
                                   "Arneb", "eristically",
                                   "serpentinic", "glaumrie",
                                   "socioromantic", "apocalypst",
                                   "tartrous", "Bassaris",
                                   "angiolymphoma", "horsefly",
                                   "kenno", "astronomize",
                                   "euphemious", "arsenide",
                                   "untongued", "parabolicness",
                                   "uvanite", "helpless",
                                   "gemmeous", "stormy",
                                   "templar", "erythrodextrin",
                                   "comism", "interfraternal",
                                   "preparative", "parastas",
                                   "frontoorbital", "Ophiosaurus",
                                   "diopside", "serosanguineous",
                                   "ununiformly", "karyological",
                                   "collegian", "allotropic",
                                   "depravity", "amylogenesis",
                                   "reformatory", "epidymides",
                                   "pleurotropous", "trillium",
                                   "dastardliness", "coadvice",
                                   "embryotic", "benthonic",
                                   "pomiferous", "figureheadship",
                                   "Megaluridae", "Harpa",
                                   "frenal", "commotion",
                                   "abthainry", "cobeliever",
                                   "manilla", "spiciferous",
                                   "nativeness", "obispo",
                                   "monilioid", "biopsic",
                                   "valvula", "enterostomy",
                                   "planosubulate", "pterostigma",
                                   "lifter", "triradiated",
                                   "venialness", "tum",
                                   "archistome", "tautness",
                                   "unswanlike", "antivenin",
                                   "Lentibulariaceae", "Triphora",
                                   "angiopathy", "anta",
                                   "Dawsonia", "becomma",
                                   "Yannigan", "winterproof",
                                   "antalgol", "harr",
                                   "underogating", "ineunt",
                                   "cornberry", "flippantness",
                                   "scyphostoma", "approbation",
                                   "Ghent", "Macraucheniidae",
                                   "scabbiness", "unanatomized",
                                   "photoelasticity", "eurythermal",
                                   "enation", "prepavement",
                                   "flushgate", "subsequentially",
                                   "Edo", "antihero",
                                   "Isokontae", "unforkedness",
                                   "porriginous", "daytime",
                                   "nonexecutive", "trisilicic",
                                   "morphiomania", "paranephros",
                                   "botchedly", "impugnation",
                                   "Dodecatheon", "obolus",
                                   "unburnt", "provedore",
                                   "Aktistetae", "superindifference",
                                   "Alethea", "Joachimite",
                                   "cyanophilous", "chorograph",
                                   "brooky", "figured",
                                   "periclitation", "quintette",
                                   "hondo", "ornithodelphous",
                                   "unefficient", "pondside",
                                   "bogydom", "laurinoxylon",
                                   "Shiah", "unharmed",
                                   "cartful", "noncrystallized",
                                   "abusiveness", "cromlech",
                                   "japanned", "rizzomed",
                                   "underskin", "adscendent",
                                   "allectory", "gelatinousness",
                                   "volcano", "uncompromisingly",
                                   "cubit", "idiotize",
                                   "unfurbelowed", "undinted",
                                   "magnetooptics", "Savitar",
                                   "diwata", "ramosopalmate",
                                   "Pishquow", "tomorn",
                                   "apopenptic", "Haversian",
                                   "Hysterocarpus", "ten",
                                   "outhue", "Bertat",
                                   "mechanist", "asparaginic",
                                   "velaric", "tonsure",
                                   "bubble", "Pyrales",
                                   "regardful", "glyphography",
                                   "calabazilla", "shellworker",
                                   "stradametrical", "havoc",
                                   "theologicopolitical", "sawdust",
                                   "diatomaceous", "jajman",
                                   "temporomastoid", "Serrifera",
                                   "Ochnaceae", "aspersor",
                                   "trailmaking", "Bishareen",
                                   "digitule", "octogynous",
                                   "epididymitis", "smokefarthings",
                                   "bacillite", "overcrown",
                                   "mangonism", "sirrah",
                                   "undecorated", "psychofugal",
                                   "bismuthiferous", "rechar",
                                   "Lemuridae", "frameable",
                                   "thiodiazole", "Scanic",
                                   "sportswomanship", "interruptedness",
                                   "admissory", "osteopaedion",
                                   "tingly", "tomorrowness",
                                   "ethnocracy", "trabecular",
                                   "vitally", "fossilism",
                                   "adz", "metopon",
                                   "prefatorial", "expiscate",
                                   "diathermacy", "chronist",
                                   "nigh", "generalizable",
                                   "hysterogen", "aurothiosulphuric",
                                   "whitlowwort", "downthrust",
                                   "Protestantize", "monander",
                                   "Itea", "chronographic",
                                   "silicize", "Dunlop",
                                   "eer", "componental",
                                   "spot", "pamphlet",
                                   "antineuritic", "paradisean",
                                   "interruptor", "debellator",
                                   "overcultured", "Florissant",
                                   "hyocholic", "pneumatotherapy",
                                   "tailoress", "rave",
                                   "unpeople", "Sebastian",
                                   "thermanesthesia", "Coniferae",
                                   "swacking", "posterishness",
                                   "ethmopalatal", "whittle",
                                   "analgize", "scabbardless",
                                   "naught", "symbiogenetically",
                                   "trip", "parodist",
                                   "columniform", "trunnel",
                                   "yawler", "goodwill",
                                   "pseudohalogen", "swangy",
                                   "cervisial", "mediateness",
                                   "genii", "imprescribable",
                                   "pony", "consumptional",
                                   "carposporangial", "poleax",
                                   "bestill", "subfebrile",
                                   "sapphiric", "arrowworm",
                                   "qualminess", "ultraobscure",
                                   "thorite", "Fouquieria",
                                   "Bermudian", "prescriber",
                                   "elemicin", "warlike",
                                   "semiangle", "rotular",
                                   "misthread", "returnability",
                                   "seraphism", "precostal",
                                   "quarried", "Babylonism",
                                   "sangaree", "seelful",
                                   "placatory", "pachydermous",
                                   "bozal", "galbulus",
                                   "spermaphyte", "cumbrousness",
                                   "pope", "signifier",
                                   "Endomycetaceae", "shallowish",
                                   "sequacity", "periarthritis",
                                   "bathysphere", "pentosuria",
                                   "Dadaism", "spookdom",
                                   "Consolamentum", "afterpressure",
                                   "mutter", "louse",
                                   "ovoviviparous", "corbel",
                                   "metastoma", "biventer",
                                   "Hydrangea", "hogmace",
                                   "seizing", "nonsuppressed",
                                   "oratorize", "uncarefully",
                                   "benzothiofuran", "penult",
                                   "balanocele", "macropterous",
                                   "dishpan", "marten",
                                   "absvolt", "jirble",
                                   "parmelioid", "airfreighter",
                                   "acocotl", "archesporial",
                                   "hypoplastral", "preoral",
                                   "quailberry", "cinque",
                                   "terrestrially", "stroking",
                                   "limpet", "moodishness",
                                   "canicule", "archididascalian",
                                   "pompiloid", "overstaid",
                                   "introducer", "Italical",
                                   "Christianopaganism", "prescriptible",
                                   "subofficer", "danseuse",
                                   "cloy", "saguran",
                                   "frictionlessly", "deindividualization",
                                   "Bulanda", "ventricous",
                                   "subfoliar", "basto",
                                   "scapuloradial", "suspend",
                                   "stiffish", "Sphenodontidae",
                                   "eternal", "verbid",
                                   "mammonish", "upcushion",
                                   "barkometer", "concretion",
                                   "preagitate", "incomprehensible",
                                   "tristich", "visceral",
                                   "hemimelus", "patroller",
                                   "stentorophonic", "pinulus",
                                   "kerykeion", "brutism",
                                   "monstership", "merciful",
                                   "overinstruct", "defensibly",
                                   "bettermost", "splenauxe",
                                   "Mormyrus", "unreprimanded",
                                   "taver", "ell",
                                   "proacquittal", "infestation",
                                   "overwoven", "Lincolnlike",
                                   "chacona", "Tamil",
                                   "classificational", "lebensraum",
                                   "reeveland", "intuition",
                                   "Whilkut", "focaloid",
                                   "Eleusinian", "micromembrane",
                                   "byroad", "nonrepetition",
                                   "bacterioblast", "brag",
                                   "ribaldrous", "phytoma",
                                   "counteralliance", "pelvimetry",
                                   "pelf", "relaster",
                                   "thermoresistant", "aneurism",
                                   "molossic", "euphonym",
                                   "upswell", "ladhood",
                                   "phallaceous", "inertly",
                                   "gunshop", "stereotypography",
                                   "laryngic", "refasten",
                                   "twinling", "oflete",
                                   "hepatorrhaphy", "electrotechnics",
                                   "cockal", "guitarist",
                                   "topsail", "Cimmerianism",
                                   "larklike", "Llandovery",
                                   "pyrocatechol", "immatchable",
                                   "chooser", "metrocratic",
                                   "craglike", "quadrennial",
                                   "nonpoisonous", "undercolored",
                                   "knob", "ultratense",
                                   "balladmonger", "slait",
                                   "sialadenitis", "bucketer",
                                   "magnificently", "unstipulated",
                                   "unscourged", "unsupercilious",
                                   "packsack", "pansophism",
                                   "soorkee", "percent",
                                   "subirrigate", "champer",
                                   "metapolitics", "spherulitic",
                                   "involatile", "metaphonical",
                                   "stachyuraceous", "speckedness",
                                   "bespin", "proboscidiform",
                                   "gul", "squit",
                                   "yeelaman", "peristeropode",
                                   "opacousness", "shibuichi",
                                   "retinize", "yote",
                                   "misexposition", "devilwise",
                                   "pumpkinification", "vinny",
                                   "bonze", "glossing",
                                   "decardinalize", "transcortical",
                                   "serphoid", "deepmost",
                                   "guanajuatite", "wemless",
                                   "arval", "lammy",
                                   "Effie", "Saponaria",
                                   "tetrahedral", "prolificy",
                                   "excerpt", "dunkadoo",
                                   "Spencerism", "insatiately",
                                   "Gilaki", "oratorship",
                                   "arduousness", "unbashfulness",
                                   "Pithecolobium", "unisexuality",
                                   "veterinarian", "detractive",
                                   "liquidity", "acidophile",
                                   "proauction", "sural",
                                   "totaquina", "Vichyite",
                                   "uninhabitedness", "allegedly",
                                   "Gothish", "manny",
                                   "Inger", "flutist",
                                   "ticktick", "Ludgatian",
                                   "homotransplant", "orthopedical",
                                   "diminutively", "monogoneutic",
                                   "Kenipsim", "sarcologist",
                                   "drome", "stronghearted",
                                   "Fameuse", "Swaziland",
                                   "alen", "chilblain",
                                   "beatable", "agglomeratic",
                                   "constitutor", "tendomucoid",
                                   "porencephalous", "arteriasis",
                                   "boser", "tantivy",
                                   "rede", "lineamental",
                                   "uncontradictableness", "homeotypical",
                                   "masa", "folious",
                                   "dosseret", "neurodegenerative",
                                   "subtransverse", "Chiasmodontidae",
                                   "palaeotheriodont", "unstressedly",
                                   "chalcites", "piquantness",
                                   "lampyrine", "Aplacentalia",
                                   "projecting", "elastivity",
                                   "isopelletierin", "bladderwort",
                                   "strander", "almud",
                                   "iniquitously", "theologal",
                                   "bugre", "chargeably",
                                   "imperceptivity", "meriquinoidal",
                                   "mesophyte", "divinator",
                                   "perfunctory", "counterappellant",
                                   "synovial", "charioteer",
                                   "crystallographical", "comprovincial",
                                   "infrastapedial", "pleasurehood",
                                   "inventurous", "ultrasystematic",
                                   "subangulated", "supraoesophageal",
                                   "Vaishnavism", "transude",
                                   "chrysochrous", "ungrave",
                                   "reconciliable", "uninterpleaded",
                                   "erlking", "wherefrom",
                                   "aprosopia", "antiadiaphorist",
                                   "metoxazine", "incalculable",
                                   "umbellic", "predebit",
                                   "foursquare", "unimmortal",
                                   "nonmanufacture", "slangy",
                                   "predisputant", "familist",
                                   "preaffiliate", "friarhood",
                                   "corelysis", "zoonitic",
                                   "halloo", "paunchy",
                                   "neuromimesis", "aconitine",
                                   "hackneyed", "unfeeble",
                                   "cubby", "autoschediastical",
                                   "naprapath", "lyrebird",
                                   "inexistency", "leucophoenicite",
                                   "ferrogoslarite", "reperuse",
                                   "uncombable", "tambo",
                                   "propodiale", "diplomatize",
                                   "Russifier", "clanned",
                                   "corona", "michigan",
                                   "nonutilitarian", "transcorporeal",
                                   "bought", "Cercosporella",
                                   "stapedius", "glandularly",
                                   "pictorially", "weism",
                                   "disilane", "rainproof",
                                   "Caphtor", "scrubbed",
                                   "oinomancy", "pseudoxanthine",
                                   "nonlustrous", "redesertion",
                                   "Oryzorictinae", "gala",
                                   "Mycogone", "reappreciate",
                                   "cyanoguanidine", "seeingness",
                                   "breadwinner", "noreast",
                                   "furacious", "epauliere",
                                   "omniscribent", "Passiflorales",
                                   "uninductive", "inductivity",
                                   "Orbitolina", "Semecarpus",
                                   "migrainoid", "steprelationship",
                                   "phlogisticate", "mesymnion",
                                   "sloped", "edificator",
                                   "beneficent", "culm",
                                   "paleornithology", "unurban",
                                   "throbless", "amplexifoliate",
                                   "sesquiquintile", "sapience",
                                   "astucious", "dithery",
                                   "boor", "ambitus",
                                   "scotching", "uloid",
                                   "uncompromisingness", "hoove",
                                   "waird", "marshiness",
                                   "Jerusalem", "mericarp",
                                   "unevoked", "benzoperoxide",
                                   "outguess", "pyxie",
                                   "hymnic", "euphemize",
                                   "mendacity", "erythremia",
                                   "rosaniline", "unchatteled",
                                   "lienteria", "Bushongo",
                                   "dialoguer", "unrepealably",
                                   "rivethead", "antideflation",
                                   "vinegarish", "manganosiderite",
                                   "doubtingness", "ovopyriform",
                                   "Cephalodiscus", "Muscicapa",
                                   "Animalivora", "angina",
                                   "planispheric", "ipomoein",
                                   "cuproiodargyrite", "sandbox",
                                   "scrat", "Munnopsidae",
                                   "shola", "pentafid",
                                   "overstudiousness", "times",
                                   "nonprofession", "appetible",
                                   "valvulotomy", "goladar",
                                   "uniarticular", "oxyterpene",
                                   "unlapsing", "omega",
                                   "trophonema", "seminonflammable",
                                   "circumzenithal", "starer",
                                   "depthwise", "liberatress",
                                   "unleavened", "unrevolting",
                                   "groundneedle", "topline",
                                   "wandoo", "umangite",
                                   "ordinant", "unachievable",
                                   "oversand", "snare",
                                   "avengeful", "unexplicit",
                                   "mustafina", "sonable",
                                   "rehabilitative", "eulogization",
                                   "papery", "technopsychology",
                                   "impressor", "cresylite",
                                   "entame", "transudatory",
                                   "scotale", "pachydermatoid",
                                   "imaginary", "yeat",
                                   "slipped", "stewardship",
                                   "adatom", "cockstone",
                                   "skyshine", "heavenful",
                                   "comparability", "exprobratory",
                                   "dermorhynchous", "parquet",
                                   "cretaceous", "vesperal",
                                   "raphis", "undangered",
                                   "Glecoma", "engrain",
                                   "counteractively", "Zuludom",
                                   "orchiocatabasis", "Auriculariales",
                                   "warriorwise", "extraorganismal",
                                   "overbuilt", "alveolite",
                                   "tetchy", "terrificness",
                                   "widdle", "unpremonished",
                                   "rebilling", "sequestrum",
                                   "equiconvex", "heliocentricism",
                                   "catabaptist", "okonite",
                                   "propheticism", "helminthagogic",
                                   "calycular", "giantly",
                                   "wingable", "golem",
                                   "unprovided", "commandingness",
                                   "greave", "haply",
                                   "doina", "depressingly",
                                   "subdentate", "impairment",
                                   "decidable", "neurotrophic",
                                   "unpredict", "bicorporeal",
                                   "pendulant", "flatman",
                                   "intrabred", "toplike",
                                   "Prosobranchiata", "farrantly",
                                   "toxoplasmosis", "gorilloid",
                                   "dipsomaniacal", "aquiline",
                                   "atlantite", "ascitic",
                                   "perculsive", "prospectiveness",
                                   "saponaceous", "centrifugalization",
                                   "dinical", "infravaginal",
                                   "beadroll", "affaite",
                                   "Helvidian", "tickleproof",
                                   "abstractionism", "enhedge",
                                   "outwealth", "overcontribute",
                                   "coldfinch", "gymnastic",
                                   "Pincian", "Munychian",
                                   "codisjunct", "quad",
                                   "coracomandibular", "phoenicochroite",
                                   "amender", "selectivity",
                                   "putative", "semantician",
                                   "lophotrichic", "Spatangoidea",
                                   "saccharogenic", "inferent",
                                   "Triconodonta", "arrendation",
                                   "sheepskin", "taurocolla",
                                   "bunghole", "Machiavel",
                                   "triakistetrahedral", "dehairer",
                                   "prezygapophysial", "cylindric",
                                   "pneumonalgia", "sleigher",
                                   "emir", "Socraticism",
                                   "licitness", "massedly",
                                   "instructiveness", "sturdied",
                                   "redecrease", "starosta",
                                   "evictor", "orgiastic",
                                   "squdge", "meloplasty",
                                   "Tsonecan", "repealableness",
                                   "swoony", "myesthesia",
                                   "molecule", "autobiographist",
                                   "reciprocation", "refective",
                                   "unobservantness", "tricae",
                                   "ungouged", "floatability",
                                   "Mesua", "fetlocked",
                                   "chordacentrum", "sedentariness",
                                   "various", "laubanite",
                                   "nectopod", "zenick",
                                   "sequentially", "analgic",
                                   "biodynamics", "posttraumatic",
                                   "nummi", "pyroacetic",
                                   "bot", "redescend",
                                   "dispermy", "undiffusive",
                                   "circular", "trillion",
                                   "Uraniidae", "ploration",
                                   "discipular", "potentness",
                                   "sud", "Hu",
                                   "Eryon", "plugger",
                                   "subdrainage", "jharal",
                                   "abscission", "supermarket",
                                   "countergabion", "glacierist",
                                   "lithotresis", "minniebush",
                                   "zanyism", "eucalypteol",
                                   "sterilely", "unrealize",
                                   "unpatched", "hypochondriacism",
                                   "critically", "cheesecutter",
                                  };
}
