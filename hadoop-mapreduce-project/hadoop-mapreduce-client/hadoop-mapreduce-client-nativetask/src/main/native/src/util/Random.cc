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

#include <math.h>
#include "lib/commons.h"
#include "util/Random.h"

namespace NativeTask {

static long RandomInitializeID = 8682522807148012ULL;

// A random list of 1000 words from /usr/share/dict/words
static const
char * Words[] = {"diurnalness", "Homoiousian", "spiranthic", "tetragynian", "silverhead",
    "ungreat", "lithograph", "exploiter", "physiologian", "by", "hellbender", "Filipendula",
    "undeterring", "antiscolic", "pentagamist", "hypoid", "cacuminal", "sertularian",
    "schoolmasterism", "nonuple", "gallybeggar", "phytonic", "swearingly", "nebular", "Confervales",
    "thermochemically", "characinoid", "cocksuredom", "fallacious", "feasibleness", "debromination",
    "playfellowship", "tramplike", "testa", "participatingly", "unaccessible", "bromate",
    "experientialist", "roughcast", "docimastical", "choralcelo", "blightbird", "peptonate",
    "sombreroed", "unschematized", "antiabolitionist", "besagne", "mastication", "bromic",
    "sviatonosite", "cattimandoo", "metaphrastical", "endotheliomyoma", "hysterolysis",
    "unfulminated", "Hester", "oblongly", "blurredness", "authorling", "chasmy", "Scorpaenidae",
    "toxihaemia", "Dictograph", "Quakerishly", "deaf", "timbermonger", "strammel", "Thraupidae",
    "seditious", "plerome", "Arneb", "eristically", "serpentinic", "glaumrie", "socioromantic",
    "apocalypst", "tartrous", "Bassaris", "angiolymphoma", "horsefly", "kenno", "astronomize",
    "euphemious", "arsenide", "untongued", "parabolicness", "uvanite", "helpless", "gemmeous",
    "stormy", "templar", "erythrodextrin", "comism", "interfraternal", "preparative", "parastas",
    "frontoorbital", "Ophiosaurus", "diopside", "serosanguineous", "ununiformly", "karyological",
    "collegian", "allotropic", "depravity", "amylogenesis", "reformatory", "epidymides",
    "pleurotropous", "trillium", "dastardliness", "coadvice", "embryotic", "benthonic",
    "pomiferous", "figureheadship", "Megaluridae", "Harpa", "frenal", "commotion", "abthainry",
    "cobeliever", "manilla", "spiciferous", "nativeness", "obispo", "monilioid", "biopsic",
    "valvula", "enterostomy", "planosubulate", "pterostigma", "lifter", "triradiated", "venialness",
    "tum", "archistome", "tautness", "unswanlike", "antivenin", "Lentibulariaceae", "Triphora",
    "angiopathy", "anta", "Dawsonia", "becomma", "Yannigan", "winterproof", "antalgol", "harr",
    "underogating", "ineunt", "cornberry", "flippantness", "scyphostoma", "approbation", "Ghent",
    "Macraucheniidae", "scabbiness", "unanatomized", "photoelasticity", "eurythermal", "enation",
    "prepavement", "flushgate", "subsequentially", "Edo", "antihero", "Isokontae", "unforkedness",
    "porriginous", "daytime", "nonexecutive", "trisilicic", "morphiomania", "paranephros",
    "botchedly", "impugnation", "Dodecatheon", "obolus", "unburnt", "provedore", "Aktistetae",
    "superindifference", "Alethea", "Joachimite", "cyanophilous", "chorograph", "brooky", "figured",
    "periclitation", "quintette", "hondo", "ornithodelphous", "unefficient", "pondside", "bogydom",
    "laurinoxylon", "Shiah", "unharmed", "cartful", "noncrystallized", "abusiveness", "cromlech",
    "japanned", "rizzomed", "underskin", "adscendent", "allectory", "gelatinousness", "volcano",
    "uncompromisingly", "cubit", "idiotize", "unfurbelowed", "undinted", "magnetooptics", "Savitar",
    "diwata", "ramosopalmate", "Pishquow", "tomorn", "apopenptic", "Haversian", "Hysterocarpus",
    "ten", "outhue", "Bertat", "mechanist", "asparaginic", "velaric", "tonsure", "bubble",
    "Pyrales", "regardful", "glyphography", "calabazilla", "shellworker", "stradametrical", "havoc",
    "theologicopolitical", "sawdust", "diatomaceous", "jajman", "temporomastoid", "Serrifera",
    "Ochnaceae", "aspersor", "trailmaking", "Bishareen", "digitule", "octogynous", "epididymitis",
    "smokefarthings", "bacillite", "overcrown", "mangonism", "sirrah", "undecorated", "psychofugal",
    "bismuthiferous", "rechar", "Lemuridae", "frameable", "thiodiazole", "Scanic",
    "sportswomanship", "interruptedness", "admissory", "osteopaedion", "tingly", "tomorrowness",
    "ethnocracy", "trabecular", "vitally", "fossilism", "adz", "metopon", "prefatorial",
    "expiscate", "diathermacy", "chronist", "nigh", "generalizable", "hysterogen",
    "aurothiosulphuric", "whitlowwort", "downthrust", "Protestantize", "monander", "Itea",
    "chronographic", "silicize", "Dunlop", "eer", "componental", "spot", "pamphlet", "antineuritic",
    "paradisean", "interruptor", "debellator", "overcultured", "Florissant", "hyocholic",
    "pneumatotherapy", "tailoress", "rave", "unpeople", "Sebastian", "thermanesthesia", "Coniferae",
    "swacking", "posterishness", "ethmopalatal", "whittle", "analgize", "scabbardless", "naught",
    "symbiogenetically", "trip", "parodist", "columniform", "trunnel", "yawler", "goodwill",
    "pseudohalogen", "swangy", "cervisial", "mediateness", "genii", "imprescribable", "pony",
    "consumptional", "carposporangial", "poleax", "bestill", "subfebrile", "sapphiric", "arrowworm",
    "qualminess", "ultraobscure", "thorite", "Fouquieria", "Bermudian", "prescriber", "elemicin",
    "warlike", "semiangle", "rotular", "misthread", "returnability", "seraphism", "precostal",
    "quarried", "Babylonism", "sangaree", "seelful", "placatory", "pachydermous", "bozal",
    "galbulus", "spermaphyte", "cumbrousness", "pope", "signifier", "Endomycetaceae", "shallowish",
    "sequacity", "periarthritis", "bathysphere", "pentosuria", "Dadaism", "spookdom",
    "Consolamentum", "afterpressure", "mutter", "louse", "ovoviviparous", "corbel", "metastoma",
    "biventer", "Hydrangea", "hogmace", "seizing", "nonsuppressed", "oratorize", "uncarefully",
    "benzothiofuran", "penult", "balanocele", "macropterous", "dishpan", "marten", "absvolt",
    "jirble", "parmelioid", "airfreighter", "acocotl", "archesporial", "hypoplastral", "preoral",
    "quailberry", "cinque", "terrestrially", "stroking", "limpet", "moodishness", "canicule",
    "archididascalian", "pompiloid", "overstaid", "introducer", "Italical", "Christianopaganism",
    "prescriptible", "subofficer", "danseuse", "cloy", "saguran", "frictionlessly",
    "deindividualization", "Bulanda", "ventricous", "subfoliar", "basto", "scapuloradial",
    "suspend", "stiffish", "Sphenodontidae", "eternal", "verbid", "mammonish", "upcushion",
    "barkometer", "concretion", "preagitate", "incomprehensible", "tristich", "visceral",
    "hemimelus", "patroller", "stentorophonic", "pinulus", "kerykeion", "brutism", "monstership",
    "merciful", "overinstruct", "defensibly", "bettermost", "splenauxe", "Mormyrus",
    "unreprimanded", "taver", "ell", "proacquittal", "infestation", "overwoven", "Lincolnlike",
    "chacona", "Tamil", "classificational", "lebensraum", "reeveland", "intuition", "Whilkut",
    "focaloid", "Eleusinian", "micromembrane", "byroad", "nonrepetition", "bacterioblast", "brag",
    "ribaldrous", "phytoma", "counteralliance", "pelvimetry", "pelf", "relaster", "thermoresistant",
    "aneurism", "molossic", "euphonym", "upswell", "ladhood", "phallaceous", "inertly", "gunshop",
    "stereotypography", "laryngic", "refasten", "twinling", "oflete", "hepatorrhaphy",
    "electrotechnics", "cockal", "guitarist", "topsail", "Cimmerianism", "larklike", "Llandovery",
    "pyrocatechol", "immatchable", "chooser", "metrocratic", "craglike", "quadrennial",
    "nonpoisonous", "undercolored", "knob", "ultratense", "balladmonger", "slait", "sialadenitis",
    "bucketer", "magnificently", "unstipulated", "unscourged", "unsupercilious", "packsack",
    "pansophism", "soorkee", "percent", "subirrigate", "champer", "metapolitics", "spherulitic",
    "involatile", "metaphonical", "stachyuraceous", "speckedness", "bespin", "proboscidiform",
    "gul", "squit", "yeelaman", "peristeropode", "opacousness", "shibuichi", "retinize", "yote",
    "misexposition", "devilwise", "pumpkinification", "vinny", "bonze", "glossing", "decardinalize",
    "transcortical", "serphoid", "deepmost", "guanajuatite", "wemless", "arval", "lammy", "Effie",
    "Saponaria", "tetrahedral", "prolificy", "excerpt", "dunkadoo", "Spencerism", "insatiately",
    "Gilaki", "oratorship", "arduousness", "unbashfulness", "Pithecolobium", "unisexuality",
    "veterinarian", "detractive", "liquidity", "acidophile", "proauction", "sural", "totaquina",
    "Vichyite", "uninhabitedness", "allegedly", "Gothish", "manny", "Inger", "flutist", "ticktick",
    "Ludgatian", "homotransplant", "orthopedical", "diminutively", "monogoneutic", "Kenipsim",
    "sarcologist", "drome", "stronghearted", "Fameuse", "Swaziland", "alen", "chilblain",
    "beatable", "agglomeratic", "constitutor", "tendomucoid", "porencephalous", "arteriasis",
    "boser", "tantivy", "rede", "lineamental", "uncontradictableness", "homeotypical", "masa",
    "folious", "dosseret", "neurodegenerative", "subtransverse", "Chiasmodontidae",
    "palaeotheriodont", "unstressedly", "chalcites", "piquantness", "lampyrine", "Aplacentalia",
    "projecting", "elastivity", "isopelletierin", "bladderwort", "strander", "almud",
    "iniquitously", "theologal", "bugre", "chargeably", "imperceptivity", "meriquinoidal",
    "mesophyte", "divinator", "perfunctory", "counterappellant", "synovial", "charioteer",
    "crystallographical", "comprovincial", "infrastapedial", "pleasurehood", "inventurous",
    "ultrasystematic", "subangulated", "supraoesophageal", "Vaishnavism", "transude",
    "chrysochrous", "ungrave", "reconciliable", "uninterpleaded", "erlking", "wherefrom",
    "aprosopia", "antiadiaphorist", "metoxazine", "incalculable", "umbellic", "predebit",
    "foursquare", "unimmortal", "nonmanufacture", "slangy", "predisputant", "familist",
    "preaffiliate", "friarhood", "corelysis", "zoonitic", "halloo", "paunchy", "neuromimesis",
    "aconitine", "hackneyed", "unfeeble", "cubby", "autoschediastical", "naprapath", "lyrebird",
    "inexistency", "leucophoenicite", "ferrogoslarite", "reperuse", "uncombable", "tambo",
    "propodiale", "diplomatize", "Russifier", "clanned", "corona", "michigan", "nonutilitarian",
    "transcorporeal", "bought", "Cercosporella", "stapedius", "glandularly", "pictorially", "weism",
    "disilane", "rainproof", "Caphtor", "scrubbed", "oinomancy", "pseudoxanthine", "nonlustrous",
    "redesertion", "Oryzorictinae", "gala", "Mycogone", "reappreciate", "cyanoguanidine",
    "seeingness", "breadwinner", "noreast", "furacious", "epauliere", "omniscribent",
    "Passiflorales", "uninductive", "inductivity", "Orbitolina", "Semecarpus", "migrainoid",
    "steprelationship", "phlogisticate", "mesymnion", "sloped", "edificator", "beneficent", "culm",
    "paleornithology", "unurban", "throbless", "amplexifoliate", "sesquiquintile", "sapience",
    "astucious", "dithery", "boor", "ambitus", "scotching", "uloid", "uncompromisingness", "hoove",
    "waird", "marshiness", "Jerusalem", "mericarp", "unevoked", "benzoperoxide", "outguess",
    "pyxie", "hymnic", "euphemize", "mendacity", "erythremia", "rosaniline", "unchatteled",
    "lienteria", "Bushongo", "dialoguer", "unrepealably", "rivethead", "antideflation",
    "vinegarish", "manganosiderite", "doubtingness", "ovopyriform", "Cephalodiscus", "Muscicapa",
    "Animalivora", "angina", "planispheric", "ipomoein", "cuproiodargyrite", "sandbox", "scrat",
    "Munnopsidae", "shola", "pentafid", "overstudiousness", "times", "nonprofession", "appetible",
    "valvulotomy", "goladar", "uniarticular", "oxyterpene", "unlapsing", "omega", "trophonema",
    "seminonflammable", "circumzenithal", "starer", "depthwise", "liberatress", "unleavened",
    "unrevolting", "groundneedle", "topline", "wandoo", "umangite", "ordinant", "unachievable",
    "oversand", "snare", "avengeful", "unexplicit", "mustafina", "sonable", "rehabilitative",
    "eulogization", "papery", "technopsychology", "impressor", "cresylite", "entame",
    "transudatory", "scotale", "pachydermatoid", "imaginary", "yeat", "slipped", "stewardship",
    "adatom", "cockstone", "skyshine", "heavenful", "comparability", "exprobratory",
    "dermorhynchous", "parquet", "cretaceous", "vesperal", "raphis", "undangered", "Glecoma",
    "engrain", "counteractively", "Zuludom", "orchiocatabasis", "Auriculariales", "warriorwise",
    "extraorganismal", "overbuilt", "alveolite", "tetchy", "terrificness", "widdle",
    "unpremonished", "rebilling", "sequestrum", "equiconvex", "heliocentricism", "catabaptist",
    "okonite", "propheticism", "helminthagogic", "calycular", "giantly", "wingable", "golem",
    "unprovided", "commandingness", "greave", "haply", "doina", "depressingly", "subdentate",
    "impairment", "decidable", "neurotrophic", "unpredict", "bicorporeal", "pendulant", "flatman",
    "intrabred", "toplike", "Prosobranchiata", "farrantly", "toxoplasmosis", "gorilloid",
    "dipsomaniacal", "aquiline", "atlantite", "ascitic", "perculsive", "prospectiveness",
    "saponaceous", "centrifugalization", "dinical", "infravaginal", "beadroll", "affaite",
    "Helvidian", "tickleproof", "abstractionism", "enhedge", "outwealth", "overcontribute",
    "coldfinch", "gymnastic", "Pincian", "Munychian", "codisjunct", "quad", "coracomandibular",
    "phoenicochroite", "amender", "selectivity", "putative", "semantician", "lophotrichic",
    "Spatangoidea", "saccharogenic", "inferent", "Triconodonta", "arrendation", "sheepskin",
    "taurocolla", "bunghole", "Machiavel", "triakistetrahedral", "dehairer", "prezygapophysial",
    "cylindric", "pneumonalgia", "sleigher", "emir", "Socraticism", "licitness", "massedly",
    "instructiveness", "sturdied", "redecrease", "starosta", "evictor", "orgiastic", "squdge",
    "meloplasty", "Tsonecan", "repealableness", "swoony", "myesthesia", "molecule",
    "autobiographist", "reciprocation", "refective", "unobservantness", "tricae", "ungouged",
    "floatability", "Mesua", "fetlocked", "chordacentrum", "sedentariness", "various", "laubanite",
    "nectopod", "zenick", "sequentially", "analgic", "biodynamics", "posttraumatic", "nummi",
    "pyroacetic", "bot", "redescend", "dispermy", "undiffusive", "circular", "trillion",
    "Uraniidae", "ploration", "discipular", "potentness", "sud", "Hu", "Eryon", "plugger",
    "subdrainage", "jharal", "abscission", "supermarket", "countergabion", "glacierist",
    "lithotresis", "minniebush", "zanyism", "eucalypteol", "sterilely", "unrealize", "unpatched",
    "hypochondriacism", "critically", "cheesecutter", };

static uint32_t WordsCount = sizeof(Words) / sizeof(char *);

Random::Random() {
  setSeed(time(NULL) + clock() + RandomInitializeID++);
}

Random::Random(int64_t seed) {
  if (seed == -1) {
    setSeed(time(NULL) + clock() + RandomInitializeID++);
  } else {
    setSeed(seed);
  }
}

Random::~Random() {

}

void Random::setSeed(int64_t seed) {
  _seed = (seed ^ multiplier) & mask;
}

int32_t Random::next(int bits) {
  _seed = (_seed * multiplier + addend) & mask;
  return (int32_t)(_seed >> (48 - bits));
}

int32_t Random::next_int32() {
  return next(32);
}

uint32_t Random::next_uint32() {
  return (uint32_t)next(32);
}

uint64_t Random::next_uint64() {
  return ((uint64_t)(next(32)) << 32) + next(32);
}

int32_t Random::next_int32(int32_t n) {
  if ((n & -n) == n)  // i.e., n is a power of 2
    return (int32_t)((n * (int64_t)next(31)) >> 31);

  int32_t bits, val;
  do {
    bits = next(31);
    val = bits % n;
  } while (bits - val + (n - 1) < 0);
  return val;
}

float Random::nextFloat() {
  return next(24) / ((float)(1 << 24));
}

double Random::nextDouble() {
  return (((uint64_t)(next(26)) << 27) + next(27)) / (double)(1L << 53);
}

uint64_t Random::nextLog2() {
  return (uint64_t)exp2(nextDouble() * 64);
}

uint64_t Random::nextLog2(uint64_t range) {
  double range_r = log2(range);
  double v = nextDouble() * range_r;
  return (uint64_t)exp2(v);
}

uint64_t Random::nextLog10(uint64_t range) {
  double range_r = log10((double)range);
  double v = nextDouble() * range_r;
  return (uint64_t)pow(10, v);
}

char Random::nextByte(const string & range) {
  if (range.length() == 0) {
    return (char)next(8);
  } else {
    return range[next_int32(range.length())];
  }
}

string Random::nextBytes(uint32_t length, const string & range) {
  string ret(length, '-');
  for (uint32_t i = 0; i < length; i++) {
    ret[i] = nextByte(range);
  }
  return ret;
}

const char * Random::nextWord(int64_t limit) {
  if (limit < 0) {
    return Words[next_int32(WordsCount)];
  }
  uint32_t r = limit < WordsCount ? limit : WordsCount;
  return Words[next_int32(r)];
}

void Random::nextWord(string & dest, int64_t limit) {
  dest = nextWord(limit);
}

} // namespace NativeTask
