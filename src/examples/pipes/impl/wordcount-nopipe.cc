#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include "hadoop/SerialUtils.hh"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

class WordCountMap: public HadoopPipes::Mapper {
public:
  WordCountMap(HadoopPipes::MapContext& context){}
  void map(HadoopPipes::MapContext& context) {
    std::vector<std::string> words = 
      HadoopUtils::splitString(context.getInputValue(), " ");
    for(unsigned int i=0; i < words.size(); ++i) {
      context.emit(words[i], "1");
    }
  }
};

class WordCountReduce: public HadoopPipes::Reducer {
public:
  WordCountReduce(HadoopPipes::ReduceContext& context){}
  void reduce(HadoopPipes::ReduceContext& context) {
    int sum = 0;
    while (context.nextValue()) {
      sum += HadoopUtils::toInt(context.getInputValue());
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(sum));
  }
};

class WordCountReader: public HadoopPipes::RecordReader {
private:
  int64_t bytesTotal;
  int64_t bytesRead;
  FILE* file;
public:
  WordCountReader(HadoopPipes::MapContext& context) {
    std::string filename;
    HadoopUtils::StringInStream stream(context.getInputSplit());
    HadoopUtils::deserializeString(filename, stream);
    struct stat statResult;
    stat(filename.c_str(), &statResult);
    bytesTotal = statResult.st_size;
    bytesRead = 0;
    file = fopen(filename.c_str(), "rt");
    HADOOP_ASSERT(file != NULL, "failed to open " + filename);
  }

  ~WordCountReader() {
    fclose(file);
  }

  virtual bool next(std::string& key, std::string& value) {
    key = HadoopUtils::toString(ftell(file));
    int ch = getc(file);
    bytesRead += 1;
    value.clear();
    while (ch != -1 && ch != '\n') {
      value += ch;
      ch = getc(file);
      bytesRead += 1;
    }
    return ch != -1;
  }

  /**
   * The progress of the record reader through the split as a value between
   * 0.0 and 1.0.
   */
  virtual float getProgress() {
    if (bytesTotal > 0) {
      return bytesRead / bytesTotal;
    } else {
      return 1.0f;
    }
  }
};

class WordCountWriter: public HadoopPipes::RecordWriter {
private:
  FILE* file;
public:
  WordCountWriter(HadoopPipes::ReduceContext& context) {
    const HadoopPipes::JobConf* job = context.getJobConf();
    int part = job->getInt("mapred.task.partition");
    std::string outDir = job->get("mapred.output.dir");
    mkdir(outDir.c_str(), 0777);
    std::string outFile = outDir + "/part-" + HadoopUtils::toString(part);
    file = fopen(outFile.c_str(), "wt");
  }

  ~WordCountWriter() {
    fclose(file);
  }

  void emit(const std::string& key, const std::string& value) {
    fprintf(file, "%s -> %s\n", key.c_str(), value.c_str());
  }
};

int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<WordCountMap, 
                              WordCountReduce, void, void, WordCountReader,
                              WordCountWriter>());
}

