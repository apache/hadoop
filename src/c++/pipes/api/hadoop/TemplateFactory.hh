#ifndef HADOOP_PIPES_TEMPLATE_FACTORY_HH
#define HADOOP_PIPES_TEMPLATE_FACTORY_HH

namespace HadoopPipes {

  template <class mapper, class reducer>
  class TemplateFactory2: public Factory {
  public:
    Mapper* createMapper(MapContext& context) const {
      return new mapper(context);
    }
    Reducer* createReducer(ReduceContext& context) const {
      return new reducer(context);
    }
  };

  template <class mapper, class reducer, class partitioner>
  class TemplateFactory3: public TemplateFactory2<mapper,reducer> {
  public:
    Partitioner* createPartitioner(MapContext& context) const {
      return new partitioner(context);
    }
  };

  template <class mapper, class reducer>
  class TemplateFactory3<mapper, reducer, void>
      : public TemplateFactory2<mapper,reducer> {
  };

  template <class mapper, class reducer, class partitioner, class combiner>
  class TemplateFactory4
   : public TemplateFactory3<mapper,reducer,partitioner>{
  public:
    Reducer* createCombiner(MapContext& context) const {
      return new combiner(context);
    }
  };

  template <class mapper, class reducer, class partitioner>
  class TemplateFactory4<mapper,reducer,partitioner,void>
   : public TemplateFactory3<mapper,reducer,partitioner>{
  };

  template <class mapper, class reducer, class partitioner, 
            class combiner, class recordReader>
  class TemplateFactory5
   : public TemplateFactory4<mapper,reducer,partitioner,combiner>{
  public:
    RecordReader* createRecordReader(MapContext& context) const {
      return new recordReader(context);
    }
  };

  template <class mapper, class reducer, class partitioner,class combiner>
  class TemplateFactory5<mapper,reducer,partitioner,combiner,void>
   : public TemplateFactory4<mapper,reducer,partitioner,combiner>{
  };

  template <class mapper, class reducer, class partitioner=void, 
            class combiner=void, class recordReader=void, 
            class recordWriter=void> 
  class TemplateFactory
   : public TemplateFactory5<mapper,reducer,partitioner,combiner,recordReader>{
  public:
    RecordWriter* createRecordWriter(ReduceContext& context) const {
      return new recordWriter(context);
    }
  };

  template <class mapper, class reducer, class partitioner, 
            class combiner, class recordReader>
  class TemplateFactory<mapper, reducer, partitioner, combiner, recordReader, 
                        void>
   : public TemplateFactory5<mapper,reducer,partitioner,combiner,recordReader>{
  };

}

#endif
