import Converter from 'yarn-ui/utils/converter';

export default {
  _initElapsedTimeSorter: function() {
    jQuery.extend(jQuery.fn.dataTableExt.oSort, {
      "elapsed-time-pre": function (a) {
         return Converter.padding(Converter.elapsedTimeToMs(a), 20);
      },
    });
  },

  initDataTableSorter: function() {
    this._initElapsedTimeSorter();
  },
}
