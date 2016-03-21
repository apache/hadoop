import Ember from 'ember';

export default Ember.Component.extend({
  didInsertElement: function() {
    var paging = this.get("paging") ? true : this.get("paging");
    var ordering = this.get("ordering") ? true : this.get("ordering");
    var info = this.get("info") ? true : this.get("info");
    var bFilter = this.get("bFilter") ? true : this.get("bFilter");

    // Defines sorter for the columns if not default.
    // Can also specify a custom sorter.
    var i;
    var colDefs = [];
    if (this.get("colTypes")) {
      var typesArr = this.get("colTypes").split(' ');
      var targetsArr = this.get("colTargets").split(' ');
      for (i = 0; i < typesArr.length; i++) {
        console.log(typesArr[i] + " " + targetsArr[i]);
        colDefs.push({
          type: typesArr[i],
          targets: parseInt(targetsArr[i])
        });
      }
    }
    // Defines initial column and sort order.
    var orderArr = [];
    if (this.get("colsOrder")) {
      var cols = this.get("colsOrder").split(' ');
      for (i = 0; i < cols.length; i++) {
        var col = cols[i].split(',');
        if (col.length != 2) {
          continue;
        }
        var order = col[1].trim();
        if (order != 'asc' && order != 'desc') {
          continue;
        }
        var colOrder = [];
        colOrder.push(parseInt(col[0]));
        colOrder.push(order);
        orderArr.push(colOrder);
      }
    }
    if (orderArr.length == 0) {
      var defaultOrder = [0, 'asc'];
      orderArr.push(defaultOrder);
    }
    console.log(orderArr[0]);
    Ember.$('#' + this.get('table-id')).DataTable({
      "paging":   paging,
      "ordering": ordering, 
      "info":     info,
      "bFilter": bFilter,
      "order": orderArr,
      "columnDefs": colDefs
    });
  }
});
