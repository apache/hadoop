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
(function() {
  "use strict";

  // The chunk size of tailing the files, i.e., how many bytes will be shown
  // in the preview.
  var TAIL_CHUNK_SIZE = 32768;

  //This stores the current directory which is being browsed
  var current_directory = "";

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  $(window).bind('hashchange', function () {
    $('#alert-panel').hide();

    var dir = window.location.hash.slice(1);
    if(dir == "") {
      dir = "/";
    }
    if(current_directory != dir) {
      browse_directory(dir);
    }
  });

  function network_error_handler(url) {
    return function (jqxhr, text, err) {
      switch(jqxhr.status) {
        case 401:
          var msg = '<p>Authentication failed when trying to open ' + url + ': Unauthorized.</p>';
          break;
        case 403:
          if(jqxhr.responseJSON !== undefined && jqxhr.responseJSON.RemoteException !== undefined) {
            var msg = '<p>' + jqxhr.responseJSON.RemoteException.message + "</p>";
            break;
          }
          var msg = '<p>Permission denied when trying to open ' + url + ': ' + err + '</p>';
          break;
        case 404:
          var msg = '<p>Path does not exist on HDFS or WebHDFS is disabled.  Please check your path or enable WebHDFS</p>';
          break;
        default:
          var msg = '<p>Failed to retrieve data from ' + url + ': ' + err + '</p>';
        }
      show_err_msg(msg);
    };
  }

  function append_path(prefix, s) {
    var l = prefix.length;
    var p = l > 0 && prefix[l - 1] == '/' ? prefix.substring(0, l - 1) : prefix;
    return p + '/' + s;
  }

  function get_response(data, type) {
    return data[type] !== undefined ? data[type] : null;
  }

  function get_response_err_msg(data) {
    return data.RemoteException !== undefined ? data.RemoteException.message : "";
  }

  function delete_path(inode_name, absolute_file_path) {
    $('#delete-modal-title').text("Delete - " + inode_name);
    $('#delete-prompt').text("Are you sure you want to delete " + inode_name
      + " ?");

    $('#delete-button').click(function() {
      // DELETE /webhdfs/v1/<path>?op=DELETE&recursive=<true|false>
      var url = '/webhdfs/v1' + encode_path(absolute_file_path) +
        '?op=DELETE' + '&recursive=true';

      $.ajax(url,
        { type: 'DELETE'
        }).done(function(data) {
          browse_directory(current_directory);
        }).error(network_error_handler(url)
         ).complete(function() {
           $('#delete-modal').modal('hide');
           $('#delete-button').button('reset');
        });
    })
    $('#delete-modal').modal();
  }

  /* This method loads the checkboxes on the permission info modal. It accepts
   * the octal permissions, eg. '644' or '755' and infers the checkboxes that
   * should be true and false
   */
  function view_perm_details(e, filename, abs_path, perms) {
    $('.explorer-perm-links').popover('destroy');
    e.popover({html: true, content: $('#explorer-popover-perm-info').html(), trigger: 'focus'})
      .on('shown.bs.popover', function(e) {
        var popover = $(this), parent = popover.parent();
        //Convert octal to binary permissions
        var bin_perms = parseInt(perms, 8).toString(2);
        bin_perms = bin_perms.length == 9 ? "0" + bin_perms : bin_perms;
        parent.find('#explorer-perm-cancel').on('click', function() { popover.popover('destroy'); });
        parent.find('#explorer-set-perm-button').off().click(function() { set_permissions(abs_path); });
        parent.find('input[type=checkbox]').each(function(idx, element) {
          var e = $(element);
          e.prop('checked', bin_perms.charAt(9 - e.attr('data-bit')) == '1');
        });
      })
      .popover('show');
  }

  // Use WebHDFS to set permissions on an absolute path
  function set_permissions(abs_path) {
    var p = 0;
    $.each($('.popover .explorer-popover-perm-body input:checked'), function(idx, e) {
      p |= 1 << (+$(e).attr('data-bit'));
    });

    var permission_mask = p.toString(8);

    // PUT /webhdfs/v1/<path>?op=SETPERMISSION&permission=<permission>
    var url = '/webhdfs/v1' + encode_path(abs_path) +
      '?op=SETPERMISSION' + '&permission=' + permission_mask;

    $.ajax(url, { type: 'PUT'
      }).done(function(data) {
        browse_directory(current_directory);
      }).error(network_error_handler(url))
      .complete(function() {
        $('.explorer-perm-links').popover('destroy');
      });
  }

  function encode_path(abs_path) {
    abs_path = encodeURIComponent(abs_path);
    var re = /%2F/g;
    return abs_path.replace(re, '/');
  }

  function view_file_details(path, abs_path) {
    function show_block_info(blocks) {
      var menus = $('#file-info-blockinfo-list');
      menus.empty();

      menus.data("blocks", blocks);
      menus.change(function() {
        var d = $(this).data('blocks')[$(this).val()];
        if (d === undefined) {
          return;
        }

        dust.render('block-info', d, function(err, out) {
          $('#file-info-blockinfo-body').html(out);
        });

      });
      for (var i = 0; i < blocks.length; ++i) {
        var item = $('<option value="' + i + '">Block ' + i + '</option>');
        menus.append(item);
      }
      menus.change();
    }

    abs_path = encode_path(abs_path);
    var url = '/webhdfs/v1' + abs_path + '?op=GET_BLOCK_LOCATIONS';
    $.ajax({url: url, dataType: 'text'}).done(function(data_text) {
      var data = JSONParseBigNum(data_text);
      var d = get_response(data, "LocatedBlocks");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      $('#file-info-tail').hide();
      $('#file-info-title').text("File information - " + path);

      var download_url = '/webhdfs/v1' + abs_path + '?op=OPEN';

      $('#file-info-download').attr('href', download_url);
      $('#file-info-preview').click(function() {
        var offset = d.fileLength - TAIL_CHUNK_SIZE;
        var url = offset > 0 ? download_url + '&offset=' + offset : download_url;
        $.get(url, function(t) {
          $('#file-info-preview-body').val(t);
          $('#file-info-tail').show();
        }, "text").error(network_error_handler(url));
      });

      if (d.fileLength > 0) {
        show_block_info(d.locatedBlocks);
        $('#file-info-blockinfo-panel').show();
      } else {
        $('#file-info-blockinfo-panel').hide();
      }
      $('#file-info').modal();
    }).error(network_error_handler(url));
  }

  /**Use X-editable to make fields editable with a nice UI.
   * elementType is the class of element(s) you want to make editable
   * op is the WebHDFS operation that will be triggered
   * parameter is (currently the 1) parameter which will be passed along with
   *   the value entered by the user
   */
  function makeEditable(elementType, op, parameter) {
    $(elementType).each(function(index, value) {
      $(this).editable({
        url: function(params) {
          var inode_name = $(this).closest('tr').attr('inode-path');
          var absolute_file_path = append_path(current_directory, inode_name);
          var url = '/webhdfs/v1' + encode_path(absolute_file_path) + '?op=' +
            op + '&' + parameter + '=' + encodeURIComponent(params.value);

          return $.ajax(url, { type: 'PUT', })
            .error(network_error_handler(url))
            .success(function() {
                browse_directory(current_directory);
             });
        },
        error: function(response, newValue) {return "";}
      });
    });
  }

  function func_size_render(data, type, row, meta) {
    if(type == 'display') {
      return dust.filters.fmt_bytes(data);
    }
    else return data;
  }

  // Change the format of date-time depending on how old the
  // the timestamp is. If older than 6 months, no need to be
  // show exact time.
  function func_time_render(data, type, row, meta) {
    if(type == 'display') {
      var cutoff = moment().subtract(6, 'months').unix() * 1000;
      if(data < cutoff) {
        return moment(Number(data)).format('MMM DD YYYY');
      } else {
        return moment(Number(data)).format('MMM DD HH:mm');
      }
    }
    return data;
  }

  function browse_directory(dir) {
    var HELPERS = {
      'helper_date_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + moment(Number(value)).format('ddd MMM DD HH:mm:ss ZZ YYYY'));
      }
    };
    var url = '/webhdfs/v1' + encode_path(dir) + '?op=LISTSTATUS';
    $.get(url, function(data) {
      var d = get_response(data, "FileStatuses");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      current_directory = dir;
      $('#directory').val(dir);
      window.location.hash = dir;
      var base = dust.makeBase(HELPERS);
      dust.render('explorer', base.push(d), function(err, out) {
        $('#panel').html(out);


        $('.explorer-browse-links').click(function() {
          var type = $(this).attr('inode-type');
          var path = $(this).closest('tr').attr('inode-path');
          var abs_path = append_path(current_directory, path);
          if (type == 'DIRECTORY') {
            browse_directory(abs_path);
          } else {
            view_file_details(path, abs_path);
          }
        });

        //Set the handler for changing permissions
        $('.explorer-perm-links').click(function() {
          var filename = $(this).closest('tr').attr('inode-path');
          var abs_path = append_path(current_directory, filename);
          var perms = $(this).closest('tr').attr('data-permission');
          view_perm_details($(this), filename, abs_path, perms);
        });

        makeEditable('.explorer-owner-links', 'SETOWNER', 'owner');
        makeEditable('.explorer-group-links', 'SETOWNER', 'group');
        makeEditable('.explorer-replication-links', 'SETREPLICATION', 'replication');

        $('.explorer-entry .glyphicon-trash').click(function() {
          var inode_name = $(this).closest('tr').attr('inode-path');
          var absolute_file_path = append_path(current_directory, inode_name);
          delete_path(inode_name, absolute_file_path);
        });
          
          $('#table-explorer').dataTable( {
              'lengthMenu': [ [25, 50, 100, -1], [25, 50, 100, "All"] ],
              'columns': [
                  {'searchable': false }, //Permissions
                  null, //Owner
                  null, //Group
                  { 'searchable': false, 'render': func_size_render}, //Size
                  { 'searchable': false, 'render': func_time_render}, //Last Modified
                  { 'searchable': false }, //Replication
                  null, //Block Size
                  null, //Name
                  { 'sortable' : false } //Trash
              ],
              "deferRender": true
          });
      });
    }).error(network_error_handler(url));
  }


  function init() {
    dust.loadSource(dust.compile($('#tmpl-explorer').html(), 'explorer'));
    dust.loadSource(dust.compile($('#tmpl-block-info').html(), 'block-info'));

    var b = function() { browse_directory($('#directory').val()); };
    $('#btn-nav-directory').click(b);
    //Also navigate to the directory when a user presses enter.
    $('#directory').on('keyup', function (e) {
      if (e.which == 13) {
        browse_directory($('#directory').val());
      }
    });
    var dir = window.location.hash.slice(1);
    if(dir == "") {
      window.location.hash = "/";
    } else {
      browse_directory(dir);
    }
  }

  $('#btn-create-directory').on('show.bs.modal', function(event) {
    $('#new_directory_pwd').text(current_directory);
  });

  $('#btn-create-directory-send').click(function () {
    $(this).prop('disabled', true);
    $(this).button('complete');

    var url = '/webhdfs/v1' + encode_path(append_path(current_directory,
      $('#new_directory').val())) + '?op=MKDIRS';

    $.ajax(url, { type: 'PUT' }
    ).done(function(data) {
      browse_directory(current_directory);
    }).error(network_error_handler(url)
     ).complete(function() {
       $('#btn-create-directory').modal('hide');
       $('#btn-create-directory-send').button('reset');
    });
  })

  init();
})();
