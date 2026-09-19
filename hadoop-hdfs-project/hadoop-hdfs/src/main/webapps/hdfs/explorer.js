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

  var home_directory = "";

  var configuration = null;

  var trash_root = "";

  function show_err_msg(msg) {
    $('#info-panel').hide();
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }
  function show_info_msg(msg) {
    $('#alert-panel').hide();
    $('#info-panel-body').html(msg);
    $('#info-panel').show();
  }
  $(window).bind('popstate', function () {
    reset_message_panel();

    var dir = decodeURIComponent(window.location.hash.slice(1));
    if(dir == "") {
      dir = get_path_from_query();
    }
    if(dir == "") {
      dir = get_homedirectory();
    }
    if(current_directory != dir) {
      browse_directory(dir);
    } else {
      if(window.location.hash != dir) {
        window.location.hash = dir;
      }
    }
    set_path_to_query(dir);
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

    if(s.length > 0 && s[0] == '/'){
      return p + s;
    }
    return p + '/' + s;
  }

  function get_response(data, type) {
    return data[type] !== undefined ? data[type] : null;
  }

  function get_response_err_msg(data) {
    return data.RemoteException !== undefined ? data.RemoteException.message : "";
  }

  function delete_path(inode_name, absolute_file_path) {
    $('#delete-modal-title').text("Trash - " + inode_name);
    $('#delete-prompt').text("Are you sure you want to move " + inode_name
      + " to trash?");

    $('#delete-button').off("click").on("click", function() {
      get_trashroot();
      if(trash_root == "") {
        $('#delete-modal').modal('hide');
        $('#delete-button').button('reset');
        return;
      }

      const trash_current = append_path(trash_root, "Current");
      // abs_file_path: /user/foo/.Trash/file
      // trash_root: /user/foo/.Trash
      if (absolute_file_path == trash_root || absolute_file_path.startsWith(trash_root + "/")){
        show_err_msg("Cannot move " + absolute_file_path + " to the trash, it is already in trash");
        $('#delete-modal').modal('hide');
        $('#delete-button').button('reset');
        return;
      }

      const trash_root_parent = parent_dir(trash_root);
      // abs_file_path: /user/foo
      // trash_root_parent: /user/foo
      if (trash_root_parent == absolute_file_path || trash_root_parent.startsWith(absolute_file_path + "/")){
        show_err_msg("Cannot move " + absolute_file_path + " to the trash, as it contains the trash");
        $('#delete-modal').modal('hide');
        $('#delete-button').button('reset');
        return;
      }

      let trash_path = append_path(trash_current, absolute_file_path);
      const base_trash_path = append_path(trash_current, parent_dir(absolute_file_path));

      if(!mkdirs(base_trash_path)){
        $('#delete-modal').modal('hide');
        $('#delete-button').button('reset');
        return;
      }

      trash_path = get_unique_trash_path(trash_path);
      const url = "/webhdfs/v1" + encode_path(absolute_file_path) + "?op=RENAME&renameoptions=TO_TRASH&destination=" + encode_path(trash_path);
      $.ajax({
        type: "PUT",
        url:  url
      }).done(function(data) {
        browse_directory(current_directory);
        show_info_msg("Moved " + absolute_file_path + " to trash at: " + trash_path);
      }).fail(network_error_handler(url))
      .always(function() {
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
   setTimeout(function() {
    e.popover({html: true,sanitize: false, content: $('#explorer-popover-perm-info').html(), trigger: 'focus'})
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
      }, 100);
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
      }).fail(network_error_handler(url))
      .always(function() {
        $('.explorer-perm-links').popover('destroy');
      });
  }

  function set_path_to_query(dir) {
    if ('URLSearchParams' in window) {
      const url = new URL(window.location)
      url.searchParams.set("p", dir)
      history.replaceState(null, '', url);
    }
  }
  function get_path_from_query() {
    const urlParams = new URLSearchParams(window.location.search);
    const v = urlParams.get("p");
    if(v === null){
      return "";
    } else {
      return v;
    }
  }

  function get_homedirectory() {
    if(home_directory != "") {
      return home_directory;
    }

    var result = "/";
    var url = '/webhdfs/v1?op=GETHOMEDIRECTORY';

    $.ajax({
      url: url,
      type: "get",
      async: false,
      success: function (data) {
        var d = get_response(data, "Path");
        if (d != null) {
          result = d;
        }
      }
    });

    home_directory = result;
    return result;
  }

  function millis_to_str(milliseconds ) {
    let temp = milliseconds / 1000;
    const years = Math.floor( temp / 31536000 ),
        days = Math.floor( ( temp %= 31536000 ) / 86400 ),
        hours = Math.floor( ( temp %= 86400 ) / 3600 ),
        minutes = Math.floor( ( temp %= 3600 ) / 60 ),
        seconds = temp % 60;

    if ( days || hours || seconds || minutes ) {
      return ( years ? years + "y " : "" ) +
          ( days ? days + "d " : "" ) +
          ( hours ? hours + "h " : ""  ) +
          ( minutes ? minutes + "m " : "" ) +
          Number.parseFloat( seconds ).toFixed( 2 ) + "s";
    }

    return "< 1s";
  }

  function get_configuration() {
    if(configuration != null){
      return configuration;
    }

    const conf = new Map();
    $.ajax({'url': '/conf', 'dataType': 'json', 'async': false}).done(
      function (data) {
        const props = data['properties'];
        if(props === undefined){
          return;
        }

        for(let i = 0; i<props.length; i++){
          const key = props[i]['key'];
          const value = props[i]['value'];
          conf.set(key, value);
        }
      });

    configuration = conf;
    return configuration;
  }

  function get_conf(key, defaultValue){
    const conf = get_configuration();
    const value = conf.get(key);
    if(value === undefined){
      return defaultValue;
    }
    return value;
  }

  function get_current_namespace(){
    return get_conf("dfs.nameservice.id", "");
  }

  function get_namespaces(){
    return get_conf("dfs.nameservices", "").split(",");
  }

  function get_permission() {
    let umask, oldUmask, actualUmask;
    umask = Number(get_conf("fs.permissions.umask-mode", "-1"));
    oldUmask = Number(get_conf("dfs.umask", "-1"));

    if (oldUmask != -1) {
      actualUmask = 777 - oldUmask;
    } else if (umask != -1) {
      actualUmask = 777 - umask;
    } else {
      actualUmask = -1;
    }
    return actualUmask;
  }

  function get_trashroot() {
    if(trash_root != "") {
      return trash_root;
    }
    let result = "";
    const url = '/webhdfs/v1?op=GETTRASHROOT';

    $.ajax(url, {type: "GET", async: false}
    ).done(function (data) {
        var d = get_response(data, "Path");
        if (d != null) {
          result = d;
        }
    }).fail(network_error_handler(url));

    trash_root = result;
    return trash_root;
  }

  function get_unique_trash_path(trash_path) {
    const origin_trash_path = trash_path
    for (let i = 0; i < 3; i++) {
      let ok = false;
      $.ajax({
        type: 'GET',
        url: "/webhdfs/v1" + encode_path(trash_path) + "?op=GETFILESTATUS",
        async: false
      }).done(function(data) {
        // file exists. append timestamp for unique filename
        trash_path = origin_trash_path + Date.now();
      }).fail(function(jqXHR, textStatus, errorThrown) {
        ok = true;
      });
      if(ok){
        break;
      }
    }
    return trash_path;
  }

  function mkdirs(dir) {
    let success = false;
    let url = '/webhdfs/v1' + encode_path(dir) + '?op=MKDIRS';
    let permission = get_permission();
    if(permission != -1){
      url += "&permission=" + permission;
    }
    $.ajax(url, { type: 'PUT', async: false }
    ).done(function(data) {
      success = true;
    }).fail(network_error_handler(url));
    return success;
  }

  function parent_dir(abs_path){
    return abs_path.replace(/\/+[^/]+\/*$/,"") || "/";
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

      var processPreview = function(url) {
        url += "&noredirect=true";
        if(request && request.readyState != 4){
         request.abort();
        }
      request =  $.ajax({
           cache: false,
          type: 'GET',
          url: url,
          async: false,
          processData: false,
          crossDomain: true
        }).done(function(data, textStatus, jqXHR) {

          url = data.Location;
          $.ajax({
            cache: false,
            type: 'GET',
            url: url,
            async: false,
            processData: false,
            crossDomain: true
          }).always(function(data, textStatus, jqXHR) {
            $('#file-info-preview-body').val(jqXHR.responseText);
            $('#file-info-tail').show();
          }).fail(function(jqXHR, textStatus, errorThrown) {
            show_err_msg("Couldn't preview the file. " + errorThrown);
          });
        }).fail(function(jqXHR, textStatus, errorThrown) {
          show_err_msg("Couldn't find datanode to read file from. " + errorThrown);
        });
      }

      var request = null;
      $('#file-info-preview-tail')
	   .off('click')
	   .on('click', function() {
        var offset = d.fileLength - TAIL_CHUNK_SIZE;
        var url = offset > 0 ? download_url + '&offset=' + offset : download_url;
        processPreview(url);
      });
      $('#file-info-preview-head')
	   .off('click')
	   .on('click', function() {
        var url = d.fileLength > TAIL_CHUNK_SIZE ? download_url + '&length=' + TAIL_CHUNK_SIZE : download_url;
        processPreview(url);
      });

      if (d.fileLength > 0) {
        show_block_info(d.locatedBlocks);
        $('#file-info-blockinfo-panel').show();
      } else {
        $('#file-info-blockinfo-panel').hide();
      }
      $('#file-info').modal();
    }).fail(network_error_handler(url));
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
            .fail(network_error_handler(url))
            .done(function() {
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
      if(data == 0){
        return "";
      }
      if(data < cutoff) {
        return moment(Number(data)).format('MMM DD YYYY');
      } else {
        return moment(Number(data)).format('MMM DD HH:mm');
      }
    }
    return data;
  }

  function browse_directory(dir) {
    if (dir.match('^/+$')) {
      $('#parentDir').prop('disabled', true);
    } else {
      $('#parentDir').prop('disabled', false);
    }
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

        $('.explorer-entry .explorer-trash-links').click(function() {
          var inode_name = $(this).closest('tr').attr('inode-path');
          var absolute_file_path = append_path(current_directory, inode_name);
          delete_path(inode_name, absolute_file_path);
        });

        $('#file-selector-all').click(function() {
          $('.file_selector').prop('checked', $('#file-selector-all')[0].checked );
        });

        $('.explorer-entry .explorer-rename-links').click(function() {
          const inode_name = $(this).closest('tr').attr('inode-path');
          $('#btn-rename-file-send').prop('disabled', true).button('reset');
          $('#new_filename').val(inode_name);

          $('#new_filename').on('input', function () {
            const new_filename = $.trim($('#new_filename').val());
            if (new_filename == null || new_filename == '' || new_filename == inode_name) {
              $('#btn-rename-file-send').prop('disabled', true);
            } else {
              $('#btn-rename-file-send').prop('disabled', false);
            }
          });

          $('#new_filename').off('keypress').on('keypress', function (event) {
            // input new file name and press enter
            if(event.keyCode == 13){
              const new_filename = $.trim($('#new_filename').val());
              if(new_filename != inode_name && new_filename != '') {
                $('#btn-rename-file-send').trigger("click");
              }
            }
          });

          $('#btn-rename-file-send').off('click').on('click', function () {
            $(this).prop('disabled', true);
            $(this).button('complete');

            const absolute_file_path = append_path(current_directory, inode_name);
            const new_filename = $.trim($('#new_filename').val());
            const new_file_path = append_path(current_directory, new_filename);
            const url = '/webhdfs/v1' + encode_path(absolute_file_path) + '?op=RENAME&destination=' + encode_path(new_file_path);
            $.ajax(url, { type: 'PUT'})
                .fail(network_error_handler(url))
                .done(function(data) {
                  const success = get_response(data, "boolean")
                  if(success) {
                    show_info_msg("Renamed " + absolute_file_path + " to " + new_file_path);
                  } else {
                    show_err_msg("Failed to rename " + absolute_file_path + " to " + new_file_path);
                  }
                  browse_directory(current_directory);
                })
                .always(function(){
                  $('#modal-rename-file').modal('hide');
                  $('#btn-rename-file-send').button('reset');
                });
          });
        });

        //This needs to be last because it repaints the table
        $('#table-explorer').dataTable( {
          'lengthMenu': [ [25, 50, 100, -1], [25, 50, 100, "All"] ],
          'columns': [
            { 'orderable' : false }, //select
            {'searchable': false }, //Permissions
            null, //Owner
            null, //Group
            { 'searchable': false, 'render': func_size_render}, //Size
            { 'searchable': false, 'render': func_time_render}, //Last Modified
            { 'searchable': false, 'render': func_time_render}, //Last Accessed
            { 'searchable': false }, //Replication
            null, //Block Size
            null, //Name
            { 'orderable' : false } //Actions
          ],
          "deferRender": true
        });
      });
    }).fail(network_error_handler(url));
  }

  $('#parentDir').click(function () {
    var current = current_directory;
    var parent = current.replace(/\/+[^/]+\/*$/,"") || '/';
    browse_directory(parent);
  });

  $('#homeDir').click(function () {
    browse_directory(get_homedirectory());
  });

  function set_namespace_info(){
    const current_ns = get_current_namespace();
    let ns_html;
    if(current_ns == ""){
      ns_html = "";
    } else {
      ns_html = "hdfs://" + current_ns;
    }
    $("#namespace").html(ns_html);
  }

  function init() {
    dust.loadSource(dust.compile($('#tmpl-explorer').html(), 'explorer'));
    dust.loadSource(dust.compile($('#tmpl-block-info').html(), 'block-info'));

    set_namespace_info();

    const homedir = get_homedirectory();

    $('#btn-copy-clipboard').click(async function (){
      const current_ns = get_current_namespace();
      const directory = $('#directory').val();

      const path = 'hdfs://' + current_ns + directory;
      await navigator.clipboard.writeText(path);
      show_info_msg('Copied to clipboard: ' + path);
    });

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
      dir = get_path_from_query();
    }
    if(dir == "") {
      window.location.hash = homedir;
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

    // Get umask from the configuration
    let actualUmask = get_permission();
    let new_dir = $('#new_directory').val();
    let url = '/webhdfs/v1' + encode_path(append_path(current_directory,
      new_dir)) + '?op=MKDIRS';

    if (actualUmask != -1) {
      url = url + '&permission=' + actualUmask;
    }

    $.ajax(url, { type: 'PUT' }
    ).done(function(data) {
      let destination = append_path(current_directory, new_dir);
      show_info_msg(destination + " created successfully")
      browse_directory(current_directory);
    }).fail(network_error_handler(url)
     ).always(function() {
       $('#btn-create-directory').modal('hide');
       $('#btn-create-directory-send').button('reset');
    });
  })

  $('#btn-upload-files').click(function() {
        $('#modal-upload-file-button').prop('disabled', true).button('reset');
        $('#modal-upload-file-input').val(null);
        $("#progress-bar").css("width", "0%");
        $("#progress-bar").text("");
      });

  $('#btn-create-dir').click(function() {
        $('#btn-create-directory-send').prop('disabled', true).button('reset');
        $('#new_directory').val(null);
      });

  $('#modal-upload-file-input').change(function() {
      if($('#modal-upload-file-input').prop('files').length >0) {
         $('#modal-upload-file-button').prop('disabled', false);
        }
      else {
        $('#modal-upload-file-button').prop('disabled', true);
        }
      });

  $('#new_directory').on('input',function() {
      const new_directory = $.trim($('#new_directory').val());
      if(new_directory == '' ||  new_directory == null) {
         $('#btn-create-directory-send').prop('disabled', true);
      } else {
         $('#btn-create-directory-send').prop('disabled', false);
      }
  });

  $('#new_directory').on('keypress',function(event) {
    // input new dir name and press enter
    if(event.keyCode == 13){
      const new_directory = $.trim($('#new_directory').val());
      if(new_directory != '') {
        $('#btn-create-directory-send').trigger("click");
      }
    }
  });

  $('#modal-upload-file-button').click(function() {
    $(this).prop('disabled', true);
    $(this).button('complete');
    var files = []
    var numCompleted = 0
    const success_message = [];
    const destination_directory = current_directory;

    for(var i = 0; i < $('#modal-upload-file-input').prop('files').length; i++) {
      (function() {
        let file = $('#modal-upload-file-input').prop('files')[i];
        let url = '/webhdfs/v1' + encode_path(append_path(destination_directory, file.name));
        let permission = get_permission();
        url += '?op=CREATE&noredirect=true';
        if(permission != -1){
          url += "&permission=" + permission;
        }
        files.push( { file: file } )
        files[i].request = $.ajax({
          type: 'PUT',
          url: url,
          processData: false,
          crossDomain: true
        });
      })()
     }

    for(let i = 0; i < files.length; i++) {
      (function() {
        var file = files[i];

        const filename = file.file.name;
        const id = "progress-bar-" + i;
        const parent_id = "progress-" + i;
        $("#progress")
            .append($($('<div>')
                .addClass('progress')
                .attr('id', parent_id)
                .append($($('<div>')
                    .addClass('progress-bar progress-bar-success')
                    .attr('role','progressbar')
                    .attr('aria-valuenow', '0')
                    .attr('aria-valuemin', '0')
                    .attr('aria-valuemax', '100')
                    .attr('id', id)
            ))
          ));

        file.request.done(function(data) {
          var url = data['Location'];
          const start = new Date();
          $.ajax({
            type: 'PUT',
            url: url,
            data: file.file,
            processData: false,
            crossDomain: true,
            contentType: 'application/octet-stream',
            xhr: function () {
              let xhr = new window.XMLHttpRequest();
              xhr.upload.addEventListener("progress", function (evt) {
                if(evt.lengthComputable) {
                  let percentComplete = Math.round((evt.loaded / evt.total) * 100);
                  const elapsed_ms = new Date().getTime() - start.getTime()

                  $('#' + id).css("width", percentComplete + "%");
                  let progress_message;
                  if(files.length == 1) {
                    progress_message = percentComplete + "% (" + millis_to_str(elapsed_ms) + ")";
                  } else {
                    progress_message = filename + " " + percentComplete + "% (" + millis_to_str(elapsed_ms) + ")";
                  }
                  $('#' + id).text(progress_message);
                }
              }, false);
              return xhr;
            }
          }).always(function(data) {
            numCompleted++;
            if(numCompleted == files.length) {
              reset_upload_button();
              browse_directory(destination_directory);
            }
            $("#" + parent_id).remove();
          }).done(function (data) {
            let destination = append_path(destination_directory, file.file.name);
            const elapsed_ms = new Date().getTime() - start.getTime()
            success_message.push(destination + " uploaded successfully (" + millis_to_str(elapsed_ms) + ")");
            show_info_msg(success_message.join("<br>"));
          }).fail(function(jqXHR, textStatus, errorThrown) {
            numCompleted++;
            reset_upload_button();
            show_err_msg("Couldn't upload the file " + file.file.name + ". "+ errorThrown);
          });
        }).fail(function(jqXHR, textStatus, errorThrown) {
          numCompleted++;
          reset_upload_button();
          show_err_msg("Couldn't find datanode to write file. " + errorThrown);
          $("#" + parent_id).remove();
        });
      })();
    }
  });

  function reset_message_panel(){
    $('#alert-panel').hide();
    $('#info-panel').hide();
  }

  //Reset the upload button
  function reset_upload_button() {
    $('#modal-upload-file').modal('hide');
    $('#modal-upload-file-button').button('reset');
  }

  //Store the list of files which have been checked into session storage
  function store_selected_files(current_directory) {
    sessionStorage.setItem("source_directory", current_directory);
    var selected_files = $("input:checked.file_selector");
    var selected_file_names = new Array();
    selected_files.each(function(index) {
      selected_file_names[index] = $(this).closest('tr').attr('inode-path');
    })
    sessionStorage.setItem("selected_file_names", JSON.stringify(selected_file_names));
    alert("Cut " + selected_file_names.length + " files/directories");
  }

  //Retrieve the list of files from session storage and rename them to the current
  //directory
  function paste_selected_files() {
    var files = JSON.parse(sessionStorage.getItem("selected_file_names"));
    var source_directory = sessionStorage.getItem("source_directory");
    $.each(files, function(index, value) {
      var url = "/webhdfs/v1"
        + encode_path(append_path(source_directory, value))
        + '?op=RENAME&destination='
        + encode_path(append_path(current_directory, value));
      $.ajax({
        type: 'PUT',
        url: url
      }).done(function(data) {
        if(index == files.length - 1) {
          browse_directory(current_directory);
        }
      }).fail(function(jqXHR, textStatus, errorThrown) {
        show_err_msg("Couldn't move file " + value + ". " + errorThrown);
      });

    })
  }

  function move_selected_files_to_trash(current_directory) {
    const selected_files = $("input:checked.file_selector");
    const selected_file_names = new Array();
    selected_files.each(function(index) {
      selected_file_names[index] = $(this).closest('tr').attr('inode-path');
    })

    if(selected_file_names.length == 0){
      alert("No selected files/directories");
      return;
    }

    $('#delete-modal-title').text("Trash");

    let file_list_summary;
    if(selected_file_names.length == 1){
      file_list_summary = selected_file_names[0];
    } else {
      file_list_summary = selected_file_names[0] + " and " + (selected_file_names.length - 1) + " more"
    }

    $('#delete-prompt').html("Are you sure you want to move <span style=\"color: red;font-weight:bold;\">"
        + file_list_summary + "</span> to trash?");

    $('#delete-button').off("click").on("click", function() {
      get_trashroot();
      if(trash_root == "") {
        $('#delete-modal').modal('hide');
        $('#delete-button').button('reset');
        return;
      }
      const trash_current = append_path(trash_root, "Current");
      const trash_root_parent = parent_dir(trash_root);

      // check trash path
      for(let i= 0; i < selected_file_names.length; i++) {
        const absolute_file_path = append_path(current_directory, selected_file_names[i]);

        // abs_file_path: /user/foo/.Trash/file
        // trash_root: /user/foo/.Trash
        if (absolute_file_path == trash_root || absolute_file_path.startsWith(trash_root + "/")){
          show_err_msg("Cannot move " + absolute_file_path + " to the trash, it is already in trash");
          $('#delete-modal').modal('hide');
          $('#delete-button').button('reset');
          return;
        }

        // abs_file_path: /user/foo
        // trash_root_parent: /user/foo
        if (trash_root_parent == absolute_file_path || trash_root_parent.startsWith(absolute_file_path + "/")){
          show_err_msg("Cannot move " + absolute_file_path + " to the trash, as it contains the trash");
          $('#delete-modal').modal('hide');
          $('#delete-button').button('reset');
          return;
        }
      }

      const base_trash_path = append_path(trash_current, current_directory);
      if (!mkdirs(base_trash_path)) {
        $('#delete-modal').modal('hide');
        $('#delete-button').button('reset');
        return;
      }

      const success_message = [];
      for(let i= 0; i < selected_file_names.length; i++) {
        const absolute_file_path = append_path(current_directory, selected_file_names[i]);
        const trash_path = get_unique_trash_path(append_path(trash_current, absolute_file_path));

        let error = false;

        const url = "/webhdfs/v1" + encode_path(absolute_file_path) + "?op=RENAME&renameoptions=TO_TRASH&destination=" + encode_path(trash_path);
        $.ajax({
          type: "PUT",
          url: url,
          async: false
        }).done(function (data) {
          success_message.push("Moved " + absolute_file_path + " to trash at: " + trash_path);
          show_info_msg(success_message.join("<br>"));
        }).fail(function(jqxhr, textStatus, errorThrown) {
          error = true;
          let msg = "";
          if(jqxhr.status == 403 && jqxhr.responseJSON !== undefined && jqxhr.responseJSON.RemoteException !== undefined) {
            msg = jqxhr.responseJSON.RemoteException.message;
          } else {
            msg = 'Failed to retrieve data from ' + url + ': ' + errorThrown;
          }
          show_err_msg("Couldn't move file " + absolute_file_path + " to trash: " + msg);
        }).always(function () {
          $('#delete-modal').modal('hide');
          $('#delete-button').button('reset');
        });
        if(error){
          break;
        }
      }
      browse_directory(current_directory);
    });

    $('#delete-modal').modal();
  }

  $('#explorer-cut').click(function() {
    store_selected_files(current_directory);
  });

  $('#explorer-paste').click(function() {
    paste_selected_files();
  });

  $('#btn-to-trash').click(function() {
    move_selected_files_to_trash(current_directory);
  });


  init();
})();
