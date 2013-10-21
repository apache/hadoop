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
  var helpers = {
    'helper_to_permission': function(chunk, ctx, bodies, params) {
      var p = ctx.current().permission;
      var dir = ctx.current().type == 'DIRECTORY' ? 'd' : '-';
      var symbols = [ '---', '--x', '-w-', '-wx', 'r--', 'r-x', 'rw-', 'rwx' ];
      var sticky = p > 1000;

      var res = "";
      for (var i = 0; i < 3; ++i) {
	res = symbols[(p % 10)] + res;
	p = Math.floor(p / 10);
      }

      if (sticky) {
	var exec = ((parms.perm % 10) & 1) == 1;
	res[res.length - 1] = exec ? 't' : 'T';
      }

      chunk.write(dir + res);
      return chunk;
    }
  };

  var base = dust.makeBase(helpers);
  var current_directory = "";

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  function network_error_handler(url) {
    return function (jqxhr, text, err) {
      var msg = '<p>Failed to retreive data from ' + url + ', cause: ' + err + '</p>';
      if (url.indexOf('/webhdfs/v1') === 0)  {
        msg += '<p>WebHDFS might be disabled. WebHDFS is required to browse the filesystem.</p>';
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
    var msg = data.RemoteException !== undefined ? data.RemoteException.message : "";
    return msg;
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

    var url = '/webhdfs/v1' + abs_path + '?op=GET_BLOCK_LOCATIONS';
    $.ajax({"url": url, "crossDomain": true}).done(function(data) {
      var d = get_response(data, "LocatedBlocks");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      $('#file-info-tail').hide();
      $('#file-info-title').text("File information - " + path);

      var download_url = '/webhdfs/v1' + abs_path + '/?op=OPEN';

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

  function browse_directory(dir) {
    var url = '/webhdfs/v1' + dir + '?op=LISTSTATUS';
    $.get(url, function(data) {
      var d = get_response(data, "FileStatuses");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      current_directory = dir;
      $('#directory').val(dir);
      dust.render('explorer', base.push(d), function(err, out) {
        $('#panel').html(out);

        $('.explorer-browse-links').click(function() {
          var type = $(this).attr('inode-type');
          var path = $(this).attr('inode-path');
          var abs_path = append_path(current_directory, path);
          if (type == 'DIRECTORY') {
            browse_directory(abs_path);
          } else {
            view_file_details(path, abs_path);
          }
        });
      });
    }).error(network_error_handler(url));
  }


  function init() {
    var templates = [
      { 'name': 'explorer', 'url': 'explorer.dust.html'},
      { 'name': 'block-info', 'url': 'explorer-block-info.dust.html'}
    ];

    load_templates(dust, templates, function () {
      var b = function() { browse_directory($('#directory').val()); };
      $('#btn-nav-directory').click(b);
      browse_directory('/');
    }, function (url, jqxhr, text, err) {
      network_error_handler(url)(jqxhr, text, err);
    });
  }

  init();
})();
