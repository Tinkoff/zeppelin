// ---------------------   jsTree - license    ------------------------
//                                                 -------- MIT license
// https://www.jstree.com/
// Copyright (c) 2014 Ivan Bozhanov
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
//     copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
//     The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
//     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//     EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//     WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Do not delete import
// eslint-disable-next-line
import jstree from 'jstree';
import sqlFormatter from 'svl-tin-sql-formatter';

angular.module('zeppelinWebApp').controller('SavedTableExplorerCtrl',
  SavedTableExplorerCtrl);

function SavedTableExplorerCtrl($scope, $http, baseUrlSrv, ngToast) {
  'ngInject';

  $scope.saved_tables_list;
  $scope.currentSource = null;
  $scope.sources = [];
  $scope.searchText = '';
  $scope.unavailableBlockDisplay = false;

  // TODO(SAN) init vars
  let metaServerUrl = null;
  let iconSize = 32;
  let jstreeDOM = null;
  let searchParams = null;
  let contentDOM = null;
  let dbExplorerDOM = null;
  let widthResizeOn = false;
  let explorerDisplayed = false;
  let $ = require('jquery');

  $scope.init = function() {
    $scope.configrations = [];
    ngToast.dismiss();

    $scope.getConfigurations = function() {
      $http.get(baseUrlSrv.getRestApiBase() + '/configurations/all')
        .success(function(data, status, headers, config) {
          $scope.configrations = data.body;
        })
        .error(function(data, status, headers, config) {
          if (status === 401) {
            ngToast.danger({
              content: 'You don\'t have permission on this page',
              verticalPosition: 'bottom',
              timeout: '3000',
            });
            setTimeout(function() {
              window.location = baseUrlSrv.getBase();
            }, 3000);
          }
        });
    };
    // 1 - in prod, 2 - in web dev mod
    let getMetaUrlApi = window.location.origin + '/api/metadata_server_url'; // 1

    console.log('----------------- database explorer controller init');
    // let getMetaUrlApi = 'http://localhost:8080' + '/api/metadata_server_url'; // 2

    // get metaserver address
    databaseServerRequest(
      getMetaUrlApi, (responseText) => {
        metaServerUrl = JSON.parse(responseText).message;

        // then load jstree search limit
        databaseServerRequest(
          metaServerUrl + '/jstree/get_search_limit', (responseText) => {
            let maxElement = parseInt(responseText);
            searchParams = {
              'MAX_DISPLAYED_ELEMENTS': maxElement,
              'CURRENT_DISPLAYED': 0,
            };
          });

        // and load sources list
        databaseServerRequest(metaServerUrl + '/jstree/databases_list', (responseText) => {
          let answer = JSON.parse(responseText);
          answer.forEach((e) => $scope.sources.push(e));
          $scope.currentSource = $scope.sources[0];
          $(document).ready(function() {
            explorerTreeInit();
          });
        });
      });
  };

  $scope.dbExplorerSearch = function(event) {
    if (event) {
      let key = event.which || event.keyCode;
      if (key !== 13) {
        return;
      }
    }
    jstreeDOM.jstree(true).clear_search();
    jstreeDOM.jstree(true).close_all();
    jstreeDOM.jstree(true).refresh();
    showMaxSearchElementBlock(false);
    if (this.searchText !== '') {
      jstreeDOM.jstree(true).search(this.searchText);
    }
  };

  $scope.$on('switchDatabaseExplorer', function() {
    console.log('--------------switch database controller from database-explorer controller');
    showDatabaseExplorer(!explorerDisplayed);
  });

  function showDatabaseExplorer(show) {
    initDomVars();
    explorerDisplayed = show;
    if (explorerDisplayed) {
      showDBExplorer();
    } else {
      hideDBExplorer();
    }
  }

  function initDomVars() {
    if (!contentDOM) {
      contentDOM = document.getElementsByClassName('notebookContent')[0];
    }
    if (!dbExplorerDOM) {
      dbExplorerDOM = document.getElementsByClassName('db-explorer')[0];
    }
  }

  function databaseServerRequest(request, callback) {
    let xhr = new XMLHttpRequest();
    xhr.open('GET', request, true);
    xhr.onreadystatechange = function() {
      if (xhr.readyState !== XMLHttpRequest.DONE) {
        return;
      }
      try {
        callback(xhr.responseText);
      } catch (err) {
        $scope.unavailableBlockDisplay = true;
      }
    };
    xhr.send();
  }

  function showDBExplorer() {
    dbExplorerDOM.style.display = 'flex';
    dbExplorerDOM.style.width = '300px';
    contentDOM.style.marginLeft = '310px';
    window.dispatchEvent(new Event('resize'));
  }

  function hideDBExplorer() {
    dbExplorerDOM.style.display = 'none';
    dbExplorerDOM.style.width = '0px';
    contentDOM.style.marginLeft = '0px';
    window.dispatchEvent(new Event('resize'));
  }

  function dbExplorerChangeHeight(px) {
    let currentSize = px;
    if (currentSize < 280) {
      currentSize = 280;
    }
    if (currentSize > 650) {
      currentSize = 650;
    }
    dbExplorerDOM.style.width = currentSize + 'px';
    contentDOM.style.marginLeft = (currentSize + 10) + 'px';
  }

  $scope.dbExplorerMouseDown = function() {
    widthResizeOn = true;
  };

  $(document).mousemove(function(event) {
    if (!widthResizeOn) {
      return;
    }
    dbExplorerChangeHeight(event.clientX);
  });

  $(document).mouseup(function() {
    widthResizeOn = false;
  });

  $scope.changeDatabase = function(newSource) {
    showMaxSearchElementBlock(false);
    $scope.currentSource = newSource;
    jstreeDOM.jstree(true).close_all();
    jstreeDOM.jstree(true).refresh();
  };

  function explorerTreeInit() {
    let local$ = require('jquery');
    jstreeDOM = local$('#jstree');
    jstreeDOM.jstree({
      'sort': function(nodeId1, nodeId2) {
        let jsTree = jstreeDOM.jstree(true);
        let node1 = jsTree.get_node(nodeId1).original;
        let node2 = jsTree.get_node(nodeId2).original;
        if (node1.type === 'schema' && node2.type === 'schema') {
          return new Intl.Collator().compare(node1.text, node2.text);
        }
      },
      'massload': {
        'url': metaServerUrl + '/jstree/massload',
        'data': function(nodes) {
          return {
            'ids': nodes.join(','),
            'database': $scope.currentSource,
          };
        },
      },
      'core': {
        'animation': 0,
        'worker': false,
        // 'force_text': true,
        'check_callback': true,
        'data': {
          'url': metaServerUrl + '/jstree/get_children',
          'data': function(node) {
            return {
              'id': node.id,
              'database': $scope.currentSource,
              'type': node.original && node.original.type,
              'schemaId': node.parents[node.parents.length - 2],
            };
          },
        },
      },
      'search': {
        'show_only_matches': true,
        'ajax': {
          'url': metaServerUrl + '/jstree/search?database=' + $scope.currentSource,
        },
        'search_callback': function(str, node) {
          if (searchParams.CURRENT_DISPLAYED
            <= searchParams.MAX_DISPLAYED_ELEMENTS) {
            if (node.text.split(' ')[0].indexOf(str) !== -1) {
              searchParams.CURRENT_DISPLAYED++;
              return true;
            }
          }
          return false;
        },
      },
    }).on('open_node.jstree', function(event, data) {
      data.node.children.forEach(addCommentBlock);
    }).on('redraw.jstree', function(event, data) {
      data.nodes.forEach(addCommentBlock);
    }).on('search.jstree', function(event, data) {
      let limitExceeded = searchParams.CURRENT_DISPLAYED
        >= searchParams.MAX_DISPLAYED_ELEMENTS;
      showMaxSearchElementBlock(limitExceeded);
      searchParams.CURRENT_DISPLAYED = 0;
    }).on('load_node.jstree', function(event, data) {
      // сделать отступы для отображения типов
      let nodes = data.node.children.map((id) => jstreeDOM.jstree(true).get_node(id));
      let maxLength = Math.max.apply(null, nodes.map((node) => node.text.length)) + 3;
      nodes.forEach(function(node) {
        let spaces = ' '.repeat(maxLength - node.original.text.length);
        if (node.original.value_type) {
          node.text = node.original.text + spaces
            + '<span class = "column-type-span">' + node.original.value_type + '</span>';
        }
      });
    }).on('model.jstree', function(event, data, parent) {
      data.nodes.forEach((id) => {
        let node = data.instance.get_node(id);
        let imageUrl = metaServerUrl + '/image/' + node.original.icon_name + '/' + iconSize;
        data.instance.set_icon(node, imageUrl);
      });
    });
    local$(document).on('dnd_stop.vakata', function(data, element) {
      pasteSelectInElement(element.event.target, element.data.nodes,
        element.event.ctrlKey);
    });
    local$(document).on('dnd_move.vakata', function() {
      $('#jstree-dnd > i').remove();
    });

    showDatabaseExplorer(explorerDisplayed);
  }

  function showMaxSearchElementBlock(show) {
    if (show) {
      $('#jstreeMaxElementDiv')[0].style.visibility = 'visible';
    } else {
      $('#jstreeMaxElementDiv')[0].style.visibility = 'collapse';
    }
  }

  function pasteSelectInElement(element, nodesIds, ctrlKey) {
    let paragraphScope = angular.element(element.closest('.paragraph')).scope();
    if (!paragraphScope) {
      return;
    }
    let aceEditor = paragraphScope.editor;
    if (!aceEditor) {
      return;
    }
    if (element.className !== 'ace_content') {
      return;
    }
    let textToPasteInEditor;

    if (nodesIds.length === 1 && !ctrlKey) {
      let node = jstreeDOM.jstree(true).get_node(nodesIds[0]);
      if (node.original.type === 'column') {
        textToPasteInEditor = node.text.split(' ')[0];
      } else {
        textToPasteInEditor = getNameWithParent(nodesIds[0]);
      }
    }
    if (nodesIds.length > 1 && !ctrlKey) {
      textToPasteInEditor = stringListOfNodes(nodesIds, true);
    }
    if (nodesIds.length >= 1 && ctrlKey) {
      textToPasteInEditor = generateAndPasteSQLSelect(aceEditor, nodesIds);
      return;
    }

    if (!textToPasteInEditor) {
      return;
    }
    aceEditor.session.insert(aceEditor.getCursorPosition(),
      textToPasteInEditor);
  }

  function generateAndPasteSQLSelect(aceEditor, nodesIds) {
    let tablesIds = new Set();
    let columnsIds = new Set();
    nodesIds.forEach(function(id) {
      let node = jstreeDOM.jstree(true).get_node(id);

      switch (node.original.type) {

        case 'table': {
          tablesIds.add(id);
          node.children.forEach((childId) => columnsIds.add(childId));
          break;
        }

        case 'column': {
          columnsIds.add(id);
          let nodeParent = jstreeDOM.jstree(true).get_node(node.parent);
          tablesIds.add(nodeParent.id);
          break;
        }
      }
    });
    tablesIds = Array.from(tablesIds);
    let needToLoadChildren = false;

    tablesIds.forEach(function loadChildren(id) {
      if (!jstreeDOM.jstree(true).is_loaded(id)) {
        needToLoadChildren = true;
        jstreeDOM.jstree(true).load_node(id, function callback(node) {
          node.children.forEach((childId) => columnsIds.add(childId));
          let allLoaded = true;
          for (let i = 0; i < tablesIds.length && allLoaded; i++) {
            allLoaded = jstreeDOM.jstree(true).is_loaded(tablesIds[i]);
          }
          if (!allLoaded) {
            return;
          }
          pasteSelect();
        });
      }
    });

    if (!needToLoadChildren) {
      pasteSelect();
    }

    function pasteSelect() {
      let select = 'SELECT ' + stringListOfNodes(columnsIds) + '\n';
      select += 'FROM ' + stringListOfNodes(tablesIds);

      // beatify sql code
      select = sqlFormatter.format(select, {maxCharacterPerLine: 128});

      aceEditor.session.insert(aceEditor.getCursorPosition(), select);
    }
  }

  function stringListOfNodes(nodesIds, columnsWithoutTable) {
    let list = [];
    nodesIds.forEach(function(id) {
      if (columnsWithoutTable) {
        let node = jstreeDOM.jstree(true).get_node(id);
        if (node.type === 'column') {
          list.push(node.text.split(' ')[0]);
          return;
        }
      }

      list.push(getNameWithParent(id));
    });
    return list.join(', ');
  }

  function getNameWithParent(nodeId) {
    let node = jstreeDOM.jstree(true).get_node(nodeId);
    let nodeParent = jstreeDOM.jstree(true).get_node(node.parent);
    let result;

    switch (node.original.type) {

      case 'schema':
        result = node.text;
        break;

      case 'table':
        result = nodeParent.text + '.' + node.text;
        break;

      case 'column':
        result = nodeParent.text + '.' + node.text.split(' ')[0];
        break;
    }

    return result;
  }

  function addCommentBlock(id) {
    if (!jstreeDOM.jstree(true).get_node(id).original) {
      return;
    }
    let commentText = jstreeDOM.jstree(true).get_node(id).original.description;
    if (!commentText) {
      return;
    }
    let nodeDOM = jstreeDOM.jstree(true).get_node(id, true);
    if (nodeDOM.length === 0) {
      return;
    }
    let info = document.createElement('i');
    info.className = 'glyphicon glyphicon-info-sign my-glyphicon-color-comment';
    info.style.paddingLeft = '10px';
    info.setAttribute('title', commentText);
    nodeDOM['0'].children['1'].append(info);

    let node = jstreeDOM.jstree(true).get_node(id);
    if (node.children.length !== 0) {
      node.children.forEach((id) => addCommentBlock(id));
    }
  }
}
