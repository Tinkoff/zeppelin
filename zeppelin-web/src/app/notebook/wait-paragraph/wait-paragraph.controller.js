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

import moment from 'moment';

require('moment-duration-format');

angular.module('zeppelinWebApp').controller('WaitingParagraphCtrl', WaitingParagraphCtrl);

function WaitingParagraphCtrl($scope, noteListFactory, websocketMsgSrv, $rootScope, arrayOrderingSrv,
                  ngToast, noteActionService, TRASH_FOLDER_ID, favoriteNotesService, $http,
                  baseUrlSrv, $route) {
  'ngInject';

  ngToast.dismiss();

  $scope.interpreterSettings = [];
  $scope.notes = noteListFactory;
  $scope.favoriteNotes = [];
  $scope.recentNotes = [];
  $scope.websocketMsgSrv = websocketMsgSrv;
  $scope.arrayOrderingSrv = arrayOrderingSrv;
  $scope.noteActionService = noteActionService;
  $scope.numberOfNotesDisplayed = window.innerHeight / 20;

  $scope.isReloading = false;
  $scope.TRASH_FOLDER_ID = TRASH_FOLDER_ID;
  $scope.query = {q: ''};

  $scope.init = function(newParagraph, note, permissions) {
    $scope.paragraph = newParagraph;
    $scope.parentNote = note;
    $scope.paragraphFocused = false;
    if (newParagraph.focus) {
      $scope.paragraphFocused = true;
    }
    if (!$scope.paragraph.config) {
      $scope.paragraph.config = {};
    }

    $scope.permissions = permissions;
    if (!permissions) {
      $scope.permissions = {
        owners: [],
        readers: [],
        runners: [],
        writers: [],
      };
    }

    favoriteNotesService.init();

    favoriteNotesService.filterFavoriteNotes($scope.notes.flatList, (notes) => {
      $scope.favoriteNotes = notes;
    });

    favoriteNotesService.filterRecentNotes($scope.notes.flatList, (notes) => {
      $scope.recentNotes = notes;
    });

    $scope.isNoteRunning = !!(note && note.hasOwnProperty('info') &&
      note.info && note.info.hasOwnProperty('isRunning') && note.info.isRunning === true);

    getInterpreterSettings();
    $scope.notePath = getNotePath($scope.paragraph.text);

    let editorSetting = $scope.paragraph.config.editorSetting;
    if ($scope.paragraph.config && editorSetting && editorSetting.warning
        && editorSetting.warning_condition && editorSetting.action) {
      $scope.warning = editorSetting.warning;
      $scope.warning_condition = editorSetting.warning_condition;
      $scope.action = editorSetting.action;
    }
  };

  $scope.setParagraphMode = function() {
    let index = _.findIndex($scope.interpreterSettings, {'shebang': $scope.paragraph.shebang});
    if (index < 0) {
      return;
    }

    _.merge($scope.paragraph.config.editorSetting, $scope.interpreterSettings[index].config.editor);

    if ($scope.interpreterSettings[index].config.editor.language) {
      let editorSetting = $scope.paragraph.config.editorSetting;
      if ($scope.paragraph.config && editorSetting && editorSetting.warning
          && editorSetting.warning_condition && editorSetting.action) {
        delete editorSetting.warning;
        delete editorSetting.warning_condition;
        delete editorSetting.action;
      }
    } else {
      let editorSetting = $scope.paragraph.config.editorSetting;
      if ($scope.paragraph.config && editorSetting && editorSetting.warning
          && editorSetting.warning_condition && editorSetting.action) {
        $scope.warning = editorSetting.warning;
        $scope.warning_condition = editorSetting.warning_condition;
        $scope.action = editorSetting.action;
      }
    }
  };

  $scope.check = function() {
    if ($scope.warning_condition) {
      return eval($scope.warning_condition);
    }
    return false;
  };

  $scope.getExecutionTime = function(pdata) {
    if (pdata.endedAt === undefined || pdata.startedAt === undefined) {
      return 'New paragraph.';
    }
    const end = convertTime(pdata.endedAt);
    const start = convertTime(pdata.startedAt);

    let timeMs = end - start;
    if (isNaN(timeMs) || timeMs < 0) {
      if ($scope.isResultOutdated(pdata)) {
        return 'outdated';
      }
      return '';
    }

    const durationFormat = moment.duration((timeMs / 1000), 'seconds').format('h [hrs] m [min] s [sec]');
    const endFormat = moment(end).format('MMMM DD YYYY, h:mm:ss A');

    let user = (pdata.user === undefined || pdata.user === null) ? 'anonymous' : pdata.user;
    let desc = `Took ${durationFormat}. Last updated by ${user} at ${endFormat}.`;

    if ($scope.isResultOutdated(pdata)) {
      desc += ' (outdated)';
    }
    return desc;
  };

  $rootScope.$on('updatePermissions', function(event, noteId, permissions) {
    if ($scope.note.databaseId === noteId) {
      $scope.permissions = permissions;
    }
  });

  $rootScope.$on('updateCron', function(event, noteId, expression, isEnabled) {
    if ($scope.note.databaseId === noteId) {
      $scope.note.scheduler.isEnabled = isEnabled;
      $scope.note.scheduler.expression = expression;
    }
  });

  $scope.isResultOutdated = function(pdata) {
    if (pdata.updated !== undefined && Date.parse(pdata.updated) > Date.parse(pdata.startedAt)) {
      return true;
    }
    return false;
  };

  function convertTime(time) {
    return new Date(time.date.year, time.date.month - 1, time.date.day, time.time.hour,
      time.time.minute, time.time.second, Math.floor(time.time.nano / 1000000));
  }


  $scope.getElapsedTime = function(paragraph) {
    return 'Started ' + moment(convertTime(paragraph.startedAt)).fromNow() + '.';
  };

  $scope.runParagraphFromButton = function() {
    if ($scope.isNoteRunning) {
      return;
    }

    if ($scope.paragraph.text !== -1) {
      let selectedText = getNoteId($scope.paragraph.text);
      websocketMsgSrv.runParagraph($scope.paragraph.id, $scope.paragraph.title,
        $scope.paragraph.text, $scope.paragraph.config, {}, selectedText);
    }
  };

  $scope.saveParagraph = function(paragraph) {
    const dirtyText = paragraph.text;
    if (dirtyText === undefined) {
      return;
    }

    $scope.commitParagraph(paragraph);
  };

  $scope.cancelParagraph = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    console.log('Cancel %o', paragraph.id);
    websocketMsgSrv.cancelParagraphRun(paragraph.id);
  };

  $scope.reloadNoteList = function() {
    $scope.isReloadingNotes = true;
    websocketMsgSrv.reloadAllNotesFromRepo();
    setTimeout(() => $scope.isReloadingNotes = false, 100);
  };

  $scope.toggleFolderNode = function(node) {
    node.hidden = !node.hidden;
  };


  $scope.columnWidthClass = function(n) {
    if ($scope.asIframe) {
      return 'col-md-12';
    } else {
      return 'paragraph-col col-md-' + n;
    }
  };

  let getInterpreterSettings = function() {
    $http.get(baseUrlSrv.getRestApiBase() + '/modules/setting/interpreters')
      .then(function(res) {
        $scope.interpreterSettings = res.data.body;
        if (!$scope.paragraph.shebang) {
          // set default shebang if interpreter exist
          if ($scope.interpreterSettings && $scope.interpreterSettings.length > 0) {
            $scope.paragraph.shebang = $scope.interpreterSettings[0].shebang;
            // it breaks paragraph cloning
            // $scope.commitParagraph($scope.paragraph);
          }
        }
      }).catch(function(res) {
        if (res.status === 401) {
          ngToast.danger({
            content: 'You don\'t have permission on this page',
            verticalPosition: 'bottom',
            timeout: '3000',
          });
          setTimeout(function() {
            window.location = baseUrlSrv.getBase();
          }, 3000);
        }
        console.log('Error %o %o', res.status, res.data ? res.data.message : '');
      });
  };

  $scope.commitParagraph = function(paragraph) {
    paragraph.text = getNoteId($scope.notePath);
    const {
      id,
      title,
      shebang,
      text,
      config,
    } = paragraph;

    return websocketMsgSrv.commitParagraph(id, title, shebang, text, config, {},
      $route.current.pathParams.noteId);
  };

  $scope.moveUp = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    $scope.$emit('moveParagraphUp', paragraph);
  };

  $scope.moveDown = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    $scope.$emit('moveParagraphDown', paragraph);
  };

  $scope.insertNew = function(position) {
    if ($scope.isNoteRunning) {
      return;
    }
    $scope.$emit('insertParagraph', $scope.paragraph.id, position);
  };


  $scope.$on('keyEvent', function(event, keyEvent) {
    if ($scope.paragraphFocused) {
      let keyCode = keyEvent.keyCode;
      let noShortcutDefined = false;

      if (!keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 13) { // Shift + Enter
        $scope.runParagraphFromButton();
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 67) { // Ctrl + Alt + c
        $scope.cancelParagraph($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 68) { // Ctrl + Alt + d
        $scope.removeParagraph($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 75) { // Ctrl + Alt + k
        $scope.moveUp($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 74) { // Ctrl + Alt + j
        $scope.moveDown($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 65) { // Ctrl + Alt + a
        $scope.insertNew('above');
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 66) { // Ctrl + Alt + b
        $scope.insertNew('below');
      } else {
        noShortcutDefined = true;
      }

      if (!noShortcutDefined) {
        keyEvent.preventDefault();
      }
    }
  });

  $scope.$on('updateParagraph', function(event, data) {
    const oldPara = $scope.paragraph;
    const newPara = data.paragraph;
    if (!isUpdateRequired(oldPara, newPara)) {
      return;
    }

    const updateCallback = () => {
      // broadcast `updateResult` message to trigger result update
      if (newPara.results && newPara.results.msg) {
        for (let i in newPara.results.msg) {
          if (newPara.results.msg.hasOwnProperty(i)) {
            const newResult = newPara.results.msg ? newPara.results.msg[i] : {};
            const oldResult = (oldPara.results && oldPara.results.msg)
              ? oldPara.results.msg[i] : {};
            const newConfig = newPara.config.results ? newPara.config.results[i] : {};
            const oldConfig = oldPara.config.results ? oldPara.config.results[i] : {};
            if (!angular.equals(newResult, oldResult) ||
              !angular.equals(newConfig, oldConfig)) {
              $rootScope.$broadcast('updateResult', newResult, newConfig, newPara, parseInt(i));
            }
          }
        }
      }
    };

    $scope.updateParagraph(oldPara, newPara, updateCallback);
  });

  let isEmpty = function(object) {
    return !object;
  };

  $scope.updateParagraph = function(oldPara, newPara, updateCallback) {
     // get status, refreshed
    const statusChanged = (newPara.status !== oldPara.status);
    const resultRefreshed = (newPara.endedAt !== oldPara.endedAt) ||
       isEmpty(newPara.results) !== isEmpty(oldPara.results) ||
       newPara.status === 'ERROR' ||
       (newPara.status === 'FINISHED' && statusChanged);

     // update texts managed by $scope
    if (oldPara.text !== newPara.text) {
      $scope.paragraph.text = newPara.text;
    }

     // update shebang
    $scope.paragraph.shebang = newPara.shebang;

     // execute callback to update result
    updateCallback();

     // update remaining paragraph objects
    $scope.paragraph.user = newPara.user;
    $scope.paragraph.updated = newPara.updated;
    $scope.paragraph.created = newPara.created;
    $scope.paragraph.endedAt = newPara.endedAt;
    $scope.paragraph.startedAt = newPara.startedAt;
    $scope.paragraph.errorMessage = newPara.errorMessage;
    $scope.paragraph.jobName = newPara.jobName;
    $scope.paragraph.title = newPara.title;
    $scope.paragraph.lineNumbers = newPara.lineNumbers;
    $scope.paragraph.status = newPara.status;
    $scope.paragraph.fontSize = newPara.fontSize;
    if (newPara.status !== 'RUNNING') {
      $scope.paragraph.results = newPara.results;
    }
    $scope.paragraph.settings = newPara.settings;

     // handle scroll down by key properly if new paragraph is added
    if (statusChanged || resultRefreshed) {
       // when last paragraph runs, zeppelin automatically appends new paragraph.
       // this broadcast will focus to the newly inserted paragraph
      const paragraphs = angular.element('div[id$="_paragraphColumn_main"]');
      if (paragraphs.length >= 2 && paragraphs[paragraphs.length - 2].id.indexOf($scope.paragraph.id) === 0) {
         // rendering output can took some time. So delay scrolling event firing for sometime.
        setTimeout(() => {
          $rootScope.$broadcast('scrollToCursor');
        }, 500);
      }
    }
  };

  $scope.$on('noteRunningStatus', function(event, status) {
    $scope.isNoteRunning = status;
  });

  $scope.$on('appendParagraphOutput', function(event, data) {
    if (data.paragraphId === $scope.paragraph.id) {
      if (!$scope.paragraph.results) {
        $scope.paragraph.results = {};

        if (!$scope.paragraph.results.msg) {
          $scope.paragraph.results.msg = [];
        }

        $scope.paragraph.results.msg[data.index] = {
          data: data.data,
          type: data.type,
        };

        $rootScope.$broadcast(
          'updateResult',
          $scope.paragraph.results.msg[data.index],
          $scope.paragraph.config.results[data.index],
          $scope.paragraph,
          data.index);
      }
    }
  });

  $scope.$on('setNoteMenu', function(event, notes) {
    $scope.isReloadingNotes = false;
  });

  let getNotePath = function(uuid) {
    let index = _.findIndex($scope.notes.flatList, {'id': uuid});
    if (index > -1) {
      return $scope.notes.flatList[index].path;
    }
    return '';
  };

  let getNoteId = function(path) {
    let index = _.findIndex($scope.notes.flatList, {'path': path});
    if (index > -1) {
      return $scope.notes.flatList[index].id;
    }
    return '';
  };

  $scope.$on('setNoteContent', function(event, note) {
    if (note) {
      $scope.note = note;

      // initialize look And Feel
      $rootScope.$broadcast('setLookAndFeel', 'home');

      // make it read only
      $scope.viewOnly = true;
      $scope.notebookHome = true;
      $scope.staticHome = false;
    } else {
      $scope.staticHome = true;
      $scope.notebookHome = false;
    }

    favoriteNotesService.init();

    favoriteNotesService.filterFavoriteNotes($scope.notes.flatList, (notes) => {
      $scope.favoriteNotes = notes;
    });

    favoriteNotesService.filterRecentNotes($scope.notes.flatList, (notes) => {
      $scope.recentNotes = notes;
    });
  });

  $scope.isFavoriteNote = function(noteId) {
    return favoriteNotesService.noteIsFavorite(noteId);
  };

  $scope.changeNote = function(notePath) {
    $scope.notePath = notePath.startsWith('/') ? notePath : '/' + notePath;
    $scope.commitParagraph($scope.paragraph);
  };

  $scope.changeNoteFavoriteStatus = function(noteId) {
    let current = favoriteNotesService.noteIsFavorite(noteId);
    if (current) {
      favoriteNotesService.removeNoteFromFavorite(noteId);
    } else {
      favoriteNotesService.addNoteToFavorite(noteId);
    }
  };

  $scope.loadMoreNotes = function() {
    $scope.numberOfNotesDisplayed += 10;
  };

  $scope.renameNote = function(nodeId, nodePath) {
    $scope.noteActionService.renameNote(nodeId, nodePath);
  };

  $scope.moveNoteToTrash = function(noteId) {
    $scope.noteActionService.moveNoteToTrash(noteId, false);
  };

  $scope.moveFolderToTrash = function(folderId) {
    $scope.noteActionService.moveFolderToTrash(folderId);
  };

  $scope.restoreNote = function(noteId) {
    websocketMsgSrv.restoreNote(noteId);
  };

  $scope.restoreFolder = function(folderId) {
    websocketMsgSrv.restoreFolder(folderId);
  };

  $scope.restoreAll = function() {
    $scope.noteActionService.restoreAll();
  };

  $scope.renameFolder = function(node) {
    $scope.noteActionService.renameFolder(node.id);
  };

  $scope.removeNote = function(noteId) {
    $scope.noteActionService.removeNote(noteId, false);
  };

  $scope.removeFolder = function(folderId) {
    $scope.noteActionService.removeFolder(folderId);
  };

  $scope.emptyTrash = function() {
    $scope.noteActionService.emptyTrash();
  };

  $scope.clearAllParagraphOutput = function(noteId) {
    $scope.noteActionService.clearAllParagraphOutput(noteId);
  };

  $scope.removeParagraph = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    if ($scope.note.paragraphs.length === 1) {
      BootstrapDialog.alert({
        closable: true,
        message: 'All the paragraphs can\'t be deleted.',
      });
    } else {
      BootstrapDialog.confirm({
        closable: true,
        title: '',
        message: 'Do you want to delete this paragraph?',
        callback: function(result) {
          if (result) {
            console.log('Remove paragraph');
            websocketMsgSrv.removeParagraph(paragraph.id);
            $scope.$emit('moveFocusToNextParagraph', $scope.paragraph.id);
          }
        },
      });
    }
  };

  $scope.isFilterNote = function(note) {
    if (!$scope.query.q) {
      return true;
    }

    let noteName = note.path;
    if (noteName.toLowerCase().indexOf($scope.query.q.toLowerCase()) > -1) {
      return true;
    }
    return false;
  };

  $scope.getNoteName = function(note) {
    return arrayOrderingSrv.getNoteName(note);
  };

  $scope.noteComparator = function(note1, note2) {
    return arrayOrderingSrv.noteComparator(note1, note2);
  };

  /**
   * @returns {boolean} true if updated is needed
   */
  function isUpdateRequired(oldPara, newPara) {
    return (newPara.id === oldPara.id &&
      (newPara.created !== oldPara.created ||
      newPara.shebang !== oldPara.shebang ||
      newPara.text !== oldPara.text ||
      newPara.endedAt !== oldPara.endedAt ||
      newPara.startedAt !== oldPara.startedAt ||
      newPara.user !== oldPara.user ||
      newPara.position !== oldPara.position ||
      newPara.updated !== oldPara.updated ||
      newPara.status !== oldPara.status ||
      newPara.jobName !== oldPara.jobName ||
      newPara.title !== oldPara.title ||
      isEmpty(newPara.results) !== isEmpty(oldPara.results) ||
      newPara.errorMessage !== oldPara.errorMessage ||
      !angular.equals(newPara.settings, oldPara.settings) ||
      !angular.equals(newPara.config, oldPara.config) ||
      !angular.equals(newPara.runtimeInfos, oldPara.runtimeInfos)));
  }
}
