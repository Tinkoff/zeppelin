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

import {SpellResult} from '../../spell';
import {isParagraphRunning, ParagraphStatus} from './paragraph.status';
import sqlFormatter from 'svl-tin-sql-formatter';

import moment from 'moment';
import DiffMatchPatch from 'diff-match-patch';

require('moment-duration-format');

const ParagraphExecutor = {
  SPELL: 'SPELL',
  INTERPRETER: 'INTERPRETER',
  NONE: '', /** meaning `DONE` */
};

angular.module('zeppelinWebApp').controller('ParagraphCtrl', ParagraphCtrl);

function ParagraphCtrl($scope, $rootScope, $route, $window, $routeParams, $location,
                       $timeout, $compile, $http, $q, websocketMsgSrv,
                       baseUrlSrv, ngToast, noteVarShareService,
                       heliumService) {
  'ngInject';

  let ANGULAR_FUNCTION_OBJECT_NAME_PREFIX = '_Z_ANGULAR_FUNC_';
  let completionListLength = undefined;
  $rootScope.keys = Object.keys;
  $scope.parentNote = null;
  $scope.paragraph = {};
  $scope.paragraph.results = {};
  $scope.paragraph.results.msg = [];
  $scope.originalText = '';
  $scope.editor = null;
  $scope.cursorPosition = null;
  $scope.diffMatchPatch = new DiffMatchPatch();
  $scope.isNoteRunning = false;
  // duplicates code from interpreter controller
  $scope.interpreterSettings = [];

  // transactional info for spell execution
  $scope.spellTransaction = {
    totalResultCount: 0,
    renderedResultCount: 0,
    propagated: false,
    resultsMsg: [],
    paragraphText: '',
  };

  let searchRanges = [];
  const getCurrentRangeDefault = function() {
    return {id: -1, markerId: -1};
  };
  let currentRange = getCurrentRangeDefault();

  let editorSetting = {};
  let paragraphScope = $rootScope.$new(true, $rootScope);

  // to keep backward compatibility
  $scope.compiledScope = paragraphScope;

  paragraphScope.z = {
    // z.runParagraph('20150213-231621_168813393')
    runParagraph: function(paragraphId) {
      if (paragraphId) {
        let filtered = $scope.parentNote.paragraphs.filter(function(x) {
          return x.id === paragraphId;
        });
        if (filtered.length === 1) {
          let paragraph = filtered[0];
          websocketMsgSrv.runParagraph(paragraph.id, paragraph.title, paragraph.text,
            paragraph.config, paragraph.settings.params);
        } else {
          ngToast.danger({
            content: 'Cannot find a paragraph with id \'' + paragraphId + '\'',
            verticalPosition: 'top',
            dismissOnTimeout: false,
          });
        }
      } else {
        ngToast.danger({
          content: 'Please provide a \'paragraphId\' when calling z.runParagraph(paragraphId)',
          verticalPosition: 'top',
          dismissOnTimeout: false,
        });
      }
    },

    // Example: z.angularBind('my_var', 'Test Value', '20150213-231621_168813393')
    angularBind: function(varName, value, paragraphId) {
      // Only push to server if there paragraphId is defined
      if (paragraphId) {
        websocketMsgSrv.clientBindAngularObject($routeParams.noteId, varName, value, paragraphId);
      } else {
        ngToast.danger({
          content: 'Please provide a \'paragraphId\' when calling ' +
          'z.angularBind(varName, value, \'PUT_HERE_PARAGRAPH_ID\')',
          verticalPosition: 'top',
          dismissOnTimeout: false,
        });
      }
    },

    // Example: z.angularUnBind('my_var', '20150213-231621_168813393')
    angularUnbind: function(varName, paragraphId) {
      // Only push to server if paragraphId is defined
      if (paragraphId) {
        websocketMsgSrv.clientUnbindAngularObject($routeParams.noteId, varName, paragraphId);
      } else {
        ngToast.danger({
          content: 'Please provide a \'paragraphId\' when calling ' +
          'z.angularUnbind(varName, \'PUT_HERE_PARAGRAPH_ID\')',
          verticalPosition: 'top',
          dismissOnTimeout: false});
      }
    },
  };

  let getInterpreterSettings = function() {
    $http.get(baseUrlSrv.getRestApiBase() + '/modules/setting/interpreters')
      .then(function(res) {
        $scope.interpreterSettings = res.data.body;
        // if shebang exists, but interpreter not found - block input
        if ($scope.paragraph.shebang && $scope.interpreterSettings
        && _.findIndex($scope.interpreterSettings, {'shebang': $scope.paragraph.shebang}) === -1) {
          if ($scope.editor) {
            $scope.editor.setReadOnly(true);
          }
        }

        if (!$scope.paragraph.shebang) {
          // set default shebang if interpreter exist, disable editor otherwise.
          if ($scope.interpreterSettings && $scope.interpreterSettings.length > 0) {
            $scope.paragraph.shebang = $scope.interpreterSettings[0].shebang;
            // it breaks paragraph cloning
            // $scope.commitParagraph($scope.paragraph);
          } else {
            if ($scope.editor) {
              $scope.editor.setReadOnly(true);
            }
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

  let angularObjectRegistry = {};

  // Controller init
  $scope.init = function(newParagraph, note) {
    $scope.paragraph = newParagraph;
    $scope.parentNote = note;
    $scope.originalText = angular.copy(newParagraph.text);
    $scope.chart = {};
    $scope.baseMapOption = ['Streets', 'Satellite', 'Hybrid', 'Topo', 'Gray', 'Oceans', 'Terrain'];
    $scope.colWidthOption = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    $scope.fontSizeOption = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
    $scope.paragraphFocused = false;
    if (newParagraph.focus) {
      $scope.paragraphFocused = true;
    }
    if (!$scope.paragraph.config) {
      $scope.paragraph.config = {};
    }

    $scope.isNoteRunning = !!(note && note.hasOwnProperty('info') &&
      note.info.hasOwnProperty('isRunning')
      && note.info.isRunning === true);

    noteVarShareService.put($scope.paragraph.id + '_paragraphScope', paragraphScope);

    initializeDefault($scope.paragraph.config);
    getInterpreterSettings();
    setTimeout(() => parseForm($scope.originalText), 250);
  };

  $scope.$on('noteRunningStatus', function(event, status) {
    $scope.isNoteRunning = status;
    $scope.editor.setReadOnly(status);
  });

  const initializeDefault = function(config) {
    if (!$scope.paragraph.settings) {
      $scope.paragraph.settings = {params: $scope.paragraph.formParams};
    }
    let forms = $scope.paragraph.settings.forms;

    if (!config.colWidth) {
      config.colWidth = 12;
    }

    if (!config.fontSize) {
      config.fontSize = 9;
    }

    if (config.enabled === undefined) {
      config.enabled = true;
    }

    for (let idx in forms) {
      if (forms[idx]) {
        if (forms[idx].options) {
          if (config.runOnSelectionChange === undefined) {
            config.runOnSelectionChange = true;
          }
        }
      }
    }

    if (!config.results) {
      config.results = {};
    }

    if (!config.editorSetting) {
      config.editorSetting = {};
    } else if (config.editorSetting.editOnDblClick) {
      editorSetting.isOutputHidden = config.editorSetting.editOnDblClick;
    }
  };

  const isTabCompletion = function() {
    const completionKey = $scope.paragraph.config.editorSetting.completionKey;
    return completionKey === 'TAB';
  };

  $scope.$on('updateParagraphOutput', function(event, data) {
    if ($scope.paragraph.id === data.paragraphId) {
      if (!$scope.paragraph.results) {
        $scope.paragraph.results = {};
      }
      if (!$scope.paragraph.results.msg) {
        $scope.paragraph.results.msg = [];
      }

      let update = ($scope.paragraph.results.msg[data.index]) ? true : false;

      $scope.paragraph.results.msg[data.index] = {
        data: data.data,
        type: data.type,
      };

      if (update) {
        $rootScope.$broadcast(
          'updateResult',
          $scope.paragraph.results.msg[data.index],
          $scope.paragraph.config.results[data.index],
          $scope.paragraph,
          data.index);
      }
    }
  });

  $scope.getIframeDimensions = function() {
    if ($scope.asIframe) {
      let paragraphid = '#' + $routeParams.paragraphId + '_container';
      let height = angular.element(paragraphid).height();
      return height;
    }
    return 0;
  };

  $scope.$watch($scope.getIframeDimensions, function(newValue, oldValue) {
    if ($scope.asIframe && newValue) {
      let message = {};
      message.height = newValue;
      message.url = $location.$$absUrl;
      $window.parent.postMessage(angular.toJson(message), '*');
    }
  });

  $scope.getEditor = function() {
    return $scope.editor;
  };

  $scope.$watch($scope.getEditor, function(newValue, oldValue) {
    if (!$scope.editor) {
      return;
    }
    if (newValue === null || newValue === undefined) {
      console.log('editor isnt loaded yet, returning');
      return;
    }
    if ($scope.revisionView === true || $scope.isNoteRunning === true) {
      $scope.editor.setReadOnly(true);
    } else {
      $scope.editor.setReadOnly(false);
    }
  });

  let isEmpty = function(object) {
    return !object;
  };

  $scope.isRunning = function(paragraph) {
    return isParagraphRunning(paragraph);
  };

  $scope.cancelParagraph = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    console.log('Cancel %o', paragraph.id);
    websocketMsgSrv.cancelParagraphRun(paragraph.id);
  };

  $scope.propagateSpellResult = function(paragraphId, paragraphTitle,
                                         paragraphText, paragraphResults,
                                         paragraphStatus, paragraphErrorMessage,
                                         paragraphConfig, paragraphSettingsParam,
                                         paragraphDateStarted, paragraphDateFinished) {
    websocketMsgSrv.paragraphExecutedBySpell(
      paragraphId, paragraphTitle,
      paragraphText, paragraphResults,
      paragraphStatus, paragraphErrorMessage,
      paragraphConfig, paragraphSettingsParam,
      paragraphDateStarted, paragraphDateFinished
    );
  };

  $scope.handleSpellError = function(paragraphText, error,
                                     digestRequired, propagated) {
    const errorMessage = error.stack;
    $scope.paragraph.status = ParagraphStatus.ERROR;
    $scope.paragraph.errorMessage = errorMessage;
    console.error('Failed to execute interpret() in spell\n', error);

    if (!propagated) {
      $scope.paragraph.dateFinished = $scope.getFormattedParagraphTime();
    }

    if (!propagated) {
      $scope.propagateSpellResult(
        $scope.paragraph.id, $scope.paragraph.title,
        paragraphText, [], $scope.paragraph.status, errorMessage,
        $scope.paragraph.config, $scope.paragraph.settings.params,
        $scope.paragraph.dateStarted, $scope.paragraph.dateFinished);
    }
  };

  $scope.prepareSpellTransaction = function(resultsMsg, propagated, paragraphText) {
    $scope.spellTransaction.totalResultCount = resultsMsg.length;
    $scope.spellTransaction.renderedResultCount = 0;
    $scope.spellTransaction.propagated = propagated;
    $scope.spellTransaction.resultsMsg = resultsMsg;
    $scope.spellTransaction.paragraphText = paragraphText;
  };

  /**
   * - update spell transaction count and
   * - check transaction is finished based on the result count
   * @returns {boolean}
   */
  $scope.increaseSpellTransactionResultCount = function() {
    $scope.spellTransaction.renderedResultCount += 1;

    const total = $scope.spellTransaction.totalResultCount;
    const current = $scope.spellTransaction.renderedResultCount;
    return total === current;
  };

  $scope.cleanupSpellTransaction = function() {
    const status = ParagraphStatus.FINISHED;
    $scope.paragraph.executor = ParagraphExecutor.NONE;
    $scope.paragraph.status = status;
    $scope.paragraph.results.code = status;

    const propagated = $scope.spellTransaction.propagated;
    const resultsMsg = $scope.spellTransaction.resultsMsg;
    const paragraphText = $scope.spellTransaction.paragraphText;

    if (!propagated) {
      $scope.paragraph.dateFinished = $scope.getFormattedParagraphTime();
    }

    if (!propagated) {
      const propagable = SpellResult.createPropagable(resultsMsg);
      $scope.propagateSpellResult(
        $scope.paragraph.id, $scope.paragraph.title,
        paragraphText, propagable, status, '',
        $scope.paragraph.config, $scope.paragraph.settings.params,
        $scope.paragraph.dateStarted, $scope.paragraph.dateFinished);
    }
  };

  $scope.runParagraphUsingSpell = function(paragraphText,
                                           magic, digestRequired, propagated) {
    $scope.paragraph.status = 'RUNNING';
    $scope.paragraph.executor = ParagraphExecutor.SPELL;
    $scope.paragraph.results = {};
    $scope.paragraph.errorMessage = '';
    if (digestRequired) {
      $scope.$digest();
    }

    try {
      // remove magic from paragraphText
      const splited = paragraphText.split(magic);
      // remove leading spaces
      const textWithoutMagic = splited[1].replace(/^\s+/g, '');

      if (!propagated) {
        $scope.paragraph.dateStarted = $scope.getFormattedParagraphTime();
      }

      // handle actual result message in promise
      heliumService.executeSpell(magic, textWithoutMagic)
        .then((resultsMsg) => {
          $scope.prepareSpellTransaction(resultsMsg, propagated, paragraphText);

          $scope.paragraph.results.msg = resultsMsg;
          $scope.paragraph.config.tableHide = false;
        })
        .catch((error) => {
          $scope.handleSpellError(paragraphText, error,
            digestRequired, propagated);
        });
    } catch (error) {
      $scope.handleSpellError(paragraphText, error,
        digestRequired, propagated);
    }
  };

  $scope.runParagraphUsingBackendInterpreter = function(paragraphText) {
    let selectedText;
    if ($scope.editor) {
      selectedText = $scope.editor.getSelectedText();
    }
    websocketMsgSrv.runParagraph($scope.paragraph.id, $scope.paragraph.title,
      paragraphText, $scope.paragraph.config, $scope.paragraph.settings.params, selectedText);
  };

  $scope.runExplain = function() {
    if ($scope.isNoteRunning || $scope.paragraph.config.editorSetting.language !== 'sql') {
      return;
    }

    let query = $scope.getEditorValue().toLowerCase();
    let postfix = query.endsWith(';') ? '' : '; ';
    let prefix = query.startsWith('explain') ? '' : 'EXPLAIN ';

    if (prefix === '') {
      websocketMsgSrv.runParagraph($scope.paragraph.id, $scope.paragraph.title, '',
        $scope.paragraph.config, $scope.paragraph.settings.params, query + postfix);
    } else {
      websocketMsgSrv.runParagraph($scope.paragraph.id, $scope.paragraph.title, '',
        $scope.paragraph.config, $scope.paragraph.settings.params, prefix + query + postfix + query + postfix);
    }
  };

  $scope.bindBeforeUnload = function() {
    angular.element(window).off('beforeunload');

    let confirmOnPageExit = function(e) {
      // If we haven't been passed the event get the window.event
      e = e || window.event;
      let message = 'Do you want to reload this site?';

      // For IE6-8 and Firefox prior to version 4
      if (e) {
        e.returnValue = message;
      }
      // For Chrome, Safari, IE8+ and Opera 12+
      return message;
    };
    angular.element(window).on('beforeunload', confirmOnPageExit);
  };

  $scope.unBindBeforeUnload = function() {
    angular.element(window).off('beforeunload');
  };

  $scope.saveParagraph = function(paragraph) {
    const dirtyText = paragraph.text;
    if (dirtyText === undefined || dirtyText === $scope.originalText) {
      return;
    }

    $scope.bindBeforeUnload();

    $scope.commitParagraph(paragraph).then(function() {
      $scope.originalText = dirtyText;
      $scope.dirtyText = undefined;
      $scope.unBindBeforeUnload();
    });
  };

  $scope.toggleEnableDisable = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    paragraph.config.enabled = !paragraph.config.enabled;
    $scope.commitParagraph(paragraph);
  };

  /**
   * @param paragraphText to be parsed
   * @param digestRequired true if calling `$digest` is required
   * @param propagated true if update request is sent from other client
   */
  $scope.runParagraph = function(paragraphText, digestRequired, propagated) {
    if (!paragraphText || $scope.isRunning($scope.paragraph)) {
      return;
    }
    const magic = SpellResult.extractMagic(paragraphText);

    if (heliumService.getSpellByMagic(magic)) {
      $scope.runParagraphUsingSpell(paragraphText, magic, digestRequired, propagated);
    } else {
      $scope.runParagraphUsingBackendInterpreter(paragraphText);
    }

    $scope.originalText = angular.copy(paragraphText);
    $scope.dirtyText = undefined;

    if ($scope.paragraph.config.editorSetting.editOnDblClick) {
      closeEditorAndOpenTable($scope.paragraph);
    } else if (editorSetting.isOutputHidden &&
      !$scope.paragraph.config.editorSetting.editOnDblClick) {
      // %md/%angular repl make output to be hidden by default after running
      // so should open output if repl changed from %md/%angular to another
      openEditorAndOpenTable($scope.paragraph);
    }
    editorSetting.isOutputHidden = $scope.paragraph.config.editorSetting.editOnDblClick;
  };

  $scope.runParagraphFromShortcut = function(paragraphText) {
    // passing `digestRequired` as true to update view immediately
    // without this, results cannot be rendered in view more than once
    $scope.commitParagraph($scope.paragraph);
    $scope.runParagraph(paragraphText, true, false);
  };

  $scope.runParagraphFromButton = function() {
    if ($scope.isNoteRunning) {
      return;
    }
    // we come here from the view, so we don't need to call `$digest()`
    $scope.runParagraph($scope.getEditorValue(), false, false);
  };

  $scope.runAllToThis = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    $scope.$emit('runAllAbove', paragraph, true);
  };

  $scope.runAllFromThis = function(paragraph) {
    if ($scope.isNoteRunning) {
      return;
    }
    $scope.$emit('runAllBelowAndCurrent', paragraph, true);
  };

  $scope.runAllToOrFromThis = function(paragraph) {
    BootstrapDialog.show({
      message: 'Run paragraphs:',
      title: '',
      buttons: [{
        label: 'Close',
        action: function(dialog) {
          dialog.close();
        },
      },
      {
        label: 'Run all above',
        cssClass: 'btn-primary',
        action: function(dialog) {
          $scope.$emit('runAllAbove', paragraph, false);
          dialog.close();
        },
      },
      {
        label: 'Run current and all below',
        cssClass: 'btn-primary',
        action: function(dialog) {
          $scope.$emit('runAllBelowAndCurrent', paragraph, false);
          dialog.close();
        },
      }],
    });
  };

  $scope.turnOnAutoRun = function(paragraph) {
    paragraph.config.runOnSelectionChange = !paragraph.config.runOnSelectionChange;
    $scope.commitParagraph(paragraph);
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

  $scope.copyPara = function(position) {
    if ($scope.isNoteRunning) {
      return;
    }
    let editorValue = $scope.getEditorValue();
    if (editorValue) {
      $scope.copyParagraph(editorValue, position);
    }
  };

  $scope.copyParagraph = function(data, position) {
    let newIndex = -1;
    for (let i = 0; i < $scope.note.paragraphs.length; i++) {
      if ($scope.note.paragraphs[i].id === $scope.paragraph.id) {
        // determine position of where to add new paragraph; default is below
        if (position === 'above') {
          newIndex = i;
        } else {
          newIndex = i + 1;
        }
        break;
      }
    }

    if (newIndex < 0 || newIndex > $scope.note.paragraphs.length) {
      return;
    }

    let config = angular.copy($scope.paragraph.config);
    config.editorHide = false;

    websocketMsgSrv.copyParagraph(newIndex, $scope.paragraph.title, data,
      config, $scope.paragraph.settings.params, $scope.paragraph.shebang);
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

  $scope.clearParagraphOutput = function(paragraph) {
    websocketMsgSrv.clearParagraphOutput(paragraph.id);
  };

  $scope.toggleEditor = function(paragraph) {
    if (paragraph.config.editorHide) {
      $scope.openEditor(paragraph);
    } else {
      $scope.closeEditor(paragraph);
    }
  };

  $scope.closeEditor = function(paragraph) {
    console.log('close the note');
    paragraph.config.editorHide = true;
    $scope.commitParagraph(paragraph);
  };

  $scope.openEditor = function(paragraph) {
    console.log('open the note');
    paragraph.config.editorHide = false;
    $scope.commitParagraph(paragraph);
  };

  $scope.closeTable = function(paragraph) {
    console.log('close the output');
    paragraph.config.tableHide = true;
    $scope.commitParagraph(paragraph);
  };

  $scope.openTable = function(paragraph) {
    console.log('open the output');
    paragraph.config.tableHide = false;
    $scope.commitParagraph(paragraph);
  };

  let openEditorAndCloseTable = function(paragraph) {
    manageEditorAndTableState(paragraph, false, true);
  };

  const closeEditorAndOpenTable = function(paragraph) {
    manageEditorAndTableState(paragraph, true, false);
  };

  const openEditorAndOpenTable = function(paragraph) {
    manageEditorAndTableState(paragraph, false, false);
  };

  const manageEditorAndTableState = function(paragraph, hideEditor, hideTable) {
    paragraph.config.editorHide = hideEditor;
    paragraph.config.tableHide = hideTable;
    $scope.commitParagraph(paragraph);
  };

  $scope.showTitle = function(paragraph) {
    paragraph.config.title = true;
    $scope.commitParagraph(paragraph);
  };

  $scope.hideTitle = function(paragraph) {
    paragraph.config.title = false;
    $scope.commitParagraph(paragraph);
  };

  $scope.setTitle = function(paragraph) {
    $scope.commitParagraph(paragraph);
  };

  $scope.showLineNumbers = function(paragraph) {
    if ($scope.editor) {
      paragraph.config.lineNumbers = true;
      $scope.editor.renderer.setShowGutter(true);
      $scope.commitParagraph(paragraph);
    }
  };

  $scope.hideLineNumbers = function(paragraph) {
    if ($scope.editor) {
      paragraph.config.lineNumbers = false;
      $scope.editor.renderer.setShowGutter(false);
      $scope.commitParagraph(paragraph);
    }
  };

  $scope.columnWidthClass = function(n) {
    if ($scope.asIframe) {
      return 'col-md-12';
    } else {
      return 'paragraph-col col-md-' + n;
    }
  };

  $scope.changeColWidth = function(paragraph, width) {
    angular.element('.navbar-right.open').removeClass('open');
    paragraph.config.colWidth = width;
    $scope.$broadcast('paragraphResized', $scope.paragraph.id);
    $scope.commitParagraph(paragraph);
  };

  $scope.changeFontSize = function(paragraph, fontSize) {
    angular.element('.navbar-right.open').removeClass('open');
    if ($scope.editor) {
      $scope.editor.setOptions({
        fontSize: fontSize + 'pt',
      });
      autoAdjustEditorHeight($scope.editor);
      paragraph.config.fontSize = fontSize;
      $scope.commitParagraph(paragraph);
    }
  };

  $scope.toggleOutput = function(paragraph) {
    paragraph.config.tableHide = !paragraph.config.tableHide;
    $scope.commitParagraph(paragraph);
  };

  $scope.setCompletionWidth = function() {
    if ($rootScope.completionLineWidth && $rootScope.completionLineWidth !== -1 && $scope.editor
    && $scope.editor.completer && $scope.editor.completer.popup) {
      let newWidth = $rootScope.completionLineWidth + 150 + 'px';
      if ($scope.editor.completer.popup.container.style.width !== newWidth) {
        $scope.editor.completer.popup.container.style.width = newWidth;
        $scope.editor.completer.popup.resize();
      }
    }
  };

  $scope.beautifySqlCode = function() {
    if ($scope.paragraph.config.editorSetting.language !== 'sql') {
      return;
    }
    $scope.editor.focus();
    let paragraphText = $scope.editor.getValue();
    let regResult = /^(\s*%.+?\s)?([\s\S]*)$/gm.exec(paragraphText);
    let interpreterRow = regResult[1] ? regResult[1] + '\n' : '';
    let script = regResult[2];
    let formattedText = interpreterRow + sqlFormatter.format(script, {maxCharacterPerLine: 128});
    $scope.editor.setValue(formattedText);
    $scope.paragraph.text = formattedText;
  };

  $scope.aceChanged = function(_, editor) {
    let session = editor.getSession();
    let dirtyText = session.getValue();
    $scope.dirtyText = dirtyText;
    if ($scope.dirtyText !== $scope.originalText) {
      if ($scope.collaborativeMode) {
        $scope.saveNote();
      } else {
        $scope.startSaveTimer();
      }
    }
    setParagraphMode(session);
    if ($scope.cursorPosition) {
      editor.moveCursorToPosition($scope.cursorPosition);
      $scope.cursorPosition = null;
    }
    parseForm(dirtyText);
  };


  let formsTextSize = 0;

  function parseForm(text, forcedUpdate) {
    let RUS = '([А-ЯЁа-яё\\w]+)'; // Any latin or russian word
    /* eslint no-invalid-regexp: "error"*/
    let ALL_FORMS_REG = /%FORM([\s\S]*)%FORM/;
    let FORM_NAME = new RegExp('#define ' + RUS + ' ');
    let PROP_AND_VALUE = /(\w+)<([^>]+)>/g;

    try {
      let formsText = text.match(ALL_FORMS_REG);
      // check forms exists
      if (!formsText) {
        setForms({});
        formsTextSize = 0;
        return;
      }
      formsText = formsText[1];

      // stop parsing if no changes
      if (formsTextSize === formsText.length && !forcedUpdate) {
        return;
      }

      setForms({});
      formsTextSize = formsText.length;

      let formsArray = formsText.trim().split('\n');
      formsArray = formsArray.map((formText) => {
        let form = {name: formText.match(FORM_NAME)[1]};
        for (let match; (match = PROP_AND_VALUE.exec(formText)) !== null;) {
          let propName = match[1];
          let propValue = match[2];
          if (propName === 'options') {
            propValue = propValue.split(';').map((option) => {
              return {value: option, displayName: option};
            });
          }
          form[propName] = propValue;
        }
        return form;
      });
      let formMap = {};
      formsArray.forEach((f) => {
        formMap[f.name] = {
          name: f.name,
          type: f.type,
          displayName: f.title,
          options: f.options,
          runOnChange: f.autorun ? f.autorun === 'true' : f.options !== undefined,
          hidden: false,
        };
      });
      setForms(formMap);
    } catch (e) {
      console.log('Parse form error' + e);
      setForms({});
      formsTextSize = 0;
    }
  }

  function setForms(newForms) {
    if (!$scope.paragraph.settings) {
      $scope.paragraph.settings = {params: $scope.paragraph.formParams};
    }
    $scope.paragraph.settings.forms = newForms;
    $scope.$apply();
  }

  $scope.formValueChange = function(changedForm) {
    let forms = $scope.paragraph.settings.forms;
    let params = $scope.paragraph.settings.params;
    let formNames = Object.keys(forms);
    let paramNames = Object.keys(params);

    // delete parameters for removed forms
    for (let paramName of paramNames) {
      if (!formNames.includes(paramName)) {
        delete params[paramName];
      }
    }

    // delete removed checkbox's options
    for (let formName of formNames) {
      let form = forms[formName];
      if (form.type === 'CheckBox') {
        let validValues = form.options.map((option) => option.value);
        params[form.name] = params[form.name].filter((param) => validValues.includes(param));
      }
    }

    $http.put(
      baseUrlSrv.getRestApiBase() + '/notebook/' + $scope.parentNote.databaseId +
      '/paragraph/' + $scope.paragraph.databaseId + '/set_forms_values',
      params
    ).then(() => {
      if (changedForm.runOnChange) {
        $scope.runParagraphFromButton();
      }
    });
  };

  $scope.sendPatch = function() {
    if (!$scope.userHasWritePermission()) {
      return;
    }
    $scope.originalText = $scope.originalText ? $scope.originalText : '';
    let patch = $scope.diffMatchPatch.patch_make($scope.originalText, $scope.dirtyText).toString();
    $scope.originalText = $scope.dirtyText;
    return websocketMsgSrv.patchParagraph($scope.paragraph.id, $route.current.pathParams.noteId, patch);
  };

  $scope.aceLoaded = function(_editor) {
    let langTools = ace.require('ace/ext/language_tools');
    let Range = ace.require('ace/range').Range;

    _editor.$blockScrolling = Infinity;
    $scope.editor = _editor;
    $scope.editor.on('input', $scope.aceChanged);
    if (_editor.container.id !== '{{paragraph.id}}_editor') {
      $scope.editor.renderer.setShowGutter($scope.paragraph.config.lineNumbers);
      $scope.editor.setShowFoldWidgets(false);
      $scope.editor.setHighlightActiveLine(false);
      $scope.editor.getSession().setUseWrapMode(true);
      $scope.editor.setTheme('ace/theme/chrome');
      $scope.editor.setReadOnly($scope.isRunning($scope.paragraph) || $scope.isNoteRunning);
      $scope.editor.setHighlightActiveLine($scope.paragraphFocused);

      if ($scope.paragraphFocused) {
        $scope.editor.focus();
        $scope.goToEnd($scope.editor);
      }

      autoAdjustEditorHeight(_editor);

      let adjustEditorListener = () => autoAdjustEditorHeight(_editor);
      angular.element(window).resize(adjustEditorListener);
      $scope.$on('$destroy', () => angular.element(window).unbind('resize', adjustEditorListener));

      if (navigator.appVersion.indexOf('Mac') !== -1) {
        $scope.editor.setKeyboardHandler('ace/keyboard/emacs');
        $rootScope.isMac = true;
      } else if (navigator.appVersion.indexOf('Win') !== -1 ||
        navigator.appVersion.indexOf('X11') !== -1 ||
        navigator.appVersion.indexOf('Linux') !== -1) {
        $rootScope.isMac = false;
        // not applying emacs key binding while the binding override Ctrl-v. default behavior of paste text on windows.
      }

      $scope.$on('completionListLength', function(event, data) {
        completionListLength = data;
        $scope.setCompletionWidth();
      });

      $scope.$on('callCompletion', function(event, data) {
        if($scope.paragraphFocused) {
          $http({
            method: 'POST',
            url: baseUrlSrv.getRestApiBase() + '/completion/' + $scope.note.id + '/' + $scope.paragraph.id,
            headers: {
              'Content-Type': 'text/plain',
            },
            data: {
              buf: data.buf,
              cursor: data.pos,
            },
          })
            .success(function(data, status, headers, config) {
              $rootScope.$broadcast('completionList', data.body);
            })
            .error(function(err, status, headers, config) {
              console.error('Error %o', err);
            });
        }
      });

      let remoteCompleter = {
        getCompletions: function(editor, session, pos, prefix, callback) {
          let defaultKeywords = new Set();
          // eslint-disable-next-line handle-callback-err

          if (!editor.isFocused()) {
            return;
          }

          pos = session.getTextRange(new Range(0, 0, pos.row, pos.column)).length;
          let buf = session.getValue();

          $rootScope.$broadcast('callCompletion', {buf: buf, pos: pos});

          $scope.$on('completionList', function(event, data) {
            let computeCaption = function(value, meta) {
              let metaLength = meta !== undefined ? meta.length : 0;
              let length = 200;
              let whitespaceLength = 3;
              let ellipses = '...';
              let maxLengthCaption = length - metaLength - whitespaceLength - ellipses.length;
              if (value !== undefined && value.length > maxLengthCaption) {
                return value.substr(0, maxLengthCaption) + ellipses;
              }
              return value;
            };
            if (data) {
              let completions = [];
              $rootScope.completionLineWidth = -1;
              for (let c in data) {
                if (data.hasOwnProperty(c)) {
                  let v = data[c];
                  if (v.meta !== undefined && v.meta === 'keyword' && defaultKeywords.has(v.value.trim())) {
                    continue;
                  }
                  if (v.name !== undefined && getTextWidth(v.name + ' ' + v.meta) > $rootScope.completionLineWidth) {
                    $rootScope.completionLineWidth = getTextWidth(v.name + ' ' + v.meta);
                  }
                  completions.push({
                    name: v.name,
                    value: v.value,
                    meta: v.meta,
                    caption: computeCaption(v.name, v.meta),
                    score: v.score,
                    className: v.meta ? 'iconable_' + v.meta : '',
                    description: v.description,
                  });
                }
              }
              $rootScope.$broadcast('completionListLength', completions.length);
              callback(null, completions);
            }
          });
        },
        getDocTooltip: function(item) {
          if (item.description && item.description !== ''
          && item.description !== item.caption) {
            item.docHTML = [
              '<div id=\"completion_card\" class=\"card\"> <div class=\"card-body\">',
              '<h5 class=\"card-title\">',
              item.caption,
              '</h5>',
              '<p class=\"card-text\">',
              item.description,
              '</p></div></div>',
            ].join('');
          }
        },
      };

      langTools.setCompleters([remoteCompleter]);

      $scope.editor.setOptions({
        fontSize: $scope.paragraph.config.fontSize + 'pt',
        enableBasicAutocompletion: true,
        enableSnippets: false,
        enableLiveAutocompletion: false,
      });

      $scope.editor.on('focus', function() {
        handleFocus(true);
      });

      $scope.editor.on('blur', function() {
        handleFocus(false);
        $scope.saveParagraph($scope.paragraph);
      });

      $scope.editor.getSession().on('change', function(e, editSession) {
        autoAdjustEditorHeight(_editor);
      });

      setParagraphMode($scope.editor.getSession());

      // autocomplete on '.'
      /*
       $scope.editor.commands.on("afterExec", function(e, t) {
       if (e.command.name == "insertstring" && e.args == "." ) {
       var all = e.editor.completers;
       //e.editor.completers = [remoteCompleter];
       e.editor.execCommand("startAutocomplete");
       //e.editor.completers = all;
       }
       });
       */

      // remove binding
      $scope.editor.commands.removeCommand('showSettingsMenu');
      $scope.editor.commands.removeCommand('find');
      $scope.editor.commands.removeCommand('replace');

      let isOption = $rootScope.isMac ? 'option' : 'alt';

      $scope.editor.commands.bindKey('ctrl-' + isOption + '-n.', null);
      $scope.editor.commands.bindKey('ctrl-' + isOption + '-l', null);
      $scope.editor.commands.bindKey('ctrl-' + isOption + '-w', null);
      $scope.editor.commands.bindKey('ctrl-' + isOption + '-a', null);
      $scope.editor.commands.bindKey('ctrl-' + isOption + '-k', null);
      $scope.editor.commands.bindKey('ctrl-' + isOption + '-e', null);
      $scope.editor.commands.bindKey('ctrl-' + isOption + '-t', null);
      $scope.editor.commands.bindKey('ctrl-space', null);

      if ($rootScope.isMac) {
        $scope.editor.commands.bindKey('command-l', null);
      } else {
        $scope.editor.commands.bindKey('ctrl-l', null);
      }

      // autocomplete on 'ctrl+.'
      $scope.editor.commands.bindKey('ctrl-.', 'startAutocomplete');

      // Show autocomplete on tab
      $scope.editor.commands.addCommand({
        name: 'tabAutocomplete',
        bindKey: {
          win: 'tab',
          mac: 'tab',
          sender: 'editor|cli',
        },
        exec: function(env, args, request) {
          let iCursor = $scope.editor.getCursorPosition();
          let currentLine = $scope.editor.session.getLine(iCursor.row);
          let isAllTabs = currentLine.substring(0, iCursor.column - 1).split('').every(function(char) {
            return (char === '\t' || char === ' ');
          });

          // If user has pressed tab on first line char or if isTabCompletion() is false, keep existing behavior
          // If user has pressed tab anywhere in between and editor mode is not %md, show autocomplete
          if (!isAllTabs && iCursor.column && isTabCompletion()) {
            $scope.editor.execCommand('startAutocomplete');
          } else {
            ace.config.loadModule('ace/ext/language_tools', function() {
              $scope.editor.indent();
            });
          }
        },
      });

      let keyBindingEditorFocusAction = function(scrollValue) {
        let numRows = $scope.editor.getSession().getLength();
        let currentRow = $scope.editor.getCursorPosition().row;
        if (currentRow === 0 && scrollValue <= 0) {
          // move focus to previous paragraph
          $scope.$emit('moveFocusToPreviousParagraph', $scope.paragraph.id);
        } else if (currentRow === numRows - 1 && scrollValue >= 0) {
          $scope.$emit('moveFocusToNextParagraph', $scope.paragraph.id);
        } else {
          $scope.scrollToCursor($scope.paragraph.id, scrollValue);
        }
      };

      // handle cursor moves
      $scope.editor.keyBinding.origOnCommandKey = $scope.editor.keyBinding.onCommandKey;
      $scope.editor.keyBinding.onCommandKey = function(e, hashId, keyCode) {
        if ($scope.editor.completer && $scope.editor.completer.activated) { // if autocompleter is active
        } else {
          // fix ace editor focus issue in chrome (textarea element goes to top: -1000px after focused by cursor move)
          if (parseInt(angular.element('#' + $scope.paragraph.id + '_editor > textarea')
              .css('top').replace('px', '')) < 0) {
            let position = $scope.editor.getCursorPosition();
            let cursorPos = $scope.editor.renderer.$cursorLayer.getPixelPosition(position, true);
            angular.element('#' + $scope.paragraph.id + '_editor > textarea').css('top', cursorPos.top);
          }

          let ROW_UP = -1;
          let ROW_DOWN = 1;

          switch (keyCode) {
            case 38:
              if (!e.shiftKey) {
                keyBindingEditorFocusAction(ROW_UP);
              }
              break;
            case 80:
              if (e.ctrlKey && !e.altKey) {
                keyBindingEditorFocusAction(ROW_UP);
              }
              break;
            case 40:
              if (!e.shiftKey) {
                keyBindingEditorFocusAction(ROW_DOWN);
              }
              break;
            case 78:
              if (e.ctrlKey && !e.altKey) {
                keyBindingEditorFocusAction(ROW_DOWN);
              }
              break;
          }
        }
        this.origOnCommandKey(e, hashId, keyCode);
      };
    }
  };

  // ref: https://github.com/ajaxorg/ace/blob/5021d0193d9f2bba5a978d0b1d7a4f73d18ce713/lib/ace/autocomplete.js#L454
  const completionSupportWithoutBackend = function(str) {
    let matches;
    if (str.length > this.filterText && str.lastIndexOf(
        this.filterText, 0) === 0) {
      matches = this.filtered;
    } else {
      matches = this.all;
    }

    this.filterText = str;
    matches = this.filterCompletions(matches, this.filterText);
    matches = matches.sort(function(a, b) {
      return b.exactMatch - a.exactMatch || b.score - a.score;
    });
    let prev = null;

    matches = matches.filter(function(item) {
      let caption = item.snippet || item.caption || item.value;
      if (caption === prev) {
        return false;
      }
      prev = caption;
      return true;
    });
    this.filtered = matches;
  };

  const completionSupportWithBackend = function(str) {
    let matches;
    if (str.length > this.filterText && str.lastIndexOf(
        this.filterText, 0) === 0) {
      matches = this.filtered;
    } else {
      matches = this.all;
    }
    this.filterText = str;
    matches = this.filterCompletions(matches, this.filterText);
    matches = matches.sort(function(a, b) {
      return b.exactMatch - a.exactMatch || b.score - a.score;
    });
    let prev = null;

    matches = matches.filter(function(item) {
      if (!_.isEmpty(item.meta)) {
        if (completionListLength !== 0) {
          return false;
        }
      }
      let caption = item.snippet || item.caption || item.value;
      if (caption === prev) {
        return false;
      }
      prev = caption;
      return true;
    });
    this.filtered = matches;
    completionListLength = undefined;
  };

  const handleFocus = function(focused, isDigestPass) {
    $scope.paragraphFocused = focused;
    if (focused) {
      let filteredList = ace.require('ace/autocomplete').FilteredList;
      if ($scope.paragraph.config.editorSetting.completionSupport) {
        filteredList.prototype.setFilter = completionSupportWithBackend;
      } else {
        filteredList.prototype.setFilter = completionSupportWithoutBackend;
      }
    }

    if ($scope.editor) {
      $scope.editor.setHighlightActiveLine(focused);
    }

    if (isDigestPass === false || isDigestPass === undefined) {
      // Protect against error in case digest is already running
      $timeout(function() {
        // Apply changes since they come from 3rd party library
        $scope.$digest();
      });
    }

    // block editor if there is no interpreters available
    if ($scope.editor && !$scope.editor.getReadOnly() && (!$scope.interpreterSettings
    || $scope.interpreterSettings.length === 0)) {
      $scope.editor.setReadOnly(true);
    }
  };

  let setEditorLanguage = function(session, language) {
    let mode = 'ace/mode/';
    mode += language;
    $scope.paragraph.config.editorMode = mode;
    session.setMode(mode);
  };

  const setParagraphMode = function(session) {
    let index = _.findIndex($scope.interpreterSettings, {'shebang': $scope.paragraph.shebang});
    if (index < 0) {
      return;
    }
    if ($scope.interpreterSettings[index].config.editor.language) {
      setEditorLanguage(session, $scope.interpreterSettings[index].config.editor.language);
      _.merge($scope.paragraph.config.editorSetting,
      $scope.interpreterSettings[index].config.editor);
    }
  };

  const autoAdjustEditorHeight = function(editor) {
    let height =
      editor.getSession().getScreenLength() *
      editor.renderer.lineHeight;

    angular.element('#' + editor.container.id).height(height.toString() + 'px');
    editor.resize();
  };

  $rootScope.$on('scrollToCursor', function(event) {
    // scroll on 'scrollToCursor' event only when cursor is in the last paragraph
    let paragraphs = angular.element('div[id$="_paragraphColumn_main"]');
    if (paragraphs[paragraphs.length - 1].id.indexOf($scope.paragraph.id) === 0) {
      $scope.scrollToCursor($scope.paragraph.id, 0);
    }
  });

  /** scrollToCursor if it is necessary
   * when cursor touches scrollTriggerEdgeMargin from the top (or bottom) of the screen, it autoscroll to place cursor around 1/3 of screen height from the top (or bottom)
   * paragraphId : paragraph that has active cursor
   * lastCursorMove : 1(down), 0, -1(up) last cursor move event
   **/
  $scope.scrollToCursor = function(paragraphId, lastCursorMove) {
    if (!$scope.editor || !$scope.editor.isFocused()) {
      // only make sense when editor is focused
      return;
    }
    let lineHeight = $scope.editor.renderer.lineHeight;
    let headerHeight = 103; // menubar, notebook titlebar
    let scrollTriggerEdgeMargin = 50;

    let documentHeight = angular.element(document).height();
    let windowHeight = angular.element(window).height();  // actual viewport height

    let scrollPosition = angular.element(document).scrollTop();
    let editorPosition = angular.element('#' + paragraphId + '_editor').offset();
    let position = $scope.editor.getCursorPosition();
    let lastCursorPosition = $scope.editor.renderer.$cursorLayer.getPixelPosition(position, true);

    let calculatedCursorPosition = editorPosition.top + lastCursorPosition.top + lineHeight * lastCursorMove;

    let scrollTargetPos;
    if (calculatedCursorPosition < scrollPosition + headerHeight + scrollTriggerEdgeMargin) {
      scrollTargetPos = calculatedCursorPosition - headerHeight - ((windowHeight - headerHeight) / 3);
      if (scrollTargetPos < 0) {
        scrollTargetPos = 0;
      }
    } else if (calculatedCursorPosition > scrollPosition + scrollTriggerEdgeMargin + windowHeight - headerHeight) {
      scrollTargetPos = calculatedCursorPosition - headerHeight - ((windowHeight - headerHeight) * 2 / 3);

      if (scrollTargetPos > documentHeight) {
        scrollTargetPos = documentHeight;
      }
    }

    // cancel previous scroll animation
    let bodyEl = angular.element('body');
    bodyEl.stop();
    bodyEl.finish();

    // scroll to scrollTargetPos
    if (scrollTargetPos) {
      bodyEl.scrollTo(scrollTargetPos, {axis: 'y', interrupt: true, duration: 100});
    }
  };

  $scope.getEditorValue = function() {
    return !$scope.editor ? $scope.paragraph.text : $scope.editor.getValue();
  };

  $scope.getProgress = function() {
    return $scope.currentProgress || 0;
  };

  $scope.getFormattedParagraphTime = () => {
    return moment().toISOString();
  };

  $scope.getExecutionTime = function(pdata) {
    const end = pdata.dateFinished;
    const start = pdata.dateStarted;
    let timeMs = Date.parse(end) - Date.parse(start);
    if (isNaN(timeMs) || timeMs < 0) {
      if ($scope.isResultOutdated(pdata)) {
        return 'outdated';
      }
      return '';
    }

    const durationFormat = moment.duration((timeMs / 1000), 'seconds').format('h [hrs] m [min] s [sec]');
    const endFormat = moment(pdata.dateFinished).format('MMMM DD YYYY, h:mm:ss A');

    let user = (pdata.user === undefined || pdata.user === null) ? 'anonymous' : pdata.user;
    let desc = `Took ${durationFormat}. Last updated by ${user} at ${endFormat}.`;

    if ($scope.isResultOutdated(pdata)) {
      desc += ' (outdated)';
    }

    return desc;
  };

  $scope.getElapsedTime = function(paragraph) {
    return 'Started ' + moment(paragraph.dateStarted).fromNow() + '.';
  };

  $scope.isResultOutdated = function(pdata) {
    if (pdata.dateUpdated !== undefined && Date.parse(pdata.dateUpdated) > Date.parse(pdata.dateStarted)) {
      return true;
    }
    return false;
  };

  $scope.goToEnd = function(editor) {
    editor.navigateFileEnd();
  };

  $scope.parseTableCell = function(cell) {
    if (!isNaN(cell)) {
      if (cell.length === 0 || Number(cell) > Number.MAX_SAFE_INTEGER || Number(cell) < Number.MIN_SAFE_INTEGER) {
        return cell;
      } else {
        return Number(cell);
      }
    }
    let d = moment(cell);
    if (d.isValid()) {
      return d;
    }
    return cell;
  };

  $scope.commitParagraph = function(paragraph) {
    const {
      id,
      title,
      shebang,
      text,
      config,
      settings: {params},
    } = paragraph;

    return websocketMsgSrv.commitParagraph(id, title, shebang, text, config, params,
      $route.current.pathParams.noteId);
  };

  /** Utility function */
  $scope.goToSingleParagraph = function() {
    let noteId = $route.current.pathParams.noteId;
    let redirectToUrl = location.protocol + '//' + location.host + location.pathname + '#/notebook/' + noteId +
      '/paragraph/' + $scope.paragraph.id + '?asIframe';
    $window.open(redirectToUrl);
  };

  $scope.linkToParagraphInContext = function() {
    let noteId = $route.current.pathParams.noteId;
    let paragraphUrl = location.protocol + '//' + location.host + location.pathname + '#/notebook/' + noteId +
      '/?paragraph=' + $scope.paragraph.id;
    $window.prompt('Copy url to clipboard: Ctrl+C, Enter', paragraphUrl);
  };

  $scope.showScrollDownIcon = function(id) {
    let doc = angular.element('#p' + id + '_text');
    if (doc[0]) {
      return doc[0].scrollHeight > doc.innerHeight();
    }
    return false;
  };

  $scope.scrollParagraphDown = function(id) {
    let doc = angular.element('#p' + id + '_text');
    doc.animate({scrollTop: doc[0].scrollHeight}, 500);
    $scope.keepScrollDown = true;
  };

  $scope.showScrollUpIcon = function(id) {
    if (angular.element('#p' + id + '_text')[0]) {
      return angular.element('#p' + id + '_text')[0].scrollTop !== 0;
    }
    return false;
  };

  $scope.scrollParagraphUp = function(id) {
    let doc = angular.element('#p' + id + '_text');
    doc.animate({scrollTop: 0}, 500);
    $scope.keepScrollDown = false;
  };

  $scope.$on('angularObjectUpdate', function(event, data) {
    let noteId = $route.current.pathParams.noteId;
    if (!data.noteId || data.noteId === noteId) {
      let scope;
      let registry;

      if (!data.paragraphId || data.paragraphId === $scope.paragraph.id) {
        scope = paragraphScope;
        registry = angularObjectRegistry;
      } else {
        return;
      }
      let varName = data.angularObject.name;

      if (angular.equals(data.angularObject.object, scope[varName])) {
        // return when update has no change
        return;
      }

      if (!registry[varName]) {
        registry[varName] = {
          interpreterGroupId: data.interpreterGroupId,
          noteId: data.noteId,
          paragraphId: data.paragraphId,
        };
      } else {
        registry[varName].noteId = registry[varName].noteId || data.noteId;
        registry[varName].paragraphId = registry[varName].paragraphId || data.paragraphId;
      }

      registry[varName].skipEmit = true;

      if (!registry[varName].clearWatcher) {
        registry[varName].clearWatcher = scope.$watch(varName, function(newValue, oldValue) {
          console.log('angular object (paragraph) updated %o %o', varName, registry[varName]);
          if (registry[varName].skipEmit) {
            registry[varName].skipEmit = false;
            return;
          }
          websocketMsgSrv.updateAngularObject(
            registry[varName].noteId,
            registry[varName].paragraphId,
            varName,
            newValue,
            registry[varName].interpreterGroupId);
        });
      }
      console.log('angular object (paragraph) created %o', varName);
      scope[varName] = data.angularObject.object;

      // create proxy for AngularFunction
      if (varName.indexOf(ANGULAR_FUNCTION_OBJECT_NAME_PREFIX) === 0) {
        let funcName = varName.substring((ANGULAR_FUNCTION_OBJECT_NAME_PREFIX).length);
        scope[funcName] = function() {
          // eslint-disable-next-line prefer-rest-params
          scope[varName] = arguments;
          // eslint-disable-next-line prefer-rest-params
          console.log('angular function (paragraph) invoked %o', arguments);
        };

        console.log('angular function (paragraph) created %o', scope[funcName]);
      }
    }
  });

  $scope.$on('updateParaInfos', function(event, data) {
    if (data.id === $scope.paragraph.id) {
      $scope.paragraph.runtimeInfos = data.infos;
    }
  });

  $scope.$on('angularObjectRemove', function(event, data) {
    let noteId = $route.current.pathParams.noteId;
    if (!data.noteId || data.noteId === noteId) {
      let scope;
      let registry;

      if (!data.paragraphId || data.paragraphId === $scope.paragraph.id) {
        scope = paragraphScope;
        registry = angularObjectRegistry;
      } else {
        return;
      }

      let varName = data.name;

      // clear watcher
      if (registry[varName]) {
        registry[varName].clearWatcher();
        registry[varName] = undefined;
      }

      // remove scope variable
      scope[varName] = undefined;

      // remove proxy for AngularFunction
      if (varName.indexOf(ANGULAR_FUNCTION_OBJECT_NAME_PREFIX) === 0) {
        let funcName = varName.substring((ANGULAR_FUNCTION_OBJECT_NAME_PREFIX).length);
        scope[funcName] = undefined;
      }
    }
  });

  /**
   * Uses canvas.measureText to compute and return the width of the given text of given font in pixels.
   *
   * @param {String} text The text to be rendered.
   * @param {String} font The css font descriptor that text is to be rendered with (e.g. "bold 14px verdana").
   *
   * @see https://stackoverflow.com/questions/118241/calculate-text-width-with-javascript/21015393#21015393
   */
  function getTextWidth(text) {
    let font = $scope.paragraph.config.fontSize / 0.75 + 'px monaco';
    // re-use canvas object for better performance
    let canvas = document.createElement('canvas');
    let context = canvas.getContext('2d');
    context.font = font;
    let metrics = context.measureText(text);
    return Math.floor(metrics.width);
  }

  /**
   * @returns {boolean} true if updated is needed
   */
  function isUpdateRequired(oldPara, newPara) {
    return (newPara.id === oldPara.id &&
      (newPara.dateCreated !== oldPara.dateCreated ||
      newPara.text !== oldPara.text ||
      newPara.dateFinished !== oldPara.dateFinished ||
      newPara.dateStarted !== oldPara.dateStarted ||
      newPara.dateUpdated !== oldPara.dateUpdated ||
      newPara.status !== oldPara.status ||
      newPara.jobName !== oldPara.jobName ||
      newPara.title !== oldPara.title ||
      isEmpty(newPara.results) !== isEmpty(oldPara.results) ||
      newPara.errorMessage !== oldPara.errorMessage ||
      !angular.equals(newPara.settings, oldPara.settings) ||
      !angular.equals(newPara.config, oldPara.config) ||
      !angular.equals(newPara.runtimeInfos, oldPara.runtimeInfos)));
  }

  $scope.updateAllScopeTexts = function(oldPara, newPara) {
    if (oldPara.text !== newPara.text) {
      if ($scope.dirtyText) {         // check if editor has local update
        if ($scope.dirtyText === newPara.text) {  // when local update is the same from remote, clear local update
          $scope.paragraph.text = newPara.text;
          $scope.dirtyText = undefined;
          $scope.originalText = angular.copy(newPara.text);
        } else { // if there're local update, keep it.
          $scope.paragraph.text = newPara.text;
        }
      } else {
        $scope.paragraph.text = newPara.text;
        $scope.originalText = angular.copy(newPara.text);
      }
    }
  };

  $scope.updateParagraphObjectWhenUpdated = function(newPara) {
    // resize col width
    if ($scope.paragraph.config.colWidth !== newPara.config.colWidth) {
      $scope.$broadcast('paragraphResized', $scope.paragraph.id);
    }

    if ($scope.paragraph.config.fontSize !== newPara.config.fontSize) {
      $rootScope.$broadcast('fontSizeChanged', newPara.config.fontSize);
    }

    /** push the rest */
    $scope.paragraph.aborted = newPara.aborted;
    $scope.paragraph.user = newPara.user;
    $scope.paragraph.dateUpdated = newPara.dateUpdated;
    $scope.paragraph.dateCreated = newPara.dateCreated;
    $scope.paragraph.dateFinished = newPara.dateFinished;
    $scope.paragraph.dateStarted = newPara.dateStarted;
    $scope.paragraph.errorMessage = newPara.errorMessage;
    $scope.paragraph.jobName = newPara.jobName;
    $scope.paragraph.title = newPara.title;
    $scope.paragraph.lineNumbers = newPara.lineNumbers;
    $scope.paragraph.status = newPara.status;
    $scope.paragraph.fontSize = newPara.fontSize;
    if (newPara.status !== ParagraphStatus.RUNNING) {
      $scope.paragraph.results = newPara.results;
    }
    $scope.paragraph.settings = newPara.settings;
    $scope.paragraph.runtimeInfos = newPara.runtimeInfos;
    if ($scope.editor) {
      let isReadOnly = $scope.isRunning(newPara) || $scope.isNoteRunning || !$scope.userHasWritePermission();
      $scope.editor.setReadOnly(isReadOnly);
    }

    if (!$scope.asIframe) {
      $scope.paragraph.config = newPara.config;
      initializeDefault(newPara.config);
    } else {
      newPara.config.editorHide = true;
      newPara.config.tableHide = false;
      $scope.paragraph.config = newPara.config;
    }
  };

  $scope.updateParagraph = function(oldPara, newPara, updateCallback) {
     // can't update on revision view
    if ($scope.revisionView === true) {
      return;
    }

     // get status, refreshed
    const statusChanged = (newPara.status !== oldPara.status);
    const resultRefreshed = (newPara.dateFinished !== oldPara.dateFinished) ||
       isEmpty(newPara.results) !== isEmpty(oldPara.results) ||
       newPara.status === ParagraphStatus.ERROR ||
       (newPara.status === ParagraphStatus.FINISHED && statusChanged);

     // update texts managed by $scope
    $scope.updateAllScopeTexts(oldPara, newPara);

     // update shebang
    $scope.paragraph.shebang = newPara.shebang;

     // execute callback to update result
    updateCallback();

     // update remaining paragraph objects
    $scope.updateParagraphObjectWhenUpdated(newPara);

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

    // 7. update form
    parseForm(newPara.text, true);

    // 8. update form parameters
    $scope.paragraph.settings.params = newPara.formParams;
  };

  /** $scope.$on */

  $scope.$on('runParagraphUsingSpell', function(event, data) {
    const oldPara = $scope.paragraph;
    let newPara = data.paragraph;
    const updateCallback = () => {
      $scope.runParagraph(newPara.text, true, true);
    };

    if (!isUpdateRequired(oldPara, newPara)) {
      return;
    }

    $scope.updateParagraph(oldPara, newPara, updateCallback);
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

  $scope.$on('patchReceived', function(event, data) {
    if (data.paragraphId === $scope.paragraph.id) {
      let patch = data.patch;
      patch = $scope.diffMatchPatch.patch_fromText(patch);
      if (!$scope.paragraph.text || $scope.paragraph.text === undefined) {
        $scope.paragraph.text = '';
      }
      $scope.paragraph.text = $scope.diffMatchPatch.patch_apply(patch, $scope.paragraph.text)[0];
      $scope.originalText = angular.copy($scope.paragraph.text);
      let newPosition = $scope.editor.getCursorPosition();
      if (newPosition && newPosition.row && newPosition.column) {
        $scope.cursorPosition = $scope.editor.getCursorPosition();
      }
    }
  });

  $scope.$on('updateProgress', function(event, data) {
    if (data.id === $scope.paragraph.id) {
      $scope.currentProgress = data.progress;
    }
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

  $scope.$on('keyEvent', function(event, keyEvent) {
    if ($scope.paragraphFocused) {
      let paragraphId = $scope.paragraph.id;
      let keyCode = keyEvent.keyCode;
      let noShortcutDefined = false;
      let editorHide = $scope.paragraph.config.editorHide;

      if (editorHide && (keyCode === 38 || (keyCode === 80 && keyEvent.ctrlKey && !keyEvent.altKey))) { // up
        // move focus to previous paragraph
        $scope.$emit('moveFocusToPreviousParagraph', paragraphId);
      } else if (editorHide && (keyCode === 40 || (keyCode === 78 && keyEvent.ctrlKey && !keyEvent.altKey))) { // down
        // move focus to next paragraph
        // $timeout stops chaining effect of focus propogation
        $timeout(() => $scope.$emit('moveFocusToNextParagraph', paragraphId));
      } else if (!keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 13) { // Shift + Enter
        $scope.runParagraphFromShortcut($scope.getEditorValue());
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 13) { // Ctrl + Shift + Enter
        $scope.runAllToOrFromThis($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 74) { // Ctrl + Shift + j
        $scope.linkToParagraphInContext();
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
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 79) { // Ctrl + Alt + o
        $scope.toggleOutput($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 82) { // Ctrl + Alt + r
        $scope.toggleEnableDisable($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 69) { // Ctrl + Alt + e
        $scope.toggleEditor($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 77) { // Ctrl + Alt + m
        if ($scope.paragraph.config.lineNumbers) {
          $scope.hideLineNumbers($scope.paragraph);
        } else {
          $scope.showLineNumbers($scope.paragraph);
        }
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 189) { // Ctrl + Shift + -
        $scope.changeColWidth($scope.paragraph, Math.max(1, $scope.paragraph.config.colWidth - 1));
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 187) { // Ctrl + Shift + =
        $scope.changeColWidth($scope.paragraph, Math.min(12, $scope.paragraph.config.colWidth + 1));
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 83) { // Ctrl + Shift + S
        $scope.toggleSelection(paragraphId);
        $scope.$apply();
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 84) { // Ctrl + Alt + t
        if ($scope.paragraph.config.title) {
          $scope.hideTitle($scope.paragraph);
        } else {
          $scope.showTitle($scope.paragraph);
        }
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 67) { // Ctrl + Alt + c
        $scope.copyPara('below');
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 76) { // Ctrl + Alt + l
        $scope.clearParagraphOutput($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 87) { // Ctrl + Alt + w
        $scope.goToSingleParagraph();
      } else if (keyEvent.ctrlKey && keyCode === 66) { // Ctrl + b
        $scope.beautifySqlCode();
      } else if (keyEvent.ctrlKey && keyCode === 77) { // Ctrl + m
        $scope.runExplain();
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 70) { // Ctrl + f
        $scope.$emit('toggleSearchBox');
      } else {
        noShortcutDefined = true;
      }

      if (!noShortcutDefined) {
        keyEvent.preventDefault();
      }
    }
  });

  $scope.$on('focusParagraph', function(event, paragraphId, cursorPosRow, cursorPosCol, mouseEvent) {
    if (cursorPosCol === null || cursorPosCol === undefined) {
      cursorPosCol = 0;
    }
    if ($scope.paragraph.id === paragraphId) {
      // focus editor
      if (!$scope.paragraph.config.editorHide) {
        if (!mouseEvent) {
          $scope.editor.focus();
          // move cursor to the first row (or the last row)
          let row;
          if (cursorPosRow >= 0) {
            row = cursorPosRow;
            $scope.editor.gotoLine(row, cursorPosCol);
          } else {
            row = $scope.editor.session.getLength();
            $scope.editor.gotoLine(row, cursorPosCol);
          }
          $scope.scrollToCursor($scope.paragraph.id, cursorPosCol);
        }
      }
      handleFocus(true);
    } else {
      if ($scope.editor !== undefined && $scope.editor !== null) {
        $scope.editor.blur();
      }
      let isDigestPass = true;
      handleFocus(false, isDigestPass);
    }
  });

  $scope.$on('doubleClickParagraph', function(event, paragraphId) {
    if ($scope.paragraph.id === paragraphId && $scope.paragraph.config.editorHide &&
      $scope.paragraph.config.editorSetting.editOnDblClick && $scope.revisionView !== true) {
      let deferred = $q.defer();
      openEditorAndCloseTable($scope.paragraph);
      $timeout(
        $scope.$on('updateParagraph', function(event, data) {
          deferred.resolve(data);
        }
        ), 1000);

      deferred.promise.then(function(data) {
        if ($scope.editor) {
          $scope.editor.focus();
          $scope.goToEnd($scope.editor);
        }
      });
    }
  });

  $scope.$on('openEditor', function(event) {
    $scope.openEditor($scope.paragraph);
  });

  $scope.$on('closeEditor', function(event) {
    $scope.closeEditor($scope.paragraph);
  });

  $scope.$on('openTable', function(event) {
    $scope.openTable($scope.paragraph);
  });

  $scope.$on('closeTable', function(event) {
    $scope.closeTable($scope.paragraph);
  });

  $scope.$on('resultRendered', function(event, paragraphId) {
    if ($scope.paragraph.id !== paragraphId) {
      return;
    }

    /** increase spell result count and return if not finished */
    if (!$scope.increaseSpellTransactionResultCount()) {
      return;
    }

    $scope.cleanupSpellTransaction();
  });

  $scope.$on('fontSizeChanged', function(event, fontSize) {
    if ($scope.editor) {
      $scope.editor.setOptions({
        fontSize: fontSize + 'pt',
      });
    }
  });

  const clearSearchSelection = function() {
    for (let i = 0; i < searchRanges.length; ++i) {
      $scope.editor.session.removeMarker(searchRanges[i].markerId);
    }
    searchRanges = [];
    if (currentRange.id !== -1) {
      $scope.editor.session.removeMarker(currentRange.markerId);
    }
    currentRange = getCurrentRangeDefault();
  };

  $scope.onEditorClick = function() {
    $scope.$emit('editorClicked');
  };

  $scope.$on('unmarkAll', function() {
    clearSearchSelection();
  });

  const markAllOccurrences = function(text) {
    clearSearchSelection();
    if (text === '') {
      return;
    }
    if ($scope.editor.findAll(text) === 0) {
      return;
    }
    let ranges = $scope.editor.selection.getAllRanges();
    $scope.editor.selection.toSingleRange();
    $scope.editor.selection.clearSelection();
    for (let i = 0; i < ranges.length; ++i) {
      let id = $scope.editor.session.addMarker(ranges[i], 'ace_selected-word', 'text');
      searchRanges.push({markerId: id, range: ranges[i]});
    }
  };

  $scope.$on('markAllOccurrences', function(event, text) {
    markAllOccurrences(text);
    if (searchRanges.length > 0) {
      $scope.$emit('occurrencesExists', searchRanges.length);
    }
  });

  $scope.$on('nextOccurrence', function(event, paragraphId) {
    if ($scope.paragraph.id !== paragraphId) {
      return;
    }
    let highlightedRangeExists = currentRange.id !== -1;
    if (highlightedRangeExists) {
      $scope.editor.session.removeMarker(currentRange.markerId);
      currentRange.markerId = -1;
    }
    ++currentRange.id;
    if (currentRange.id >= searchRanges.length) {
      currentRange.id = -1;
      $scope.$emit('noNextOccurrence');
      return;
    }
    currentRange.markerId = $scope.editor.session.addMarker(
      searchRanges[currentRange.id].range, 'ace_selection', 'text');
  });

  $scope.$on('prevOccurrence', function(event, paragraphId) {
    if ($scope.paragraph.id !== paragraphId) {
      return;
    }
    let highlightedRangeExists = currentRange.id !== -1;
    if (highlightedRangeExists) {
      $scope.editor.session.removeMarker(currentRange.markerId);
      currentRange.markerId = -1;
    }
    if (currentRange.id === -1) {
      currentRange.id = searchRanges.length;
    }
    --currentRange.id;
    if (currentRange.id === -1) {
      $scope.$emit('noPrevOccurrence');
      return;
    }
    currentRange.markerId = $scope.editor.session.addMarker(
      searchRanges[currentRange.id].range, 'ace_selection', 'text');
  });

  $scope.$on('replaceCurrent', function(event, from, to) {
    if (currentRange.id === -1) {
      return;
    }
    let indexFromEnd = searchRanges.length - currentRange.id - 1;
    let prevId = currentRange.id;
    $scope.editor.session.removeMarker(currentRange.markerId);
    $scope.editor.session.replace(searchRanges[currentRange.id].range, to);
    markAllOccurrences(from);
    let currentIndex = searchRanges.length - indexFromEnd;
    $scope.$emit('occurrencesCountChanged', currentIndex - prevId - 1);
    currentRange.id = currentIndex;
    if (currentRange.id === searchRanges.length) {
      currentRange.id = -1;
      $scope.$emit('noNextOccurrenceAfterReplace');
    } else {
      currentRange.markerId = $scope.editor.session.addMarker(
        searchRanges[currentRange.id].range, 'ace_selection', 'text');
    }
  });

  $scope.$on('replaceAll', function(event, from, to) {
    clearSearchSelection();
    $scope.editor.replaceAll(to, {needle: from});
  });

  $scope.$on('checkOccurrences', function() {
    if (searchRanges.length > 0) {
      $scope.$emit('occurrencesExists', searchRanges.length);
    }
  });

  $scope.$on('multipleAction', function(event, type, ids, data) {
    if (!ids.has($scope.paragraph.id)) {
      return;
    }

    switch (type) {
      case 'clearOutput':
        $scope.clearParagraphOutput($scope.paragraph);
        break;

      case 'toggleTable':
        $scope.paragraph.config.tableHide = data.toggleTableStatus;
        $scope.commitParagraph($scope.paragraph);
        break;

      case 'toggleEditor':
        if (data.toggleEditorStatus) {
          $scope.openEditor($scope.paragraph);
        } else {
          $scope.closeEditor($scope.paragraph);
        }
        break;

      case 'toggleEnableRun':
        $scope.paragraph.config.enabled = data.toggleEnableRunStatus;
        $scope.commitParagraph($scope.paragraph);
        break;

      case 'delete':
        websocketMsgSrv.removeParagraph($scope.paragraph.id);
        break;
    }
  });
}
