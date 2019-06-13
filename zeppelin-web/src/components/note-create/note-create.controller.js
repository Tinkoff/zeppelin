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

import './note-create.css';

angular.module('zeppelinWebApp').controller('NoteCreateCtrl', NoteCreateCtrl);

function NoteCreateCtrl($scope, noteListFactory, $routeParams, websocketMsgSrv) {
  'ngInject';

  let vm = this;
  vm.clone = false;
  vm.notes = noteListFactory;
  vm.websocketMsgSrv = websocketMsgSrv;
  $scope.note = {};
  $scope.interpreterSettings = {};
  $scope.note.defaultInterpreter = null;

  vm.createNote = function() {
    if (!vm.clone) {
      vm.websocketMsgSrv.createNotebook($scope.note.path);
    } else {
      let noteId = $routeParams.noteId;
      vm.websocketMsgSrv.cloneNote(noteId, $scope.note.path);
    }
  };

  vm.handleNameEnter = function() {
    angular.element('#noteCreateModal').modal('toggle');
    vm.createNote();
  };

  vm.preVisible = function(clone, notePath, relativePath) {
    vm.clone = clone;
    vm.notePath = notePath;
    $scope.note.path = vm.clone ? vm.cloneNoteName() : notePath || vm.newNoteName(relativePath);
    $scope.$apply();
  };

  vm.newNoteName = function(relativePath) {
    let path = relativePath ? '/' + relativePath : `/Users/${$scope.ticket.screenUsername}`;
    path += '/Untitled Note ';
    let newCount = 1;
    angular.forEach(vm.notes.flatList, function(note) {
      let regExp = new RegExp(`^${path}([0-9]+)$`);
      let match = note.path.match(regExp);
      if (match) {
        let lastCount = parseInt(match[1]);
        if (newCount <= lastCount) {
          newCount = lastCount + 1;
        }
      }
    });
    return path + newCount;
  };

  vm.cloneNoteName = function() {
    let copyCount = 1;
    let newCloneName = '';
    let lastIndex = vm.notePath.lastIndexOf(' ');
    let endsWithNumber = !!vm.notePath.match('^.+?\\s\\d$');
    let noteNamePrefix = endsWithNumber ? vm.notePath.substr(0, lastIndex) : vm.notePath;
    let regexp = new RegExp('^' + noteNamePrefix + ' .+');

    angular.forEach(vm.notes.flatList, function(noteName) {
      noteName = noteName.path;
      if (noteName.match(regexp)) {
        let lastCopyCount = noteName.substr(lastIndex).trim();
        newCloneName = noteNamePrefix;
        lastCopyCount = parseInt(lastCopyCount);
        if (copyCount < lastCopyCount) {
          copyCount = lastCopyCount;
        }
      }
    });

    if (!newCloneName) {
      newCloneName = vm.notePath;
    }
    if (!noteNamePrefix.endsWith('Clone')) {
      return vm.notePath + ' Clone';
    }
    return newCloneName + ' ' + (copyCount + 1);
  };

  vm.getInterpreterSettings = function() {
    vm.websocketMsgSrv.getInterpreterSettings();
  };

  $scope.$on('interpreterSettings', function(event, data) {
    $scope.interpreterSettings = data.interpreterSettings;

    // initialize default interpreter with Spark interpreter
    $scope.note.defaultInterpreter = data.interpreterSettings[0];
  });
}
