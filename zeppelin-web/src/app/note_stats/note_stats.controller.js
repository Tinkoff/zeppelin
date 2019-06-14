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
import {getJobIconByStatus, getJobColorByStatus, getJobTooltip, getNoteStatus} from './job-status';

angular.module('zeppelinWebApp').controller('NoteStatsCtrl', NoteStatsController);

function NoteStatsController($scope, $rootScope, $http, baseUrlSrv, ngToast) {
  'ngInject';

  ngToast.dismiss();
  $scope.favoriteNoteStats = [];
  $scope.recentNotesStats = [];

  $scope.getJobIconByStatus = getJobIconByStatus;
  $scope.getJobColorByStatus = getJobColorByStatus;
  $scope.getJobTooltip = getJobTooltip;
  $scope.getNoteStatus = getNoteStatus;

  /**
   * Gets available notes stats.
   */
  function getNoteStatsInfo(noteType) {
    $http.get(baseUrlSrv.getRestApiBase() + '/statistics?note_type=' + noteType)
      .success(function(data, status, headers, config) {
        if (noteType === 'favorite') {
          $scope.favoriteNoteStats = data.body;
          processList($scope.favoriteNoteStats);
          console.log('Success %o %o', status, $scope.favoriteNoteStats);
        } else if (noteType === 'recent') {
          $scope.recentNotesStats = data.body;
          processList($scope.recentNotesStats);
          console.log('Success %o %o', status, $scope.recentNotesStats);
        }
      })
      .error(function(data, status, headers, config) {
        showToast(data.message, 'danger');
        console.error('Error %o %o', status, data.message);
      });
  }

  function convertTime(time) {
    return new Date(time.date.year, time.date.month - 1, time.date.day, time.time.hour,
      time.time.minute, time.time.second, Math.floor(time.time.nano / 1000000)
    );
  }

  /**
   * Converts times and computes duration.
   */
  function processList(list) {
    for (let i = 0; i < list.length; i++) {
      for (let j = 0; j < list[i].inner.length; j++) {
        let start = list[i].inner[j].startedAt;
        if (start === undefined) {
          list[i].inner[j].startedAt = 'New Paragraph';
          list[i].inner[j].endedAt = 'New Paragraph';
          continue;
        }
        list[i].inner[j].startedAt = moment(convertTime(start)).format('MMMM DD YYYY, h:mm:ss A');

        let end = list[i].inner[j].endedAt;
        let duration = '';
        if (end === undefined) {
          end = 'Running';
          duration = moment(convertTime(start)).toNow(true);
        } else {
          end = moment(convertTime(end)).format('MMMM DD YYYY, h:mm:ss A');
          duration = moment.utc(moment(end, 'MMMM DD YYYY, h:mm:ss A')
            .diff(moment(list[i].inner[j].startedAt, 'MMMM DD YYYY, h:mm:ss A')))
            .format('HH:mm:ss');
        }
        list[i].inner[j].endedAt = end;
        list[i].inner[j].duration = duration;
      }
    }
  }

  function showToast(message, type) {
    const verticalPosition = 'bottom';
    const timeout = '3000';

    if (type === 'success') {
      ngToast.success({content: message, verticalPosition: verticalPosition, timeout: timeout});
    } else if (type === 'info') {
      ngToast.info({content: message, verticalPosition: verticalPosition, timeout: timeout});
    } else {
      BootstrapDialog.show({
        closable: false,
        closeByBackdrop: false,
        closeByKeyboard: false,
        title: 'Error',
        message: _.escape(message),
        buttons: [{
          // close all the dialogs when there are error on running all paragraphs
          label: 'Close',
          action: function() {
            BootstrapDialog.closeAll();
          },
        }],
      });
    }
  }

  let init = function() {
    getNoteStatsInfo('favorite');
    getNoteStatsInfo('recent');
  };

  init();
}
