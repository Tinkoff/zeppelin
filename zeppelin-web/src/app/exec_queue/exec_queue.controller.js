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
angular.module('zeppelinWebApp').controller('ExecQueueCtrl', ExecQueueCtrl);

function ExecQueueCtrl($scope, $rootScope, $http, baseUrlSrv, ngToast) {
  'ngInject';
  ngToast.dismiss();
  $scope.execQueue = {};

  let init = function() {
    getExecQueue();
    console.log($scope.execQueue);
  };

  init();

  /**
   * Gets available Interpreters execution queues.
   */
  function getExecQueue() {
    $http.get(baseUrlSrv.getRestApiBase() + '/interpreter_queue')
      .success(function(data, status, headers, config) {
        $scope.execQueue = data.body;
        processList($scope.execQueue);
        console.log('Success load %o %o', status, $scope.execQueue);
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

  function processList(list) {
    for (let prop in list) {
      if(list.hasOwnProperty(prop)) {
        for (let i = 0; i < list[prop].length; i++) {
          list[prop][i].paragraphPosition++;
          let start = list[prop][i].startedAt;
          list[prop][i].startedAt = moment(convertTime(start)).format('MMMM DD YYYY, h:mm:ss A');
          console.log(list[prop][i].startedAt);
        }
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
}
