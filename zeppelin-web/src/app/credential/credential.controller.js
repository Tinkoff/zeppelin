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

angular.module('zeppelinWebApp').controller('CredentialCtrl', CredentialController);

function CredentialController($scope, $rootScope, $http, baseUrlSrv, ngToast) {
  'ngInject';

  ngToast.dismiss();
  $scope.availableCredentials = [];
  $scope.creatingCredentialMode = 'none';


  let flushNewCredential = function() {
    $scope.creatingCredential = {
      id: -1,
      key: 'Z_ENV_CREDENTIAL_',
      value: '',
      owners: [$rootScope.ticket.principal.trim()],
      readers: [$rootScope.ticket.principal.trim()],
      description: '',
    };
  };

  /**
   * Gets configuration for selectors.
   * see https://select2.org/configuration/options-api
   */
  function getSelectConfiguration() {
    let selectJson = {
      tokenSeparators: [',', ' '],
      ajax: {
        url: function(params) {
          if (!params.term) {
            return false;
          }
          return baseUrlSrv.getRestApiBase() + '/security/userlist/' + params.term;
        },
        delay: 250,
        processResults: function(data, params) {
          let results = [];

          if (data.body.users.length !== 0) {
            let users = [];
            for (let len = 0; len < data.body.users.length; len++) {
              users.push({
                'id': data.body.users[len],
                'text': data.body.users[len],
              });
            }
            results.push({
              'text': 'Users :',
              'children': users,
            });
          }
          if (data.body.roles.length !== 0) {
            let roles = [];
            for (let len = 0; len < data.body.roles.length; len++) {
              roles.push({
                'id': data.body.roles[len],
                'text': data.body.roles[len],
              });
            }
            results.push({
              'text': 'Roles :',
              'children': roles,
            });
          }
          return {
            results: results,
            pagination: {
              more: false,
            },
          };
        },
        cache: false,
      },
      // needed to work on bootsrap dialog, see https://stackoverflow.com/a/54100010
      dropdownParent: angular.element('#credentialModal .modal-content'),
      width: ' ',
      tags: true,
      minimumInputLength: 3,
    };
    return selectJson;
  }


  /**
   * Gets available credentials.
   */
  function getCredentialInfo() {
    $http.get(baseUrlSrv.getRestApiBase() + '/credential')
      .success(function(data, status, headers, config) {
        $scope.availableCredentials = data.body;
        console.log('Success %o %o', status, $scope.availableCredentials);
      })
      .error(function(data, status, headers, config) {
        showToast(data.message, 'danger');
        console.error('Error %o %o', status, data.message);
      });
  }

  /**
   * Gets permissions from select2 elements and removes it.
   */
  function getCredentialPermissions() {
    $scope.creatingCredential.owners =
    [...new Set(angular.element('#newCredentialOwners').val())];
    $scope.creatingCredential.readers =
    [...new Set(angular.element('#newCredentialReaders').val())];

    angular.element('.permissionsForm select').find('option:not([is-select2="false"])').remove();
  }

  $scope.setSelectedCredential = function(credential, mode) {
    if(credential === null) {
      flushNewCredential();
    } else {
      $scope.creatingCredential = angular.copy(credential);
    }
    $scope.creatingCredentialMode = mode;
    angular.element('#newCredentialOwners').select2(getSelectConfiguration());
    angular.element('#newCredentialReaders').select2(getSelectConfiguration());
  };


  $scope.addNewCredential = function() {
    getCredentialPermissions();

    $http({
      method: 'PUT',
      url: baseUrlSrv.getRestApiBase() + '/credential',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingCredential,
    })
    .success(function(data, status, headers, config) {
      getCredentialInfo();
      angular.element('#credentialModal').modal('hide');
      flushNewCredential();
      showToast('Successfully saved credentials.', 'success');
      console.log('Success %o %o', status, $scope.availableCredentials);
    })
    .error(function(data, status, headers, config) {
      showToast(data.message, 'danger');
      console.error('Error %o %o', status, data.message);
    });
  };

  $scope.updateCredential = function() {
    getCredentialPermissions();

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/credential',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingCredential,
    })
    .success(function(data, status, headers, config) {
      getCredentialInfo();
      angular.element('#credentialModal').modal('hide');
      flushNewCredential();
      showToast('Successfully saved credentials.', 'success');
      console.log('Success %o %o', status, $scope.availableCredentials);
    })
    .error(function(data, status, headers, config) {
      showToast(data.message, 'danger');
      console.error('Error %o %o', status, data.message);
    });
  };

  $scope.removeCredentialInfo = function(key) {
    BootstrapDialog.confirm({
      closable: false,
      closeByBackdrop: false,
      closeByKeyboard: false,
      title: '',
      message: 'Do you want to delete this credential information?',
      callback: function(result) {
        if (result) {
          $http.delete(baseUrlSrv.getRestApiBase() + '/credential/' + key)
            .success(function(data, status, headers, config) {
              getCredentialInfo();
              console.log('Success %o %o', status, data.message);
            })
            .error(function(data, status, headers, config) {
              showToast(data.message, 'danger');
              console.log('Error %o %o', status, data.message);
            });
        }
      },
    });
  };

  $scope.copyText = function(copyString) {
    const strToClipboard = document.createElement('textarea');
    strToClipboard.value = copyString;
    document.body.appendChild(strToClipboard);
    strToClipboard.select();
    document.execCommand('copy');
    document.body.removeChild(strToClipboard);
    return;
  };

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
    flushNewCredential();
    getCredentialInfo();
  };

  init();
}
