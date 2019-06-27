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

angular.module('zeppelinWebApp').controller('ConfigurationCtrl', ConfigurationCtrl);

function ConfigurationCtrl($scope, $http, baseUrlSrv, ngToast) {
  'ngInject';

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

  $scope.setConfigurationParam = function(index) {
    let value = null;
    let reg = /^-?\d+\.?\d*$/;
    if ($scope.configrations[index]['valueType'] === 'NUMBER') {
      value = document.getElementById(index + '_number_input').value;
      if (!reg.test(value)) {
        alert('Parameter must be number.');
        document.getElementById(index + '_number_input').value
          = $scope.configrations[index]['value'];
        return;
      }
    } else {
      if ($scope.configrations[index]['valueType'] === 'BOOLEAN') {
        value = !!$scope.configrations[index]['value'];
        value = !value;
      } else {
        value = document.getElementById(index + '_input').value;
      }
    }
    $http.post(baseUrlSrv.getRestApiBase() + '/configurations/set?name='
      + $scope.configrations[index]['name'] + '&value='
      + value)
      .then(function(response) {
        if (response.data.status === 'OK') {
          if ($scope.configrations[index]['valueType'] === 'NUMBER') {
            document.getElementById(index + '_number_input').value = value;
          } else {
            if ($scope.configrations[index]['valueType'] === 'BOOLEAN') {
              if (!value) {
                document.getElementById('fa').classList.add('fa-square-o');
                document.getElementById('fa').classList.remove('fa-check-square-o');
              } else {
                document.getElementById('fa').classList.add('fa-check-square-o');
                document.getElementById('fa').classList.remove('fa-square-o');
              }
            } else {
              document.getElementById(index + '_input').value = value;
            }
          }
          $scope.configrations[index]['value'] = value;
        } else {
          if (response.status === 401) {
            ngToast.danger({
              content: 'You don\'t have permission on this page',
              verticalPosition: 'bottom',
              timeout: '3000',
            });
            setTimeout(function() {
              window.location = baseUrlSrv.getBase();
            }, 3000);
          }
          console.log('Error %o %o', status, response.data.message);
        }
      });
  };
  let init = function() {
    $scope.getConfigurations();
  };

  init();
}
