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

// noinspection JSUnresolvedVariable
angular.module('zeppelinWebApp').controller('DatetimePickerDFormCtrl', DatetimePickerDFormCtrl);

function DatetimePickerDFormCtrl($scope) {
  let form = null;
  let datetimePickerInputDom = null;

  $scope.controllerInit = function(dForm, pickerInputDomId) {
    form = dForm;
    tryCreateDatetimePicker(pickerInputDomId, 0);
  };

  function tryCreateDatetimePicker(inputDomId, createTryCount) {
    setTimeout(() => {
      // noinspection ES6ModulesDependencies
      datetimePickerInputDom = jQuery(`#${inputDomId}`);
      if (datetimePickerInputDom.length !== 0) {
        let text = datetimePickerInputDom[0].value + '';
        createDatetimePicker();
        // sometimes DatetimePicker drop content. it fix it
        datetimePickerInputDom[0].value = text;
      } else {
        // if something wrong in dom structure stop recursion
        if (createTryCount < 100) {
          setTimeout(() => tryCreateDatetimePicker(inputDomId, createTryCount + 1), 250);
        }
      }
    }, 100);
  }

  let timePatternSymbols = ['h', 'i', 'a', 'A'];

  function createDatetimePicker() {
    let datePattern = form.pattern;
    let enableTimePicker = false;

    // enable or not Time selectors use pattern
    try {
      for (let symbol of datePattern.split('')) {
        enableTimePicker = timePatternSymbols.includes(symbol);
        if (enableTimePicker) {
          break;
        }
      }
    } catch (e) {
      datetimePickerInputDom.value = 'Please set correct datetime pattern!';
    }

    let parameters = {
      timepicker: enableTimePicker,
      timeFormat: ' ',
      keyboardNav: false,
      dateFormat: datePattern,
      toggleSelected: false,
      minutesStep: 5,
      onHide: () => datetimePickerInputDom[0].dispatchEvent(new Event('change')), // call global onChange
    };
    // noinspection JSUnresolvedFunction
    datetimePickerInputDom.datepicker(parameters);
  }

  $scope.onCalendarClick = function() {
    datetimePickerInputDom.focus();
  };
}
