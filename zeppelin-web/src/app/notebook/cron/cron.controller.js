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

angular.module('zeppelinWebApp').controller('CronCtrl', CronCtrl);

function CronCtrl($rootScope, $scope, $http, baseUrlSrv) {
  $scope.$on('initCron', function(event, note) {
    noteId = note.databaseId;
    loadCronConfiguration();
  });

  $scope.$on('updateNote', function() {
    loadCronConfiguration();
  });

  function loadCronConfiguration() {
    $http.get(`${baseUrlSrv.getRestApiBase()}/notebook/${noteId}/cron/`).then((response) => {
      $scope.cron.enable = response.data.body.enable;
      $scope.cron.user = response.data.body.user;
      $scope.cron.roles = response.data.roles;
      $scope.initCron(response.data.body.cron);
    });
  }

  let noteId;
  $scope.ticket = $rootScope.ticket;
  $scope.cron = {
    timePeriod: {
      list: ['Hourly', 'Daily', 'Weekly', 'Monthly', 'Custom'],
      selected: 'Custom',
    },
    daysOfWeek: [
      {checked: false, name: 'Monday'},
      {checked: false, name: 'Tuesday'},
      {checked: false, name: 'Wednesday'},
      {checked: false, name: 'Thursday'},
      {checked: false, name: 'Friday'},
      {checked: false, name: 'Saturday'},
      {checked: false, name: 'Sunday'},
    ],
    selectedHour: '00',
    selectedMinute: '00',
    selectedPeriod: '1',
    selectedDayOfMonth: '1 st',
    timeVisible: false,
    periodVisible: false,
    dayOfMonthVisible: false,
    dayOfWeekVisible: false,
    cronExpressionVisible: true,
    spanText: {
      timePrefix: 'Run note at',
      dayOfMonthPrefix: 'on',
      dayOfMonthPostfix: '',
      periodPrefix: 'day of the month and run every',
      periodPostfix: 'day',
    },
    expression: '',
    enable: false,
    dirtyExpression: '',
    user: 'empty',
    roles: [],
    isValidCronExpression: false,

    // https://gist.github.com/andrew-templeton/ae4126a8efe219b796a3
    cronRegular: '^\\s*($|#|\\w+\\s*=|(\\?|\\*|(?:[0-5]?\\d)(?:(?:-|\/|\\,)(?:[0-5]?\\d))?'+
    '(?:,(?:[0-5]?\\d)(?:(?:-|\/|\\,)(?:[0-5]?\\d))?)*)\\s+(\\?|\\*|(?:[0-5]?\\d)(?:(?:-'+
    '|\/|\\,)(?:[0-5]?\\d))?(?:,(?:[0-5]?\\d)(?:(?:-|\/|\\,)(?:[0-5]?\\d))?)*)\\s+(\\?|\\'+
    '*|(?:[01]?\\d|2[0-3])(?:(?:-|\/|\\,)(?:[01]?\\d|2[0-3]))?(?:,(?:[01]?\\d|2[0-3])(?:'+
    '(?:-|\/|\\,)(?:[01]?\\d|2[0-3]))?)*)\\s+(\\?|\\*|(?:0?[1-9]|[12]\\d|3[01])(?:(?:-|'+
    '\/|\\,)(?:0?[1-9]|[12]\\d|3[01]))?(?:,(?:0?[1-9]|[12]\\d|3[01])(?:(?:-|\/|\\,)(?:0?[1'+
    '-9]|[12]\\d|3[01]))?)*)\\s+(\\?|\\*|(?:[1-9]|1[012])(?:(?:-|\/|\\,)(?:[1-9]|1[012]))'+
    '?(?:L|W)?(?:,(?:[1-9]|1[012])(?:(?:-|\/|\\,)(?:[1-9]|1[012]))?(?:L|W)?)*|\\?|\\*|(?:'+
    'JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(?:(?:-)(?:JAN|FEB|MAR|APR|MAY|JUN|JUL'+
    '|AUG|SEP|OCT|NOV|DEC))?(?:,(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(?:(?:-)'+
    '(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))?)*)\\s+(\\?|\\*|(?:[0-6])(?:(?:-|'+
    '\/|\\,|#)(?:[0-6]))?(?:L)?(?:,(?:[0-6])(?:(?:-|\/|\\,|#)(?:[0-6]))?(?:L)?)*|\\?|\\*|('+
    '?:MON|TUE|WED|THU|FRI|SAT|SUN)(?:(?:-)(?:MON|TUE|WED|THU|FRI|SAT|SUN))?(?:,(?:MON|TUE'+
    '|WED|THU|FRI|SAT|SUN)(?:(?:-)(?:MON|TUE|WED|THU|FRI|SAT|SUN))?)*)(|\\s)+(\\?|\\*|(?:|'+
    '\\d{4})(?:(?:-|\/|\\,)(?:|\\d{4}))?(?:,(?:|\\d{4})(?:(?:-|\/|\\,)(?:|\\d{4}))?)*))$',
  };

  $scope.initCron = function(initExpression) {
    if (!initExpression) {
      return;
    }
    let [
      seconds,
      minutes,
      hours,
      dayOfM,
      month,
      dayOfW,
    ] = initExpression.split(' ');
    $scope.cron.expression = initExpression;
    $scope.cron.dirtyExpression = initExpression;

    // Custom
    if (seconds !== '0' || month !== '*' || minutes === '*' || hours === '*' || minutes.indexOf('/') !== -1
      || [seconds, minutes, hours, dayOfM, month].some(p => p.includes(','))
      || [seconds, minutes, hours, dayOfM, month, dayOfW].some(p => p.includes('-'))) {
      $scope.cron.timePeriod.selected = 'Custom';

    // Monthly
    } else if (dayOfM !== '*' && dayOfM !== '?' && dayOfM.indexOf('/') === -1 && dayOfW === '?'
      && hours.indexOf('/') === -1) {
      $scope.cron.timePeriod.selected = 'Monthly';
      $scope.cron.selectedDayOfMonth = $scope.generateDayOfMonth(dayOfM);

    // Weekly
    } else if (dayOfW !== '?' && dayOfW !== '*' && dayOfM === '?' && hours.indexOf('/') === -1) {
      $scope.cron.timePeriod.selected = 'Weekly';
      let cronDays = dayOfW.split(',');
      $scope.cron.daysOfWeek.forEach((day) => {
        day.checked = cronDays.includes(day.name.substr(0, 3).toUpperCase());
      });

    // Daily
    } else if (dayOfW === '?' && dayOfM.indexOf('/') !== -1 && hours.indexOf('/') === -1) {
      $scope.cron.timePeriod.selected = 'Daily';
      $scope.cron.selectedDayOfMonth = $scope.generateDayOfMonth(dayOfM.split('/')[0]);
      $scope.cron.selectedPeriod = dayOfM.split('/')[1];
      if (31 - parseInt(dayOfM.split('/')[0]) < parseInt($scope.cron.selectedPeriod)) {
        $scope.cron.timePeriod.selected = 'Custom';
      }

    // Hourly
    } else if (dayOfW === '?' && dayOfM === '*' && (hours === '23' || hours.indexOf('/') !== -1)) {
      $scope.cron.timePeriod.selected = 'Hourly';
      $scope.cron.selectedPeriod = hours.split('/')[1];
      if (23 - parseInt(hours.split('/')[0]) < parseInt($scope.cron.selectedPeriod)) {
        $scope.cron.timePeriod.selected = 'Custom';
      }

    // else Custom
    } else {
      $scope.cron.timePeriod.selected = 'Custom';
    }

    if ($scope.cron.timePeriod.selected !== 'Custom') {
      $scope.cron.selectedMinute = minutes.length === 1 ? '0' + minutes : minutes;
      let h = hours.split('/')[0];
      $scope.cron.selectedHour = h.length === 1 ? '0' + h : h;
    } else {
      $scope.verifyCustomExpression(initExpression);
    }

    $scope.cronPeriodTypeChange($scope.cron.timePeriod.selected);
  };

  $scope.getAvailablePeriods = function() {
    let timeLeft = 0;
    switch ($scope.cron.timePeriod.selected) {
      case 'Hourly':
        timeLeft = 23 - parseInt($scope.cron.selectedHour);
        break;
      case 'Daily':
        timeLeft = 31 - parseInt($scope.cron.selectedDayOfMonth);
        break;
    }
    let periodList = [];
    for (let i = 1; i <= timeLeft; i++) {
      periodList.push(i);
    }

    // check valid selected period
    if (periodList.length !== 0) {
      let last = periodList[periodList.length - 1];
      if (parseInt($scope.cron.selectedPeriod) === 0) {
        $scope.cron.selectedPeriod = '1';
      }
      if (parseInt($scope.cron.selectedPeriod) > last) {
        $scope.cron.selectedPeriod = '' + last;
      }
    } else {
      periodList.push('0');
      $scope.cron.selectedPeriod = '0';
    }

    return periodList;
  };

  $scope.generateDayOfMonth = function(day) {
    let n = parseInt(day);
    if (n > 3) {
      return n + ' th';
    }
    return ['1 st', '2 nd', '3 rd'][n - 1];
  };

  $scope.cronPeriodTypeChange = function(period) {
    let span = $scope.cron.spanText;
    span.dayOfMonthPostfix = '';
    $scope.cron.timeVisible = false;
    $scope.cron.dayOfMonthVisible = false;
    $scope.cron.periodVisible = false;
    $scope.cron.dayOfWeekVisible = false;
    $scope.cron.cronExpressionVisible = false;

    switch (period) {
      case 'Hourly':
        span.timePrefix = 'Run note every day at';
        span.periodPrefix = 'and every';
        span.periodPostfix = 'hour(s) after it.';
        $scope.cron.timeVisible = true;
        $scope.cron.periodVisible = true;
        break;

      case 'Daily':
        span.timePrefix = 'Run note at';
        span.periodPrefix = 'day of the month and every';
        span.periodPostfix = 'day(s) after it.';
        $scope.cron.timeVisible = true;
        $scope.cron.dayOfMonthVisible = true;
        $scope.cron.periodVisible = true;
        break;

      case 'Weekly':
        span.timePrefix = 'Run note at';
        $scope.cron.timeVisible = true;
        $scope.cron.dayOfWeekVisible = true;
        break;

      case 'Monthly':
        span.timePrefix = 'Run note at';
        span.dayOfMonthPostfix = 'day of the month.';
        $scope.cron.timeVisible = true;
        $scope.cron.dayOfMonthVisible = true;
        break;

      case 'Custom':
        $scope.cron.cronExpressionVisible = true;
        break;
    }

    $scope.generateCronExpression();
  };

  $scope.generateCronExpression = function() {
    if ($scope.cron.timePeriod.selected === 'Custom') {
      return;
    }
    let hour = parseInt($scope.cron.selectedHour);
    let minute = parseInt($scope.cron.selectedMinute);
    let period = parseInt($scope.cron.selectedPeriod);
    let dayOfMonth = parseInt($scope.cron.selectedDayOfMonth);

    let exp = '0 min hour dayOfM * dayOfW';
    exp = exp.replace('min', minute);

    $scope.cron.isValidCronExpression = true;

    switch ($scope.cron.timePeriod.selected) {
      case 'Hourly':
        exp = exp.replace('hour', hour + (period === 0 ? '' : '/' + period));
        break;

      case 'Daily':
        exp = exp.replace('hour', hour);
        exp = exp.replace('dayOfM',
          dayOfMonth + (period === 0 ? '' : '/' + period));
        break;

      case 'Weekly': {
        exp = exp.replace('hour', hour);
        let daysList = $scope.cron.daysOfWeek.filter((day) => day.checked).map(
          (day) => day.name.substr(0, 3).toUpperCase());
        if (daysList.length === 0) {
          $scope.cron.isValidCronExpression = false;
        }
        exp = exp.replace('dayOfW', daysList.join(','));
        break;
      }

      case 'Monthly':
        exp = exp.replace('hour', hour);
        exp = exp.replace('dayOfM', dayOfMonth);
        break;
    }

    // if no replace before
    if (exp.contains('dayOfW')) {
      exp = exp.replace('dayOfW', '?');
      exp = exp.replace('dayOfM', '*');
    } else {
      exp = exp.replace('dayOfM', '?');
    }

    $scope.cron.dirtyExpression = exp;
  };

  $scope.verifyCustomExpression = function(exp) {
    if (!exp || exp.trim().length === 0) {
      $scope.cron.isValidCronExpression = false;
      return;
    }
    if (exp.search($scope.cron.cronRegular) === -1) {
      $scope.cron.isValidCronExpression = false;
      return;
    }
    $http.get(
      `${baseUrlSrv.getRestApiBase()}/cron/check_valid?cronExpression=` + exp)
    .then(function(response) {
      if (response.data.status === 'OK') {
        $scope.cron.isValidCronExpression = response.data.message === 'valid';
      }
    });
  };

  $scope.confirmNewExpression = function() {
    if ($scope.cron.isValidCronExpression) {
      $scope.updateCron($scope.cron.dirtyExpression, true);
    }
  };

  $scope.setCurrentUserAsOwner = function() {
    $http.put(
      `${baseUrlSrv.getRestApiBase()}/notebook/${noteId}/cron`,
      {
        expression: null,
        enable: null,
        doUpdateUser: true,
      }
    );
  };

  $scope.removeCron = function() {
    $http.delete(`${baseUrlSrv.getRestApiBase()}/notebook/${noteId}/cron/`)
    .then((response) => {
      if(response.data.status === 'OK') {
        $scope.cron.expression = '';
        $scope.cron.enable = false;
        $scope.cron.dirtyExpression = '';
        $scope.cron.user = '';
        $scope.cron.roles = [];
        $scope.cron.isValidCronExpression = false;
      }
    });
  };

  $scope.updateCron = function(expression, isEnable) {
    $http.put(
      `${baseUrlSrv.getRestApiBase()}/notebook/${noteId}/cron`,
      {
        expression: expression,
        enable: isEnable,
        doUpdateUser: false,
      }
    ).then((response) => {
      if (response.data.status === 'OK') {
        $scope.cron.expression = response.data.body.newCronExpression;
        $scope.cron.enable = response.data.body.enable;
        $scope.$emit('updateCron', noteId, $scope.cron.expression, $scope.cron.enable);
      }
    });
  };
}
