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

export const JobStatus = {
  PENDING: 'PENDING',
  RUNNING: 'RUNNING',
  DONE: 'DONE',
  ERROR: 'ERROR',
  CANCELED: 'CANCELED',
  ABORTING: 'ABORTING',
  ABORTED: 'ABORTED',
};

export function getJobIconByStatus(jobStatus) {
  if (jobStatus === JobStatus.PENDING) {
    return 'fa fa-hourglass-o';
  } else if (jobStatus === JobStatus.RUNNING) {
    return 'fa fa-spinner';
  } else if (jobStatus === JobStatus.DONE) {
    return 'fa fa-check';
  } else if (jobStatus === JobStatus.ERROR) {
    return 'fa fa-times';
  } else if (jobStatus === JobStatus.CANCELED) {
    return 'fa fa-stop';
  } else if (jobStatus === JobStatus.ABORTING) {
    return 'fa fa-hourglass-half';
  } else if (jobStatus === JobStatus.ABORTED) {
    return 'fa fa-times';
  }
}

export function getJobColorByStatus(jobStatus) {
  if (jobStatus === JobStatus.PENDING) {
    return 'green';
  } else if (jobStatus === JobStatus.RUNNING) {
    return 'green';
  } else if (jobStatus === JobStatus.DONE) {
    return 'green';
  } else if (jobStatus === JobStatus.ERROR) {
    return 'red';
  } else if (jobStatus === JobStatus.CANCELED) {
    return 'orange';
  } else if (jobStatus === JobStatus.ABORTING) {
    return 'orange';
  } else if (jobStatus === JobStatus.ABORTED) {
    return 'orange';
  }
}

export function getJobTooltip(jobStatus) {
  if (jobStatus === JobStatus.PENDING) {
    return 'Paragraph is waiting for execution';
  } else if (jobStatus === JobStatus.RUNNING) {
    return 'Paragraph is running';
  } else if (jobStatus === JobStatus.DONE) {
    return 'Paragraph successfully finished';
  } else if (jobStatus === JobStatus.ERROR) {
    return 'Error occurred during running the paragraph';
  } else if (jobStatus === JobStatus.CANCELED) {
    return 'Paragraph canceled';
  } else if (jobStatus === JobStatus.ABORTING) {
    return 'Paragraph is aborting';
  } else if (jobStatus === JobStatus.ABORTED) {
    return 'Paragraph successfully aborted';
  }
}
