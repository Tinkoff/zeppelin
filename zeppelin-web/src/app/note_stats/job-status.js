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
    return 'Pending';
  } else if (jobStatus === JobStatus.RUNNING) {
    return 'Running';
  } else if (jobStatus === JobStatus.DONE) {
    return 'Success';
  } else if (jobStatus === JobStatus.ERROR) {
    return 'Errored';
  } else if (jobStatus === JobStatus.CANCELED) {
    return 'Canceled';
  } else if (jobStatus === JobStatus.ABORTING) {
    return 'Aborting';
  } else if (jobStatus === JobStatus.ABORTED) {
    return 'Aborted';
  }
}

export function getNoteStatus(note) {
  let isPending = false;
  let isCanceled = false;
  let isAborted = false;
  for (let i = 0; i < note.inner.length; ++i) {
    let jobStatus = note.inner[i].status;

    if (jobStatus === JobStatus.PENDING) {
      isPending = true;
    } else if (jobStatus === JobStatus.RUNNING) {
      return JobStatus.RUNNING;
    } else if (jobStatus === JobStatus.ERROR) {
      return JobStatus.ERROR;
    } else if (jobStatus === JobStatus.CANCELED) {
      isCanceled = true;
    } else if (jobStatus === JobStatus.ABORTING) {
      return JobStatus.ABORTING;
    } else if (jobStatus === JobStatus.ABORTED) {
      isAborted = true;
    }
  }

  if (isPending) {
    return JobStatus.PENDING;
  } else if (isAborted) {
    return JobStatus.ABORTED;
  } else if (isCanceled) {
    return JobStatus.CANCELED;
  } else {
    return JobStatus.DONE;
  }
}
