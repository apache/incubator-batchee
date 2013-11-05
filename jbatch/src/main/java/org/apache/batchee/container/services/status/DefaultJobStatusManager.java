/*
 * Copyright 2012 International Business Machines Corp.
 * 
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.container.services.status;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.container.status.StepStatus;

import javax.batch.runtime.BatchStatus;
import java.util.Properties;

public class DefaultJobStatusManager implements JobStatusManagerService {
    private PersistenceManagerService persistenceManager;

    @Override
    public JobStatus createJobStatus(long jobInstanceId) throws BatchContainerServiceException {
        return persistenceManager.createJobStatus(jobInstanceId);
    }

    @Override
    public JobStatus getJobStatus(final long jobInstanceId) throws BatchContainerServiceException {
        return persistenceManager.getJobStatus(jobInstanceId);
    }

    @Override
    public void updateJobStatus(final JobStatus jobStatus) {
        persistJobStatus(jobStatus.getJobInstanceId(), jobStatus);
    }

    @Override
    public JobStatus getJobStatusFromExecutionId(final long executionId) throws BatchContainerServiceException {
        return persistenceManager.getJobStatusFromExecution(executionId);
    }

    @Override
    public void updateJobBatchStatus(final long jobInstanceId, final BatchStatus batchStatus) throws BatchContainerServiceException {
        final JobStatus js = getJobStatus(jobInstanceId);
        if (js == null) {
            throw new IllegalStateException("Couldn't find entry to update for id = " + jobInstanceId);
        }
        js.setBatchStatus(batchStatus);
        persistJobStatus(jobInstanceId, js);
    }

    @Override
    public void updateJobExecutionStatus(final long jobInstanceId, final BatchStatus batchStatus, final String exitStatus) throws BatchContainerServiceException {
        final JobStatus js = getJobStatus(jobInstanceId);
        if (js == null) {
            throw new IllegalStateException("Couldn't find entry to update for id = " + jobInstanceId);
        }
        js.setBatchStatus(batchStatus);
        js.setExitStatus(exitStatus);
        persistJobStatus(jobInstanceId, js);
    }

    @Override
    public void updateJobCurrentStep(final long jobInstanceId, final String currentStepName) throws BatchContainerServiceException {
        final JobStatus js = getJobStatus(jobInstanceId);
        if (js == null) {
            throw new IllegalStateException("Couldn't find entry to update for id = " + jobInstanceId);
        }
        js.setCurrentStepId(currentStepName);
        persistJobStatus(jobInstanceId, js);
    }


    @Override
    public void updateJobStatusWithNewExecution(final long jobInstanceId, final long newExecutionId) throws BatchContainerServiceException {
        final JobStatus js = getJobStatus(jobInstanceId);
        if (js == null) {
            throw new IllegalStateException("Couldn't find entry to update for id = " + jobInstanceId);
        }
        js.setRestartOn(null);
        js.setLatestExecutionId(newExecutionId);
        js.setBatchStatus(BatchStatus.STARTING);
        persistJobStatus(jobInstanceId, js);
    }

    private void persistJobStatus(long jobInstanceId, JobStatus newJobStatus) throws BatchContainerServiceException {
        persistenceManager.updateJobStatus(jobInstanceId, newJobStatus);
    }

    @Override
    public StepStatus createStepStatus(final long stepExecutionId) throws BatchContainerServiceException {
        return persistenceManager.createStepStatus(stepExecutionId);
    }

    /*
     * @return - StepStatus or null if one is unknown
     */
    @Override
    public StepStatus getStepStatus(final long jobInstanceId, final String stepId) throws BatchContainerServiceException {
        return persistenceManager.getStepStatus(jobInstanceId, stepId);
    }

    @Override
    public void updateStepStatus(final long stepExecutionId, final StepStatus newStepStatus) {
        persistenceManager.updateStepStatus(stepExecutionId, newStepStatus);
    }

    @Override
    public void init(final Properties batchConfig) throws BatchContainerServiceException {
        persistenceManager = ServicesManager.service(PersistenceManagerService.class);
    }

    /*
     * Inefficient, since we've already updated the status to stopped.. would be better to have a single update.
     */
    @Override
    public void updateJobStatusFromJSLStop(final long jobInstanceId, final String restartOn) throws BatchContainerServiceException {
        final JobStatus js = getJobStatus(jobInstanceId);
        if (js == null) {
            throw new IllegalStateException("Couldn't find entry to update for id = " + jobInstanceId);
        }
        js.setRestartOn(restartOn);
        persistJobStatus(jobInstanceId, js);
    }
}
