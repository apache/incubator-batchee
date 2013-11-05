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
package org.apache.batchee.container.services;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.container.status.StepStatus;
import org.apache.batchee.spi.BatchService;

import javax.batch.runtime.BatchStatus;

public interface JobStatusManagerService extends BatchService {

    /**
     * This method creates an entry for a new job instance
     */
    JobStatus createJobStatus(long jobInstanceId) throws BatchContainerServiceException;

    void updateJobStatus(JobStatus jobStatus);

    /**
     * Returns the JobStatus for a given jobInstance id
     *
     * @param jobInstanceId
     * @return
     * @throws BatchContainerServiceException
     */
    JobStatus getJobStatus(long jobInstanceId) throws BatchContainerServiceException;

    JobStatus getJobStatusFromExecutionId(long executionId) throws BatchContainerServiceException;

    void updateJobBatchStatus(long jobInstanceId, BatchStatus batchStatus) throws BatchContainerServiceException;

    void updateJobExecutionStatus(long jobInstanceId, BatchStatus batchStatus, String exitStatus) throws BatchContainerServiceException;

    void updateJobStatusFromJSLStop(long jobInstanceId, String restartOn) throws BatchContainerServiceException;

    /*
     * A side effect of this method is that it nulls out the 'restartOn' value from the previous execution gets zeroed out.
     * 
     * Also sets BatchStatus to STARTING
     */
    void updateJobStatusWithNewExecution(long jobInstanceId, long newExecutionId) throws BatchContainerServiceException;


    void updateJobCurrentStep(long jobInstanceId, String currentStepName) throws BatchContainerServiceException;

    /**
     * Creates an entry for the step in the stepstatus table during jobsetup
     *
     * @param stepExecutionId
     * @throws BatchContainerServiceException
     */
    StepStatus createStepStatus(long stepExecutionId) throws BatchContainerServiceException;

    void updateStepStatus(long stepExecutionId, StepStatus newStepStatus) throws BatchContainerServiceException;

    StepStatus getStepStatus(long jobInstanceId, String stepId) throws BatchContainerServiceException;
}
