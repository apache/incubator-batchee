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
package org.apache.batchee.spi;

import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.StepExecutionImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.impl.controller.chunk.CheckpointData;
import org.apache.batchee.container.impl.controller.chunk.CheckpointDataKey;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.container.status.StepStatus;

import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public interface PersistenceManagerService extends BatchService {

    public enum TimestampType { CREATE, END, LAST_UPDATED, STARTED }

    /**
     * JOB OPERATOR ONLY METHODS
     */

    int jobOperatorGetJobInstanceCount(String jobName);

    int jobOperatorGetJobInstanceCount(String jobName, String appTag);

    Map<Long, String> jobOperatorGetExternalJobInstanceData();

    List<Long> jobOperatorGetJobInstanceIds(String jobName, int start, int count);

    List<Long> jobOperatorGetJobInstanceIds(String jobName, String appTag, int start, int count);

    Timestamp jobOperatorQueryJobExecutionTimestamp(long key, TimestampType timetype);

    String jobOperatorQueryJobExecutionBatchStatus(long key);

    String jobOperatorQueryJobExecutionExitStatus(long key);

    List<StepExecution> getStepExecutionsForJobExecution(long execid);

    void updateBatchStatusOnly(long executionId, BatchStatus batchStatus, Timestamp timestamp);

    void markJobStarted(long key, Timestamp startTS);

    void updateWithFinalExecutionStatusesAndTimestamps(long key, BatchStatus batchStatus, String exitStatus, Timestamp updatets);

    InternalJobExecution jobOperatorGetJobExecution(long jobExecutionId);

    Properties getParameters(long executionId) throws NoSuchJobExecutionException;

    List<InternalJobExecution> jobOperatorGetJobExecutions(long jobInstanceId);

    Set<Long> jobOperatorGetRunningExecutions(String jobName);

    JobStatus getJobStatusFromExecution(long executionId);

    long getJobInstanceIdByExecutionId(long executionId) throws NoSuchJobExecutionException;

    // JOBINSTANCEDATA

    /**
     * Creates a JobIntance
     *
     * @param name          the job id from job.xml
     * @param apptag        the application tag that owns this job
     * @param jobXml        the resolved job xml
     * @return the job instance
     */
    JobInstance createJobInstance(String name, String apptag, String jobXml);

    // EXECUTIONINSTANCEDATA

    /**
     * Create a JobExecution
     *
     * @param jobInstance   the parent job instance
     * @param jobParameters the parent job instance parameters
     * @param batchStatus   the current BatchStatus
     * @return the RuntimeJobExecution class for this JobExecution
     */
    RuntimeJobExecution createJobExecution(JobInstance jobInstance, Properties jobParameters, BatchStatus batchStatus);

    // STEPEXECUTIONINSTANCEDATA

    /**
     * Create a StepExecution
     *
     * @param jobExecId   the parent JobExecution id
     * @param stepContext the step context for this step execution
     * @return the StepExecution
     */
    StepExecutionImpl createStepExecution(long jobExecId, StepContextImpl stepContext);

    /**
     * Update a StepExecution
     *
     * @param jobExecId   the parent JobExecution id
     * @param stepContext the step context for this step execution
     */
    void updateStepExecution(long jobExecId, StepContextImpl stepContext);


    // JOB_STATUS

    /**
     * Create a JobStatus
     *
     * @param jobInstanceId the parent job instance id
     * @return the JobStatus
     */
    JobStatus createJobStatus(long jobInstanceId);

    /**
     * Get a JobStatus
     *
     * @param instanceId the parent job instance id
     * @return the JobStatus
     */
    JobStatus getJobStatus(long instanceId);

    /**
     * Update a JobStatus
     *
     * @param instanceId the parent job instance id
     * @param jobStatus  the job status to be updated
     */
    void updateJobStatus(long instanceId, JobStatus jobStatus);

    // STEP_STATUS

    /**
     * Create a StepStatus
     *
     * @param stepExecId the parent step execution id
     * @return the StepStatus
     */
    StepStatus createStepStatus(long stepExecId);

    /**
     * Get a StepStatus
     * <p/>
     * The parent job instance id and this step name from the job xml
     * are used to determine if the current step execution have previously run.
     *
     * @param instanceId the parent job instance id
     * @param stepName   the step name
     * @return the StepStatus
     */
    StepStatus getStepStatus(long instanceId, String stepName);

    /**
     * Update a StepStatus
     *
     * @param stepExecutionId the parent step execution id
     * @param stepStatus      the step status to be updated
     */
    void updateStepStatus(long stepExecutionId, StepStatus stepStatus);

    void setCheckpointData(CheckpointDataKey key, CheckpointData value);

    CheckpointData getCheckpointData(CheckpointDataKey key);

    long getMostRecentExecutionId(long jobInstanceId);

    JobInstance createSubJobInstance(String name, String apptag);

    RuntimeFlowInSplitExecution createFlowInSplitExecution(JobInstance jobInstance, BatchStatus batchStatus);

    StepExecution getStepExecutionByStepExecutionId(long stepExecId);

    void cleanUp(final long instanceId);
}
