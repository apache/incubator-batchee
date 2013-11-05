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
package org.apache.batchee.container.status;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.impl.controller.chunk.PersistentDataWrapper;
import org.apache.batchee.container.util.TCCLObjectInputStream;

import javax.batch.runtime.BatchStatus;
import java.io.ByteArrayInputStream;
import java.io.Serializable;

public class StepStatus implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private long stepExecutionId;
    private BatchStatus batchStatus;
    private String exitStatus;
    private int startCount;
    private PersistentDataWrapper persistentUserData;
    private Integer numPartitions;

    private long lastRunStepExecutionId;

    public StepStatus(final long stepExecutionId, final int startCount) {
        this.startCount = startCount;
        this.stepExecutionId = stepExecutionId;
        this.lastRunStepExecutionId = stepExecutionId;
        this.batchStatus = BatchStatus.STARTING;
    }

    public StepStatus(final long stepExecutionId) {
        this(stepExecutionId, 1);
    }

    public void setBatchStatus(BatchStatus batchStatus) {
        this.batchStatus = batchStatus;
    }

    public BatchStatus getBatchStatus() {
        return batchStatus;
    }

    @Override
    public String toString() {
        return ("stepExecutionId: " + stepExecutionId)
            + ",batchStatus: " + batchStatus
            + ",exitStatus: " + exitStatus
            + ",startCount: " + startCount
            + ",persistentUserData: " + persistentUserData
            + ",numPartitions: " + numPartitions;
    }

    public long getStepExecutionId() {
        return stepExecutionId;
    }

    public int getStartCount() {
        return startCount;
    }

    public void incrementStartCount() {
        startCount++;
    }

    public void setExitStatus(String exitStatus) {
        this.exitStatus = exitStatus;
    }

    public String getExitStatus() {
        return exitStatus;
    }

    public void setPersistentUserData(final PersistentDataWrapper persistentUserData) {
        this.persistentUserData = persistentUserData;
    }

    public byte[] getRawPersistentUserData() {
        if (this.persistentUserData != null) {
            return persistentUserData.getPersistentDataBytes();
        }
        return null;
    }

    public Serializable getPersistentUserData() {
        if (this.persistentUserData != null) {
            final byte[] persistentToken = this.persistentUserData.getPersistentDataBytes();
            final ByteArrayInputStream persistentByteArrayInputStream = new ByteArrayInputStream(persistentToken);
            TCCLObjectInputStream persistentOIS;
            Serializable persistentObject;
            try {
                persistentOIS = new TCCLObjectInputStream(persistentByteArrayInputStream);
                persistentObject = Serializable.class.cast(persistentOIS.readObject());
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(e);
            }
            return persistentObject;
        }
        return null;
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public void setStepExecutionId(long stepExecutionId) {
        this.stepExecutionId = stepExecutionId;
        this.lastRunStepExecutionId = this.stepExecutionId;
    }

    public long getLastRunStepExecutionId() {
        return lastRunStepExecutionId;
    }

    public void setLastRunStepExecutionId(long lastRunStepExecutionId) {
        this.lastRunStepExecutionId = lastRunStepExecutionId;
    }

}
