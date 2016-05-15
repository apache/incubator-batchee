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
package org.apache.batchee.container.impl.controller.chunk;

import java.util.HashMap;
import java.util.Map;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.spi.DataRepresentationService;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.api.chunk.CheckpointAlgorithm;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;


public class CheckpointManager {
    private final PersistenceManagerService persistenceManagerService;
    private final DataRepresentationService dataRepresentationService;
    private final ItemReader readerProxy;
    private final ItemWriter writerProxy;
    private final CheckpointAlgorithm checkpointAlgorithm;
    private final String stepId;
    private final long jobInstanceID;


    public CheckpointManager(final ItemReader reader, final ItemWriter writer,
                             final CheckpointAlgorithm chkptAlg,
                             final long jobInstanceID, final String stepId,
                             final PersistenceManagerService persistenceManagerService,
                             final DataRepresentationService dataRepresentationService) {
        this.readerProxy = reader;
        this.writerProxy = writer;
        this.checkpointAlgorithm = chkptAlg;
        this.stepId = stepId;
        this.jobInstanceID = jobInstanceID;

        this.persistenceManagerService = persistenceManagerService;
        this.dataRepresentationService = dataRepresentationService;
    }

    public void beginCheckpoint() {
        try {
            this.checkpointAlgorithm.beginCheckpoint();
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException("Checkpoint algorithm beginCheckpoint() failed", e);
        }
    }

    public void endCheckpoint() {
        try {
            this.checkpointAlgorithm.endCheckpoint();
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException("Checkpoint algorithm endCheckpoint() failed", e);
        }
    }

    public boolean applyCheckPointPolicy() {
        try {
            return checkpointAlgorithm.isReadyToCheckpoint();
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException("Checkpoint algorithm failed", e);
        }
    }

    /**
     * Takes the current checkpoint data from the ItemReader and ItemWriter
     * and store them in the database
     */
    public Map<CheckpointDataKey, CheckpointData> prepareCheckpoints() {
        final CheckpointDataKey readerChkptDK;
        final CheckpointDataKey writerChkptDK;
        Map<CheckpointDataKey, CheckpointData> checkpoints = new HashMap<CheckpointDataKey, CheckpointData>(2);
        try {
            byte[] checkpointBytes = dataRepresentationService.toInternalRepresentation(readerProxy.checkpointInfo());
            CheckpointData readerChkptData = new CheckpointData(jobInstanceID, stepId, CheckpointType.READER);
            readerChkptData.setRestartToken(checkpointBytes);
            readerChkptDK = new CheckpointDataKey(jobInstanceID, stepId, CheckpointType.READER);

            checkpoints.put(readerChkptDK, readerChkptData);

            checkpointBytes = dataRepresentationService.toInternalRepresentation(writerProxy.checkpointInfo());
            CheckpointData writerChkptData = new CheckpointData(jobInstanceID, stepId, CheckpointType.WRITER);
            writerChkptData.setRestartToken(checkpointBytes);
            writerChkptDK = new CheckpointDataKey(jobInstanceID, stepId, CheckpointType.WRITER);

            checkpoints.put(writerChkptDK, writerChkptData);
        } catch (final Exception ex) {
            // is this what I should be throwing here?
            throw new BatchContainerServiceException("Cannot persist the checkpoint data for [" + stepId + "]", ex);
        }

        return checkpoints;
    }

    public void storeCheckPoints(Map<CheckpointDataKey, CheckpointData> checkpoints) {
        try {
            for (Map.Entry<CheckpointDataKey, CheckpointData> checkpointEntry : checkpoints.entrySet()) {
                persistenceManagerService.setCheckpointData(checkpointEntry.getKey(), checkpointEntry.getValue());
            }
        } catch (final Exception ex) {
            throw new BatchContainerServiceException("Cannot persist the checkpoint data for [" + stepId + "]", ex);
        }

    }

    public int checkpointTimeout() {
        try {
            return this.checkpointAlgorithm.checkpointTimeout();
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException("Checkpoint algorithm checkpointTimeout() failed", e);
        }
    }
}
