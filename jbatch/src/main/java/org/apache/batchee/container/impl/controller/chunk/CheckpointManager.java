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

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.proxy.ItemReaderProxy;
import org.apache.batchee.container.proxy.ItemWriterProxy;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.container.services.ServicesManager;

import javax.batch.api.chunk.CheckpointAlgorithm;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

public class CheckpointManager {
    private final PersistenceManagerService persistenceManagerService;
    private final ItemReaderProxy readerProxy;
    private final ItemWriterProxy writerProxy;
    private final CheckpointAlgorithm checkpointAlgorithm;
    private final String stepId;
    private final long jobInstanceID;


    public CheckpointManager(final ItemReaderProxy reader, final ItemWriterProxy writer,
                             final CheckpointAlgorithm chkptAlg,
                             final long jobInstanceID, final String stepId) {
        this.readerProxy = reader;
        this.writerProxy = writer;
        this.checkpointAlgorithm = chkptAlg;
        this.stepId = stepId;
        this.jobInstanceID = jobInstanceID;

        this.persistenceManagerService = ServicesManager.service(PersistenceManagerService.class);
    }

    public boolean applyCheckPointPolicy() {
        try {
            return checkpointAlgorithm.isReadyToCheckpoint();
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException("Checkpoint algorithm failed", e);
        }
    }

    public void checkpoint() {
        final ByteArrayOutputStream readerChkptBA = new ByteArrayOutputStream();
        final ByteArrayOutputStream writerChkptBA = new ByteArrayOutputStream();
        final ObjectOutputStream readerOOS, writerOOS;
        final CheckpointDataKey readerChkptDK, writerChkptDK;
        try {
            readerOOS = new ObjectOutputStream(readerChkptBA);
            readerOOS.writeObject(readerProxy.checkpointInfo());
            readerOOS.close();
            CheckpointData readerChkptData = new CheckpointData(jobInstanceID, stepId, CheckpointType.READER);
            readerChkptData.setRestartToken(readerChkptBA.toByteArray());
            readerChkptDK = new CheckpointDataKey(jobInstanceID, stepId, CheckpointType.READER);

            persistenceManagerService.setCheckpointData(readerChkptDK, readerChkptData);

            writerOOS = new ObjectOutputStream(writerChkptBA);
            writerOOS.writeObject(writerProxy.checkpointInfo());
            writerOOS.close();
            CheckpointData writerChkptData = new CheckpointData(jobInstanceID, stepId, CheckpointType.WRITER);
            writerChkptData.setRestartToken(writerChkptBA.toByteArray());
            writerChkptDK = new CheckpointDataKey(jobInstanceID, stepId, CheckpointType.WRITER);

            persistenceManagerService.setCheckpointData(writerChkptDK, writerChkptData);

        } catch (final Exception ex) {
            // is this what I should be throwing here?
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
