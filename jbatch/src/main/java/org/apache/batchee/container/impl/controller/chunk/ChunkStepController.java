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
import org.apache.batchee.container.impl.MetricImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.controller.SingleThreadedStepController;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.container.util.TCCLObjectInputStream;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.spi.BatchArtifactFactory;
import org.apache.batchee.spi.DataRepresentationService;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.api.chunk.CheckpointAlgorithm;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.api.chunk.listener.ChunkListener;
import javax.batch.api.chunk.listener.ItemProcessListener;
import javax.batch.api.chunk.listener.ItemReadListener;
import javax.batch.api.chunk.listener.ItemWriteListener;
import javax.batch.api.chunk.listener.RetryProcessListener;
import javax.batch.api.chunk.listener.RetryReadListener;
import javax.batch.api.chunk.listener.RetryWriteListener;
import javax.batch.api.chunk.listener.SkipProcessListener;
import javax.batch.api.chunk.listener.SkipReadListener;
import javax.batch.api.chunk.listener.SkipWriteListener;
import javax.batch.operations.BatchRuntimeException;
import javax.batch.runtime.BatchStatus;
import javax.transaction.Status;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChunkStepController extends SingleThreadedStepController {

    private final static Logger logger = Logger.getLogger(ChunkStepController.class.getName());

    protected static final int DEFAULT_TRAN_TIMEOUT_SECONDS = 180;  // From the spec Sec. 9.7

    private final PersistenceManagerService persistenceManagerService;
    private final BatchArtifactFactory artifactFactory;
    private final DataRepresentationService dataRepresentationService;

    private Chunk chunk = null;
    private ItemReader readerProxy = null;
    private ItemProcessor processorProxy = null;
    private ItemWriter writerProxy = null;
    private CheckpointManager checkpointManager;
    private SkipHandler skipHandler = null;
    private CheckpointDataKey readerChkptDK = null;
    private CheckpointDataKey writerChkptDK = null;
    private List<ChunkListener> chunkListeners = null;
    private List<ItemReadListener> itemReadListeners = null;
    private List<ItemProcessListener> itemProcessListeners = null;
    private List<ItemWriteListener> itemWriteListeners = null;
    private RetryHandler retryHandler;

    protected ChunkStatus currentChunkStatus;
    protected SingleItemStatus currentItemStatus;

    // Default is item-based policy
    protected boolean customCheckpointPolicy = false;
    protected Integer checkpointAtThisItemCount = null;  // Default to spec value elsewhere.

    protected int stepPropertyTranTimeoutSeconds = DEFAULT_TRAN_TIMEOUT_SECONDS;

    public ChunkStepController(final RuntimeJobExecution jobExecutionImpl, final Step step, final StepContextImpl stepContext,
                               final long rootJobExecutionId, final BlockingQueue<PartitionDataWrapper> analyzerStatusQueue,
                               final ServicesManager servicesManager) {
        super(jobExecutionImpl, step, stepContext, rootJobExecutionId, analyzerStatusQueue, servicesManager);
        this.persistenceManagerService = servicesManager.service(PersistenceManagerService.class);
        this.artifactFactory = servicesManager.service(BatchArtifactFactory.class);
        this.dataRepresentationService = servicesManager.service(DataRepresentationService.class);
    }

    /**
     * Utility Class to hold status for a single item as the read-process portion of
     * the chunk loop interact.
     */
    private class SingleItemStatus  {

        public boolean isSkipped() {
            return skipped;
        }

        public void setSkipped(boolean skipped) {
            this.skipped = skipped;
        }

        public boolean isFiltered() {
            return filtered;
        }

        public void setFiltered(boolean filtered) {
            this.filtered = filtered;
        }

        private boolean skipped = false;
        private boolean filtered = false;
    }

    private enum ChunkStatusType {
        NORMAL, RETRY_AFTER_ROLLBACK
    }

    /**
     * Utility Class to hold status for the chunk as a whole.
     *
     * One key usage is to maintain the state reflecting the sequence in which
     * we catch a retryable exception, rollback the previous chunk, process 1-item-at-a-time
     * until we reach "where we left off", then revert to normal chunk processing.
     *
     * Another usage is simply to communicate that the reader readItem() returned 'null', so
     * we're done the chunk.
     */
    private class ChunkStatus {
        ChunkStatusType type;

        ChunkStatus() {
            this(ChunkStatusType.NORMAL);
        }

        ChunkStatus(ChunkStatusType type) {
            this.type = type;
        }

        public boolean isRetryingAfterRollback() {
            return type == ChunkStatusType.RETRY_AFTER_ROLLBACK;
        }

        public boolean wasMarkedForRollbackWithRetry() {
            return markedForRollbackWithRetry;
        }

        public Exception getRetryableException() {
            return retryableException;
        }

        public void markForRollbackWithRetry(Exception retryableException) {
            this.markedForRollbackWithRetry = true;
            this.retryableException = retryableException;
        }

        public int getItemsTouchedInCurrentChunk() {
            return itemsTouchedInCurrentChunk;
        }

        public void incrementItemsTouchedInCurrentChunk() {
            this.itemsTouchedInCurrentChunk++;
        }

        public int getItemsToProcessOneByOneAfterRollback() {
            return itemsToProcessOneByOneAfterRollback;
        }

        public void setItemsToProcessOneByOneAfterRollback(
                int itemsToProcessOneByOneAfterRollback) {
            this.itemsToProcessOneByOneAfterRollback = itemsToProcessOneByOneAfterRollback;
        }

        public boolean isFinished() {
            return finished;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
        }

        private boolean finished = false;
        private Exception retryableException = null;
        private boolean markedForRollbackWithRetry = false;
        private int itemsTouchedInCurrentChunk = 0;
        private int itemsToProcessOneByOneAfterRollback = 0; // For retry with rollback
    }

    /**
     * We read and process one item at a time but write in chunks (group of
     * items). So, this method loops until we either reached the end of the
     * reader (not more items to read), or the writer buffer is full or a
     * checkpoint is triggered.
     *
     * @return an array list of objects to write
     */
    private List<Object> readAndProcess() {
        List<Object> chunkToWrite = new ArrayList<Object>();
        Object itemRead;
        Object itemProcessed;
        int readProcessedCount = 0;

        while (true) {
            currentItemStatus = new SingleItemStatus();
            currentChunkStatus.incrementItemsTouchedInCurrentChunk();
            itemRead = readItem();

            if (currentChunkStatus.wasMarkedForRollbackWithRetry()) {
                break;
            }

            if (!currentItemStatus.isSkipped() && !currentChunkStatus.isFinished()) {
                itemProcessed = processItem(itemRead);

                if (currentChunkStatus.wasMarkedForRollbackWithRetry()) {
                    break;
                }

                if (!currentItemStatus.isSkipped() && !currentItemStatus.isFiltered()) {
                    chunkToWrite.add(itemProcessed);
                }
            }

            // Break out of the loop to deliver one-at-a-time processing after rollback.
            // No point calling isReadyToCheckpoint(), we know we're done.  Let's not
            // complicate the checkpoint algorithm to hold this logic, just break right here.
            if (currentChunkStatus.isRetryingAfterRollback()) {
                break;
            }

            // write buffer size reached
            // This will force the current item to finish processing on a stop request
            if (stepContext.getBatchStatus().equals(BatchStatus.STOPPING)) {
                currentChunkStatus.setFinished(true);
            }

            // The spec, in Sec. 11.10, Chunk with Custom Checkpoint Processing, clearly
            // outlines that this gets called even when we've already read a null (which
            // arguably is pointless).   But we'll follow the spec.
            if (checkpointManager.applyCheckPointPolicy()) {
                break;
            }

            // last record in readerProxy reached
            if (currentChunkStatus.isFinished()) {
                break;
            }

        }
        return chunkToWrite;
    }

    /**
     * Reads an item from the reader
     *h
     * @return the item read
     */
    private Object readItem() {
        Object itemRead = null;

        try {
            // call read listeners before and after the actual read
            for (ItemReadListener readListenerProxy : itemReadListeners) {
                readListenerProxy.beforeRead();
            }

            itemRead = readerProxy.readItem();

            for (ItemReadListener readListenerProxy : itemReadListeners) {
                readListenerProxy.afterRead(itemRead);
            }

            // itemRead == null means we reached the end of
            // the readerProxy "resultset"
            currentChunkStatus.setFinished(itemRead == null);
        } catch (Exception e) {
            stepContext.setException(e);
            for (ItemReadListener readListenerProxy : itemReadListeners) {
                try {
                    readListenerProxy.onReadError(e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
            if(!currentChunkStatus.isRetryingAfterRollback()) {
                if (retryReadException(e)) {
                    if (!retryHandler.isRollbackException(e)) {
                        itemRead = readItem();
                    } else {
                        // retry with rollback
                        currentChunkStatus.markForRollbackWithRetry(e);
                    }
                } else if (skipReadException(e)) {
                    currentItemStatus.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.READ_SKIP_COUNT).incValue();

                } else {
                    throw new BatchContainerRuntimeException(e);
                }
            } else {
                // coming from a rollback retry
                if (skipReadException(e)) {
                    currentItemStatus.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.READ_SKIP_COUNT).incValue();

                } else if (retryReadException(e)) {
                    if (!retryHandler.isRollbackException(e)) {
                        // retry without rollback
                        itemRead = readItem();
                    } else {
                        // retry with rollback
                        currentChunkStatus.markForRollbackWithRetry(e);
                    }
                } else {
                    throw new BatchContainerRuntimeException(e);
                }
            }

        } catch (final Throwable e) {
            throw new BatchContainerRuntimeException(e);
        }

        return itemRead;
    }


    /**
     * Process an item previously read by the reader
     *
     * @param itemRead the item read
     * @return the processed item
     */
    private Object processItem(final Object itemRead) {
        Object processedItem = null;

        // if no processor defined for this chunk
        if (processorProxy == null) {
            return itemRead;
        }

        try {

            // call process listeners before and after the actual process call
            for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                processListenerProxy.beforeProcess(itemRead);
            }

            processedItem = processorProxy.processItem(itemRead);

            if (processedItem == null) {
                currentItemStatus.setFiltered(true);
            }

            for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                processListenerProxy.afterProcess(itemRead, processedItem);
            }
        } catch (final Exception e) {
            for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                try {
                    processListenerProxy.onProcessError(itemRead, e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
            if(!currentChunkStatus.isRetryingAfterRollback()) {
                if (retryProcessException(e, itemRead)) {
                    if (!retryHandler.isRollbackException(e)) {
                        processedItem = processItem(itemRead);
                    } else {
                        currentChunkStatus.markForRollbackWithRetry(e);
                    }
                } else if (skipProcessException(e, itemRead)) {
                    currentItemStatus.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.PROCESS_SKIP_COUNT).incValue();
                } else {
                    throw new BatchContainerRuntimeException(e);
                }
            } else {
                if (skipProcessException(e, itemRead)) {
                    currentItemStatus.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.PROCESS_SKIP_COUNT).incValue();
                } else if (retryProcessException(e, itemRead)) {
                    if (!retryHandler.isRollbackException(e)) {
                        // retry without rollback
                        processedItem = processItem(itemRead);
                    } else {
                        // retry with rollback
                        currentChunkStatus.markForRollbackWithRetry(e);
                    }
                } else {
                    throw new BatchContainerRuntimeException(e);
                }
            }

        } catch (final Throwable e) {
            throw new BatchContainerRuntimeException(e);
        }

        return processedItem;
    }

    /**
     * Writes items
     *
     * @param theChunk the array list with all items processed ready to be written
     */
    private void writeChunk(List<Object> theChunk) {
        if (!theChunk.isEmpty()) {
            try {

                // call read listeners before and after the actual read
                for (ItemWriteListener writeListenerProxy : itemWriteListeners) {
                    writeListenerProxy.beforeWrite(theChunk);
                }

                writerProxy.writeItems(theChunk);

                for (ItemWriteListener writeListenerProxy : itemWriteListeners) {
                    writeListenerProxy.afterWrite(theChunk);
                }
            } catch (Exception e) {
                this.stepContext.setException(e);
                for (ItemWriteListener writeListenerProxy : itemWriteListeners) {
                    try {
                        writeListenerProxy.onWriteError(theChunk, e);
                    } catch (Exception e1) {
                        ExceptionConfig.wrapBatchException(e1);
                    }
                }
                if(!currentChunkStatus.isRetryingAfterRollback()) {
                    if (retryWriteException(e, theChunk)) {
                        if (!retryHandler.isRollbackException(e)) {
                            // retry without rollback
                            writeChunk(theChunk);
                        } else {
                            // retry with rollback
                            currentChunkStatus.markForRollbackWithRetry(e);
                        }
                    } else if (skipWriteException(e, theChunk)) {
                        stepContext.getMetric(MetricImpl.MetricType.WRITE_SKIP_COUNT).incValueBy(1);
                    } else {
                        throw new BatchContainerRuntimeException(e);
                    }

                } else {
                    if (skipWriteException(e, theChunk)) {
                        stepContext.getMetric(MetricImpl.MetricType.WRITE_SKIP_COUNT).incValueBy(1);
                    } else if (retryWriteException(e, theChunk)) {
                        if (!retryHandler.isRollbackException(e)) {
                            // retry without rollback
                            writeChunk(theChunk);
                        } else {
                            // retry with rollback
                            currentChunkStatus.markForRollbackWithRetry(e);
                        }
                    } else {
                        throw new BatchContainerRuntimeException(e);
                    }
                }

            } catch (Throwable e) {
                throw new BatchContainerRuntimeException(e);
            }
        }
    }

    /**
     * Prime the next chunk's ChunkStatus based on the previous one
     * (if there was one), particularly taking into account retry-with-rollback
     * and the one-at-a-time processing it entails.
     * @return the upcoming chunk's ChunkStatus
     */
    private ChunkStatus getNextChunkStatusBasedOnPrevious() {
        // If this is the first chunk
        if (currentChunkStatus == null) {
            return new ChunkStatus();
        }

        ChunkStatus nextChunkStatus = null;

        // At this point the 'current' status is the previous chunk's status.
        if (currentChunkStatus.wasMarkedForRollbackWithRetry()) {

            // Re-position reader & writer
            transactionManager.begin();
            positionReaderAtCheckpoint();
            positionWriterAtCheckpoint();
            transactionManager.commit();

            nextChunkStatus = new ChunkStatus(ChunkStatusType.RETRY_AFTER_ROLLBACK);

            // What happens if we get a retry-with-rollback on a single item that we were processing
            // after a prior retry with rollback?   We don't want to revert to normal processing
            // after completing only the single item of the "single item chunk".  We want to complete
            // the full portion of the original chunk.  So be careful to propagate this number if
            // it already exists.
            int numToProcessOneByOne = currentChunkStatus.getItemsToProcessOneByOneAfterRollback();
            if (numToProcessOneByOne > 0) {
                // Retry after rollback AFTER a previous retry after rollback
                nextChunkStatus.setItemsToProcessOneByOneAfterRollback(numToProcessOneByOne);
            } else {
                // "Normal" (i.e. the first) retry after rollback.
                nextChunkStatus.setItemsToProcessOneByOneAfterRollback(currentChunkStatus.getItemsTouchedInCurrentChunk());
            }
        } else if (currentChunkStatus.isRetryingAfterRollback()) {
            // In this case the 'current' (actually the last) chunk was a single-item retry after rollback chunk,
            // so we have to see if it's time to revert to normal processing.
            int numToProcessOneByOne = currentChunkStatus.getItemsToProcessOneByOneAfterRollback();
            if (numToProcessOneByOne == 1) {
                // we're done, revert to normal
                nextChunkStatus = new ChunkStatus();
            } else {
                nextChunkStatus = new ChunkStatus(ChunkStatusType.RETRY_AFTER_ROLLBACK);
                nextChunkStatus.setItemsToProcessOneByOneAfterRollback(numToProcessOneByOne - 1);
            }
        } else {
            nextChunkStatus = new ChunkStatus();
        }

        return nextChunkStatus;
    }

    private void invokeChunk() {

        try {
            transactionManager.begin();
            this.openReaderAndWriter();
            transactionManager.commit();
        } catch (final Exception e) {
            rollback(e);
            return;
        }
        try {
            while (true) {

                currentChunkStatus = getNextChunkStatusBasedOnPrevious();

                // Sequence surrounding beginCheckpoint() updated per MR
                // https://java.net/bugzilla/show_bug.cgi?id=5873
                setNextChunkTransactionTimeout();

                // Remember we "wrap" the built-in item-count + time-limit "algorithm"
                // in a CheckpointAlgorithm for ease in keeping the sequence consistent
                checkpointManager.beginCheckpoint();

                transactionManager.begin();

                for (ChunkListener chunkProxy : chunkListeners) {
                    chunkProxy.beforeChunk();
                }

                final List<Object> chunkToWrite = readAndProcess();

                if (currentChunkStatus.wasMarkedForRollbackWithRetry()) {
                    rollbackAfterRetryableException();
                    continue;
                }

                // MR 1.0 Rev A clarified we'd only write a chunk with at least one item.
                // See, e.g. Sec 11.6 of Spec
                if (chunkToWrite.size() > 0) {
                    writeChunk(chunkToWrite);
                }

                if (currentChunkStatus.wasMarkedForRollbackWithRetry()) {
                    rollbackAfterRetryableException();

                    continue;
                }

                for (ChunkListener chunkProxy : chunkListeners) {
                    chunkProxy.afterChunk();
                }

                Map<CheckpointDataKey, CheckpointData> checkpoints = checkpointManager.prepareCheckpoints();
                PersistentDataWrapper userData = resolveUserData();
                try {
                    transactionManager.commit();
                    storeUserData(userData);
                    checkpointManager.storeCheckPoints(checkpoints);
                } catch (Exception e) {
                    // only set the Exception if we didn't blow up before anyway
                    if (this.stepContext.getException() != null) {
                        this.stepContext.setException(e);
                    }
                    if (e instanceof BatchRuntimeException) {
                        throw e;
                    }
                    throw new BatchContainerServiceException("Cannot commit the transaction for the step.", e);
                }

                checkpointManager.endCheckpoint();

                invokeCollectorIfPresent();

                updateNormalMetrics(chunkToWrite.size());

                // exit loop when last record is written
                if (currentChunkStatus.isFinished()) {
                    transactionManager.begin();
                    if (doClose()) {
                        transactionManager.commit();
                    } else {
                        transactionManager.rollback();
                    }
                    break;
                }
            }
        } catch (final Exception e) {
            logger.log(Level.SEVERE, "Failure in Read-Process-Write Loop", e);
            // Only try to call onError() if we have an Exception, but not an Error.
            for (ChunkListener chunkProxy : chunkListeners) {
                try {
                    chunkProxy.onError(e);
                } catch (final Exception e1) {
                    logger.log(Level.SEVERE, e1.getMessage(), e1);
                }
            }
            rollback(e);
        } catch (final Throwable t) {
            rollback(t);
        }
    }

    private void updateNormalMetrics(int writeCount) {
        int readCount = currentChunkStatus.getItemsTouchedInCurrentChunk();
        if (currentChunkStatus.isFinished()) {
            readCount--;
        }
        int filterCount = readCount - writeCount;

        if (readCount < 0 || filterCount < 0 || writeCount < 0) {
            throw new IllegalStateException("Somehow one of the metrics was zero.  Read count: " + readCount +
                    ", Filter count: " + filterCount + ", Write count: " + writeCount);
        }
        stepContext.getMetric(MetricImpl.MetricType.COMMIT_COUNT).incValue();
        stepContext.getMetric(MetricImpl.MetricType.READ_COUNT).incValueBy(readCount);
        stepContext.getMetric(MetricImpl.MetricType.FILTER_COUNT).incValueBy(filterCount);
        stepContext.getMetric(MetricImpl.MetricType.WRITE_COUNT).incValueBy(writeCount);
    }

    private boolean doClose() {
        try {
            readerProxy.close();
            writerProxy.close();
            return true;
        } catch (final Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Reflect spec order in Sec. 11.9 "Rollback Procedure".
     *
     * Also do final rollback in try-finally
     */
    private void rollback(final Throwable t) {
        try {
            try {
                doClose();
            } catch (Exception e) {
                // ignore, we blow up anyway
            }

            // ignore, we blow up anyway
            transactionManager.setRollbackOnly();

            if (t instanceof Exception) {
                Exception e = (Exception) t;
                for (ChunkListener chunkProxy : chunkListeners) {
                    try {
                        chunkProxy.onError(e);
                    } catch (final Exception e1) {
                        logger.log(Level.SEVERE, e1.getMessage(), e1);
                    }
                }
            }
            // Count non-retryable rollback against metric as well.  Not sure this has
            // ever come up in the spec, but seems marginally more useful.
            stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
        } finally {
            int txStatus = transactionManager.getStatus();
            if (txStatus == Status.STATUS_ACTIVE || txStatus == Status.STATUS_MARKED_ROLLBACK) {
                transactionManager.rollback();
            }
            throw new BatchContainerRuntimeException("Failure in Read-Process-Write Loop", t);
        }
    }

    private void rollbackAfterRetryableException() throws Exception {
        doClose();

        for (ChunkListener chunkProxy : chunkListeners) {
            try {
                chunkProxy.onError(currentChunkStatus.getRetryableException());
            } catch (final Exception e1) {
                logger.log(Level.SEVERE, e1.getMessage(), e1);
            }
        }
        transactionManager.rollback();

        stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
    }

    @Override
    protected void invokeCoreStep() throws BatchContainerServiceException {

        this.chunk = step.getChunk();

        initializeChunkArtifacts();

        initializeCheckpointManager();

        invokeChunk();
    }

    private void initializeCheckpointManager() {
        CheckpointAlgorithm checkpointAlgorithm = null;

        checkpointAtThisItemCount = ChunkHelper.getItemCount(chunk);
        int timeLimitSeconds = ChunkHelper.getTimeLimit(chunk);
        customCheckpointPolicy = ChunkHelper.isCustomCheckpointPolicy(chunk);  // Supplies default if needed

        if (!customCheckpointPolicy) {
            ItemCheckpointAlgorithm ica = new ItemCheckpointAlgorithm();
            ica.setItemCount(checkpointAtThisItemCount);
            ica.setTimeLimitSeconds(timeLimitSeconds);
            checkpointAlgorithm = ica;

        } else {
            final List<Property> propList;

            if (chunk.getCheckpointAlgorithm() == null) {
                throw new IllegalArgumentException("Configured checkpoint-policy of 'custom' but without a corresponding <checkpoint-algorithm> element.");
            } else {
                propList = (chunk.getCheckpointAlgorithm().getProperties() == null) ? null : chunk.getCheckpointAlgorithm().getProperties().getPropertyList();
            }

            InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, propList);
            checkpointAlgorithm = ProxyFactory.createCheckpointAlgorithmProxy(artifactFactory, chunk.getCheckpointAlgorithm().getRef(), injectionRef, jobExecutionImpl);
        }

        // Finally, for both policies now
        checkpointManager = new CheckpointManager(readerProxy, writerProxy, checkpointAlgorithm,
                jobExecutionImpl.getJobInstance().getInstanceId(), step.getId(), persistenceManagerService, dataRepresentationService);

        // A related piece of data we'll calculate here is the tran timeout.   Though we won't include
        // it in the checkpoint manager since we'll set it directly on the tran mgr before each chunk.
        stepPropertyTranTimeoutSeconds = initStepTransactionTimeout();
    }

    /*
     * Initialize itemreader, itemwriter, and item processor checkpoint
     */
    private void initializeChunkArtifacts() {
        {
            final org.apache.batchee.jaxb.ItemReader itemReader = chunk.getReader();
            final List<Property> itemReaderProps = itemReader.getProperties() == null ? null : itemReader.getProperties().getPropertyList();
            final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, itemReaderProps);
            readerProxy = ProxyFactory.createItemReaderProxy(artifactFactory, itemReader.getRef(), injectionRef, jobExecutionImpl);
        }

        {
            final org.apache.batchee.jaxb.ItemProcessor itemProcessor = chunk.getProcessor();
            if (itemProcessor != null) {
                final List<Property> itemProcessorProps = itemProcessor.getProperties() == null ? null : itemProcessor.getProperties().getPropertyList();
                final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, itemProcessorProps);
                processorProxy = ProxyFactory.createItemProcessorProxy(artifactFactory, itemProcessor.getRef(), injectionRef, jobExecutionImpl);
            }
        }

        {
            final org.apache.batchee.jaxb.ItemWriter itemWriter = chunk.getWriter();
            final List<Property> itemWriterProps = itemWriter.getProperties() == null ? null : itemWriter.getProperties().getPropertyList();
            final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, itemWriterProps);
            writerProxy = ProxyFactory.createItemWriterProxy(artifactFactory, itemWriter.getRef(), injectionRef, jobExecutionImpl);
        }

        {
            final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, null);

            this.chunkListeners = jobExecutionImpl.getListenerFactory().getListeners(ChunkListener.class, step, injectionRef, jobExecutionImpl);
            this.itemReadListeners = jobExecutionImpl.getListenerFactory().getListeners(ItemReadListener.class, step, injectionRef, jobExecutionImpl);
            this.itemProcessListeners = jobExecutionImpl.getListenerFactory().getListeners(ItemProcessListener.class, step, injectionRef, jobExecutionImpl);
            this.itemWriteListeners = jobExecutionImpl.getListenerFactory().getListeners(ItemWriteListener.class, step, injectionRef, jobExecutionImpl);
            final List<SkipProcessListener> skipProcessListeners
                    = jobExecutionImpl.getListenerFactory().getListeners(SkipProcessListener.class, step, injectionRef, jobExecutionImpl);
            final List<SkipReadListener> skipReadListeners
                    = jobExecutionImpl.getListenerFactory().getListeners(SkipReadListener.class, step, injectionRef, jobExecutionImpl);
            final List<SkipWriteListener> skipWriteListeners
                    = jobExecutionImpl.getListenerFactory().getListeners(SkipWriteListener.class, step, injectionRef, jobExecutionImpl);
            final List<RetryProcessListener> retryProcessListeners
                    = jobExecutionImpl.getListenerFactory().getListeners(RetryProcessListener.class, step, injectionRef, jobExecutionImpl);
            final List<RetryReadListener> retryReadListeners
                    = jobExecutionImpl.getListenerFactory().getListeners(RetryReadListener.class, step, injectionRef, jobExecutionImpl);
            final List<RetryWriteListener> retryWriteListeners
                    = jobExecutionImpl.getListenerFactory().getListeners(RetryWriteListener.class, step, injectionRef, jobExecutionImpl);

            skipHandler = new SkipHandler(chunk);
            skipHandler.addSkipProcessListener(skipProcessListeners);
            skipHandler.addSkipReadListener(skipReadListeners);
            skipHandler.addSkipWriteListener(skipWriteListeners);

            retryHandler = new RetryHandler(chunk);

            retryHandler.addRetryProcessListener(retryProcessListeners);
            retryHandler.addRetryReadListener(retryReadListeners);
            retryHandler.addRetryWriteListener(retryWriteListeners);
        }
    }

    private void setNextChunkTransactionTimeout() {
        int nextTimeout = 0;

        if (customCheckpointPolicy) {
            // Even on a retry-with-rollback, we'll continue to let
            // the custom CheckpointAlgorithm set a tran timeout.
            //
            // We're guessing the application could need a smaller timeout than
            // 180 seconds, (the default established by the batch chunk).
            nextTimeout = this.checkpointManager.checkpointTimeout();
        } else  {
            nextTimeout = stepPropertyTranTimeoutSeconds;
        }
        transactionManager.setTransactionTimeout(nextTimeout);
    }

    /**
     * Note we can rely on the StepContext properties already having been set at this point.
     *
     * @return global transaction timeout defined in step properties. default
     */
    private int initStepTransactionTimeout() {
        Properties p = stepContext.getProperties();
        int timeout = DEFAULT_TRAN_TIMEOUT_SECONDS; // default as per spec.
        if (p != null && !p.isEmpty()) {

            String propertyTimeOut = p.getProperty("javax.transaction.global.timeout");
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "javax.transaction.global.timeout = {0}", propertyTimeOut==null ? "<null>" : propertyTimeOut);
            }
            if (propertyTimeOut != null && !propertyTimeOut.isEmpty()) {
                timeout = Integer.parseInt(propertyTimeOut, 10);
            }
        }
        return timeout;
    }

    private void openReaderAndWriter() {
        readerChkptDK = new CheckpointDataKey(jobExecutionImpl.getJobInstance().getInstanceId(), step.getId(), CheckpointType.READER);
        CheckpointData readerChkptData = persistenceManagerService.getCheckpointData(readerChkptDK);
        try {

            // check for data in backing store
            if (readerChkptData != null) {
                final byte[] readertoken = readerChkptData.getRestartToken();
                try {
                    readerProxy.open((Serializable) dataRepresentationService.toJavaRepresentation(readertoken));
                } catch (final Exception ex) {
                    // is this what I should be throwing here?
                    throw new BatchContainerServiceException("Cannot read the checkpoint data for [" + step.getId() + "]", ex);
                }
            } else {
                // no chkpt data exists in the backing store
                readerChkptData = null;
                try {
                    readerProxy.open(null);
                } catch (final Exception ex) {
                    // is this what I should be throwing here?
                    throw new BatchContainerServiceException("Exception while opening step [" + step.getId() + "]", ex);
                }
            }
        } catch (final ClassCastException e) {
            throw new IllegalStateException("Expected CheckpointData but found" + readerChkptData);
        }

        writerChkptDK = new CheckpointDataKey(jobExecutionImpl.getJobInstance().getInstanceId(), step.getId(), CheckpointType.WRITER);
        CheckpointData writerChkptData = persistenceManagerService.getCheckpointData(writerChkptDK);
        try {
            // check for data in backing store
            if (writerChkptData != null) {
                final byte[] writertoken = writerChkptData.getRestartToken();
                try {
                    writerProxy.open((Serializable) dataRepresentationService.toJavaRepresentation(writertoken));
                } catch (final Exception ex) {
                    throw new BatchContainerServiceException("Cannot persist the checkpoint data for [" + step.getId() + "]", ex);
                }
            } else {
                // no chkpt data exists in the backing store
                writerChkptData = null;
                try {
                    writerProxy.open(null);
                } catch (Exception e) {
                    ExceptionConfig.wrapBatchException(e);
                }
            }
        } catch (final ClassCastException e) {
            throw new IllegalStateException("Expected Checkpoint but found" + writerChkptData);
        }
    }

    @Override
    public void stop() {
        stepContext.setBatchStatus(BatchStatus.STOPPING);

        // we don't need to call stop on the chunk implementation here since a
        // chunk always returns control to
        // the batch container after every item.

    }

    private boolean skipReadException(final Exception e) {
        try {
            skipHandler.handleExceptionRead(e);
        } catch (final BatchContainerRuntimeException bcre) {
            return false;
        }
        return true;
    }

    private boolean retryReadException(final Exception e) {
        try {
            retryHandler.handleExceptionRead(e);
        } catch (final BatchContainerRuntimeException bcre) {
            return false;
        }
        return true;

    }

    private boolean skipProcessException(final Exception e, final Object record) {
        try {
            skipHandler.handleExceptionWithRecordProcess(e, record);
        } catch (BatchContainerRuntimeException bcre) {
            return false;
        }
        return true;

    }

    private boolean retryProcessException(final Exception e, final Object record) {
        try {
            retryHandler.handleExceptionProcess(e, record);
        } catch (BatchContainerRuntimeException bcre) {
            return false;
        }
        return true;
    }

    private boolean skipWriteException(final Exception e, final List<Object> chunkToWrite) {
        try {
            skipHandler.handleExceptionWithRecordListWrite(e, chunkToWrite);
        } catch (BatchContainerRuntimeException bcre) {
            return false;
        }
        return true;
    }

    private boolean retryWriteException(final Exception e, final List<Object> chunkToWrite) {
        try {
            retryHandler.handleExceptionWrite(e, chunkToWrite);
        } catch (BatchContainerRuntimeException bcre) {
            return false;
        }
        return true;
    }

    private void positionReaderAtCheckpoint() {
        readerChkptDK = new CheckpointDataKey(jobExecutionImpl.getJobInstance().getInstanceId(), step.getId(), CheckpointType.READER);

        CheckpointData readerData = persistenceManagerService.getCheckpointData(readerChkptDK);
        try {
            // check for data in backing store
            if (readerData != null) {
                byte[] readertoken = readerData.getRestartToken();
                try {
                    readerProxy.open((Serializable) dataRepresentationService.toJavaRepresentation(readertoken));
                } catch (Exception ex) {
                    // is this what I should be throwing here?
                    throw new BatchContainerServiceException("Cannot persist the checkpoint data for [" + step.getId() + "]", ex);
                }
            } else {
                // no chkpt data exists in the backing store
                readerData = null;
                try {
                    readerProxy.open(null);
                } catch (final Exception ex) {
                    // is this what I should be throwing here?
                    throw new BatchContainerServiceException("Cannot persist the checkpoint data for [" + step.getId() + "]", ex);
                }
            }
        } catch (final ClassCastException e) {
            throw new IllegalStateException("Expected CheckpointData but found" + readerData);
        }
    }

    private void positionWriterAtCheckpoint() {
        writerChkptDK = new CheckpointDataKey(jobExecutionImpl.getJobInstance().getInstanceId(), step.getId(), CheckpointType.WRITER);

        CheckpointData writerData = persistenceManagerService.getCheckpointData(writerChkptDK);
        try {
            // check for data in backing store
            if (writerData != null) {
                byte[] writertoken = writerData.getRestartToken();
                TCCLObjectInputStream writerOIS;
                try {
                    writerProxy.open((Serializable) dataRepresentationService.toJavaRepresentation(writertoken));
                } catch (Exception ex) {
                    // is this what I should be throwing here?
                    throw new BatchContainerServiceException("Cannot read the checkpoint data for [" + step.getId() + "]", ex);
                }
            } else {
                // no chkpt data exists in the backing store
                writerData = null;
                try {
                    writerProxy.open(null);
                } catch (Exception ex) {
                    throw new BatchContainerServiceException("Cannot open the step [" + step.getId() + "]", ex);
                }
            }
        } catch (ClassCastException e) {
            throw new IllegalStateException("Expected CheckpointData but found" + writerData);
        }
    }
}
