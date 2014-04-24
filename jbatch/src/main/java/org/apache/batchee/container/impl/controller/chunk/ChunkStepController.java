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
import org.apache.batchee.container.proxy.CheckpointAlgorithmProxy;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.container.util.TCCLObjectInputStream;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.spi.BatchArtifactFactory;
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
import javax.batch.runtime.BatchStatus;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChunkStepController extends SingleThreadedStepController {

    private final static Logger logger = Logger.getLogger(ChunkStepController.class.getName());

    private final PersistenceManagerService persistenceManagerService;
    private final BatchArtifactFactory artifactFactory;

    private Chunk chunk = null;
    private ItemReader readerProxy = null;
    private ItemProcessor processorProxy = null;
    private ItemWriter writerProxy = null;
    private CheckpointAlgorithmProxy checkpointProxy = null;
    private CheckpointAlgorithm chkptAlg = null;
    private CheckpointManager checkpointManager;
    private SkipHandler skipHandler = null;
    private CheckpointDataKey readerChkptDK = null;
    private CheckpointDataKey writerChkptDK = null;
    private List<ChunkListener> chunkListeners = null;
    private List<ItemReadListener> itemReadListeners = null;
    private List<ItemProcessListener> itemProcessListeners = null;
    private List<ItemWriteListener> itemWriteListeners = null;
    private RetryHandler retryHandler;

    private boolean rollbackRetry = false;

    public ChunkStepController(final RuntimeJobExecution jobExecutionImpl, final Step step, final StepContextImpl stepContext,
                               final long rootJobExecutionId, final BlockingQueue<PartitionDataWrapper> analyzerStatusQueue,
                               final ServicesManager servicesManager) {
        super(jobExecutionImpl, step, stepContext, rootJobExecutionId, analyzerStatusQueue, servicesManager);
        this.persistenceManagerService = servicesManager.service(PersistenceManagerService.class);
        this.artifactFactory = servicesManager.service(BatchArtifactFactory.class);
    }

    /**
     * Utility Class to hold statuses at each level of Read-Process-Write loop
     */
    private class ItemStatus {

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

        public boolean isCheckPointed() {
            return checkPointed;
        }

        public void setCheckPointed(boolean checkPointed) {
            this.checkPointed = checkPointed;
        }

        public boolean isFinished() {
            return finished;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
        }

        public void setRetry(boolean ignored) {
            // no-op
        }

        public boolean isRollback() {
            return rollback;
        }

        public void setRollback(boolean rollback) {
            this.rollback = rollback;
        }

        private boolean skipped = false;
        private boolean filtered = false;
        private boolean finished = false;
        private boolean checkPointed = false;
        private boolean rollback = false;

    }

    /**
     * We read and process one item at a time but write in chunks (group of
     * items). So, this method loops until we either reached the end of the
     * reader (not more items to read), or the writer buffer is full or a
     * checkpoint is triggered.
     *
     * @param chunkSize write buffer size
     * @param theStatus flags when the read-process reached the last record or a
     *                  checkpoint is required
     * @return an array list of objects to write
     */
    private List<Object> readAndProcess(int chunkSize, ItemStatus theStatus) {
        List<Object> chunkToWrite = new ArrayList<Object>();
        Object itemRead;
        Object itemProcessed;
        int readProcessedCount = 0;

        while (true) {
            ItemStatus status = new ItemStatus();
            itemRead = readItem(status);

            if (status.isRollback()) {
                theStatus.setRollback(true);
                // inc rollbackCount
                stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
                break;
            }

            if (!status.isSkipped() && !status.isFinished()) {
                itemProcessed = processItem(itemRead, status);

                if (status.isRollback()) {
                    theStatus.setRollback(true);
                    // inc rollbackCount
                    stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
                    break;
                }

                if (!status.isSkipped() && !status.isFiltered()) {
                    chunkToWrite.add(itemProcessed);
                    readProcessedCount++;
                }
            }

            theStatus.setFinished(status.isFinished());
            theStatus.setCheckPointed(checkpointManager.applyCheckPointPolicy());

            // This will force the current item to finish processing on a stop
            // request
            if (stepContext.getBatchStatus().equals(BatchStatus.STOPPING)) {
                theStatus.setFinished(true);
            }

            // write buffer size reached
            if ((readProcessedCount == chunkSize) && !("custom".equals(checkpointProxy.getCheckpointType()))) {
                break;
            }

            // checkpoint reached
            if (theStatus.isCheckPointed()) {
                break;
            }

            // last record in readerProxy reached
            if (theStatus.isFinished()) {
                break;
            }

        }
        return chunkToWrite;
    }

    /**
     * Reads an item from the reader
     *
     * @param status flags the current read status
     * @return the item read
     */
    private Object readItem(ItemStatus status) {
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
            status.setFinished(itemRead == null);
            if (!status.isFinished()) {
                stepContext.getMetric(MetricImpl.MetricType.READ_COUNT).incValue();
            }
        } catch (Exception e) {
            stepContext.setException(e);
            for (ItemReadListener readListenerProxy : itemReadListeners) {
                try {
                    readListenerProxy.onReadError(e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
            if (!rollbackRetry) {
                if (retryReadException(e)) {
                    for (ItemReadListener readListenerProxy : itemReadListeners) {
                        try {
                            readListenerProxy.onReadError(e);
                        } catch (Exception e1) {
                            ExceptionConfig.wrapBatchException(e1);
                        }
                    }
                    // if not a rollback exception, just retry the current item
                    if (!retryHandler.isRollbackException(e)) {
                        itemRead = readItem(status);
                    } else {
                        status.setRollback(true);
                        rollbackRetry = true;
                        // inc rollbackCount
                        stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
                    }
                } else if (skipReadException(e)) {
                    status.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.READ_SKIP_COUNT).incValue();

                } else {
                    throw new BatchContainerRuntimeException(e);
                }
            } else {
                // coming from a rollback retry
                if (skipReadException(e)) {
                    status.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.READ_SKIP_COUNT).incValue();

                } else if (retryReadException(e)) {
                    if (!retryHandler.isRollbackException(e)) {
                        itemRead = readItem(status);
                    } else {
                        status.setRollback(true);
                        // inc rollbackCount
                        stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
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
     * @param status   flags the current process status
     * @return the processed item
     */
    private Object processItem(final Object itemRead, final ItemStatus status) {
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
                // inc filterCount
                stepContext.getMetric(MetricImpl.MetricType.FILTER_COUNT).incValue();
                status.setFiltered(true);
            }

            for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                processListenerProxy.afterProcess(itemRead, processedItem);
            }
        } catch (final Exception e) {
            for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                try {
                    processListenerProxy.onProcessError(processedItem, e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
            if (!rollbackRetry) {
                if (retryProcessException(e, itemRead)) {
                    if (!retryHandler.isRollbackException(e)) {
                        // call process listeners before and after the actual
                        // process call
                        for (ItemProcessListener processListenerProxy : itemProcessListeners) {
                            try {
                                processListenerProxy.beforeProcess(itemRead);
                            } catch (Exception e1) {
                                ExceptionConfig.wrapBatchException(e1);
                            }
                        }
                        processedItem = processItem(itemRead, status);
                        if (processedItem == null) {
                            // inc filterCount
                            stepContext.getMetric(MetricImpl.MetricType.FILTER_COUNT).incValue();
                            status.setFiltered(true);
                        }

                        for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                            try {
                                processListenerProxy.afterProcess(itemRead, processedItem);
                            } catch (Exception e1) {
                                ExceptionConfig.wrapBatchException(e1);
                            }
                        }
                    } else {
                        status.setRollback(true);
                        rollbackRetry = true;
                        // inc rollbackCount
                        stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
                    }
                } else if (skipProcessException(e, itemRead)) {
                    status.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.PROCESS_SKIP_COUNT).incValue();
                } else {
                    throw new BatchContainerRuntimeException(e);
                }
            } else {
                if (skipProcessException(e, itemRead)) {
                    status.setSkipped(true);
                    stepContext.getMetric(MetricImpl.MetricType.PROCESS_SKIP_COUNT).incValue();
                } else if (retryProcessException(e, itemRead)) {
                    if (!retryHandler.isRollbackException(e)) {
                        // call process listeners before and after the actual
                        // process call
                        for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                            try {
                                processListenerProxy.beforeProcess(itemRead);
                            } catch (Exception e1) {
                                ExceptionConfig.wrapBatchException(e1);
                            }
                        }
                        processedItem = processItem(itemRead, status);
                        if (processedItem == null) {
                            // inc filterCount
                            stepContext.getMetric(MetricImpl.MetricType.FILTER_COUNT).incValue();
                            status.setFiltered(true);
                        }

                        for (final ItemProcessListener processListenerProxy : itemProcessListeners) {
                            try {
                                processListenerProxy.afterProcess(itemRead, processedItem);
                            } catch (Exception e1) {
                                ExceptionConfig.wrapBatchException(e1);
                            }
                        }
                    } else {
                        status.setRollback(true);
                        rollbackRetry = true;
                        // inc rollbackCount
                        stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
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
    private void writeChunk(List<Object> theChunk, ItemStatus status) {
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
                stepContext.getMetric(MetricImpl.MetricType.WRITE_COUNT).incValueBy(theChunk.size());
            } catch (Exception e) {
                this.stepContext.setException(e);
                for (ItemWriteListener writeListenerProxy : itemWriteListeners) {
                    try {
                        writeListenerProxy.onWriteError(theChunk, e);
                    } catch (Exception e1) {
                        ExceptionConfig.wrapBatchException(e1);
                    }
                }
                if (!rollbackRetry) {
                    if (retryWriteException(e, theChunk)) {
                        if (!retryHandler.isRollbackException(e)) {
                            writeChunk(theChunk, status);
                        } else {
                            rollbackRetry = true;
                            status.setRollback(true);
                            // inc rollbackCount
                            stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
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
                            status.setRetry(true);
                            writeChunk(theChunk, status);
                        } else {
                            rollbackRetry = true;
                            status.setRollback(true);
                            // inc rollbackCount
                            stepContext.getMetric(MetricImpl.MetricType.ROLLBACK_COUNT).incValue();
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

    private void invokeChunk() {
        int itemCount = ChunkHelper.getItemCount(chunk);
        int timeInterval = ChunkHelper.getTimeLimit(chunk);
        boolean checkPointed = true;
        boolean rollback = false;

        // begin new transaction at first iteration or after a checkpoint commit

        try {
            transactionManager.begin();
            this.openReaderAndWriter();
            transactionManager.commit();

            while (true) {

                if (checkPointed || rollback) {
                    if ("custom".equals(checkpointProxy.getCheckpointType())) {
                        int newtimeOut = this.checkpointManager.checkpointTimeout();
                        transactionManager.setTransactionTimeout(newtimeOut);
                    }
                    transactionManager.begin();
                    for (ChunkListener chunkProxy : chunkListeners) {
                        chunkProxy.beforeChunk();
                    }

                    if (rollback) {
                        positionReaderAtCheckpoint();
                        positionWriterAtCheckpoint();
                        checkpointManager = new CheckpointManager(readerProxy, writerProxy,
                            getCheckpointAlgorithm(itemCount, timeInterval), jobExecutionImpl
                            .getJobInstance().getInstanceId(), step.getId(), persistenceManagerService);
                    }
                }

                ItemStatus status = new ItemStatus();

                if (rollback) {
                    rollback = false;
                }

                final List<Object> chunkToWrite = readAndProcess(itemCount, status);

                if (status.isRollback()) {
                    itemCount = 1;
                    rollback = true;

                    readerProxy.close();
                    writerProxy.close();

                    transactionManager.rollback();

                    continue;
                }

                writeChunk(chunkToWrite, status);

                if (status.isRollback()) {
                    itemCount = 1;
                    rollback = true;

                    readerProxy.close();
                    writerProxy.close();

                    transactionManager.rollback();

                    continue;
                }
                checkPointed = status.isCheckPointed();

                // we could finish the chunk in 3 conditions: buffer is full,
                // checkpoint, not more input
                if (status.isCheckPointed() || status.isFinished()) {
                    // TODO: missing before checkpoint listeners
                    // 1.- check if spec list proper steps for before checkpoint
                    // 2.- ask Andy about retry
                    // 3.- when do we stop?

                    checkpointManager.checkpoint();

                    for (ChunkListener chunkProxy : chunkListeners) {
                        chunkProxy.afterChunk();
                    }

                    this.persistUserData();

                    this.chkptAlg.beginCheckpoint();

                    transactionManager.commit();

                    this.chkptAlg.endCheckpoint();

                    invokeCollectorIfPresent();

                    // exit loop when last record is written
                    if (status.isFinished()) {
                        transactionManager.begin();

                        readerProxy.close();
                        writerProxy.close();

                        transactionManager.commit();
                        // increment commitCount
                        stepContext.getMetric(MetricImpl.MetricType.COMMIT_COUNT).incValue();
                        break;
                    } else {
                        // increment commitCount
                        stepContext.getMetric(MetricImpl.MetricType.COMMIT_COUNT).incValue();
                    }

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

    private void rollback(final Throwable t) {
        transactionManager.setRollbackOnly();
        try {
            readerProxy.close();
            writerProxy.close();
        } catch (Exception e) {
            // ignore, we blow up anyway
        }
        transactionManager.rollback();
        throw new BatchContainerRuntimeException("Failure in Read-Process-Write Loop", t);
    }

    protected void invokeCoreStep() throws BatchContainerServiceException {

        this.chunk = step.getChunk();

        initializeChunkArtifacts();

        invokeChunk();
    }

    private CheckpointAlgorithm getCheckpointAlgorithm(final int itemCount, final int timeInterval) {
        final CheckpointAlgorithm alg;
        if ("item".equals(checkpointProxy.getCheckpointType())) {
            alg = new ItemCheckpointAlgorithm();
            ((ItemCheckpointAlgorithm) alg).setThresholds(itemCount, timeInterval);
        } else { // custom chkpt alg
            alg = checkpointProxy;
        }

        return alg;
    }

    /*
     * Initialize itemreader, itemwriter, and item processor checkpoint
     */
    private void initializeChunkArtifacts() {
        final int itemCount = ChunkHelper.getItemCount(chunk);
        final int timeInterval = ChunkHelper.getTimeLimit(chunk);

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
            final List<Property> propList;
            if (chunk.getCheckpointAlgorithm() != null) {
                propList = (chunk.getCheckpointAlgorithm().getProperties() == null) ? null : chunk.getCheckpointAlgorithm().getProperties().getPropertyList();
            } else {
                propList = null;
            }

            final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, propList);
            checkpointProxy = CheckpointAlgorithmFactory.getCheckpointAlgorithmProxy(artifactFactory, step, injectionRef, jobExecutionImpl);
        }

        {
            final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, null);

            this.chunkListeners = jobExecutionImpl.getListenerFactory().getChunkListeners(step, injectionRef, stepContext, jobExecutionImpl);
            this.itemReadListeners = jobExecutionImpl.getListenerFactory().getItemReadListeners(step, injectionRef, stepContext, jobExecutionImpl);
            this.itemProcessListeners = jobExecutionImpl.getListenerFactory().getItemProcessListeners(step, injectionRef, stepContext, jobExecutionImpl);
            this.itemWriteListeners = jobExecutionImpl.getListenerFactory().getItemWriteListeners(step, injectionRef, stepContext, jobExecutionImpl);
            final List<SkipProcessListener> skipProcessListeners
                    = jobExecutionImpl.getListenerFactory().getSkipProcessListeners(step, injectionRef, stepContext, jobExecutionImpl);
            final List<SkipReadListener> skipReadListeners
                    = jobExecutionImpl.getListenerFactory().getSkipReadListeners(step, injectionRef, stepContext, jobExecutionImpl);
            final List<SkipWriteListener> skipWriteListeners
                    = jobExecutionImpl.getListenerFactory().getSkipWriteListeners(step, injectionRef, stepContext, jobExecutionImpl);
            final List<RetryProcessListener> retryProcessListeners
                    = jobExecutionImpl.getListenerFactory().getRetryProcessListeners(step, injectionRef, stepContext, jobExecutionImpl);
            final List<RetryReadListener> retryReadListeners
                    = jobExecutionImpl.getListenerFactory().getRetryReadListeners(step, injectionRef, stepContext, jobExecutionImpl);
            final List<RetryWriteListener> retryWriteListeners
                    = jobExecutionImpl.getListenerFactory().getRetryWriteListeners(step, injectionRef, stepContext, jobExecutionImpl);

            if ("item".equals(checkpointProxy.getCheckpointType())) {
                chkptAlg = new ItemCheckpointAlgorithm();
                ItemCheckpointAlgorithm.class.cast(chkptAlg).setThresholds(itemCount, timeInterval);
            } else { // custom chkpt alg
                chkptAlg = checkpointProxy;
            }

            checkpointManager
                    = new CheckpointManager(readerProxy, writerProxy, chkptAlg, jobExecutionImpl.getJobInstance().getInstanceId(), step.getId(), persistenceManagerService);

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

    private void openReaderAndWriter() {
        readerChkptDK = new CheckpointDataKey(jobExecutionImpl.getJobInstance().getInstanceId(), step.getId(), CheckpointType.READER);
        CheckpointData readerChkptData = persistenceManagerService.getCheckpointData(readerChkptDK);
        try {

            // check for data in backing store
            if (readerChkptData != null) {
                final byte[] readertoken = readerChkptData.getRestartToken();
                final ByteArrayInputStream readerChkptBA = new ByteArrayInputStream(readertoken);
                TCCLObjectInputStream readerOIS;
                try {
                    readerOIS = new TCCLObjectInputStream(readerChkptBA);
                    readerProxy.open((Serializable) readerOIS.readObject());
                    readerOIS.close();
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
                final ByteArrayInputStream writerChkptBA = new ByteArrayInputStream(writertoken);
                TCCLObjectInputStream writerOIS;
                try {
                    writerOIS = new TCCLObjectInputStream(writerChkptBA);
                    writerProxy.open((Serializable) writerOIS.readObject());
                    writerOIS.close();
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
                ByteArrayInputStream readerChkptBA = new ByteArrayInputStream(readertoken);
                TCCLObjectInputStream readerOIS;
                try {
                    readerOIS = new TCCLObjectInputStream(readerChkptBA);
                    readerProxy.open((Serializable) readerOIS.readObject());
                    readerOIS.close();
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
                ByteArrayInputStream writerChkptBA = new ByteArrayInputStream(writertoken);
                TCCLObjectInputStream writerOIS;
                try {
                    writerOIS = new TCCLObjectInputStream(writerChkptBA);
                    writerProxy.open((Serializable) writerOIS.readObject());
                    writerOIS.close();
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
