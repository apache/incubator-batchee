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
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.ExceptionClassFilter;

import java.util.List;

import javax.batch.api.chunk.listener.SkipProcessListener;
import javax.batch.api.chunk.listener.SkipReadListener;
import javax.batch.api.chunk.listener.SkipWriteListener;

public class SkipHandler {

    /**
     * Logic for handling skipped records.
     */

    private List<SkipProcessListener> _skipProcessListener = null;
    private List<SkipReadListener> _skipReadListener = null;
    private List<SkipWriteListener> _skipWriteListener = null;

    private final ExceptionConfig config = new ExceptionConfig();

    private int _skipLimit = Integer.MIN_VALUE;
    private long _skipCount = 0;

    public SkipHandler(final Chunk chunk) {
        try {
            if (chunk.getSkipLimit() != null) {
                _skipLimit = Integer.parseInt(chunk.getSkipLimit());
                if (_skipLimit < 0) {
                    throw new IllegalArgumentException("The skip-limit attribute on a chunk cannot be a negative value");
                }
            }
        } catch (final NumberFormatException nfe) {
            throw new RuntimeException("NumberFormatException reading skip-limit", nfe);
        }


        // Read the include/exclude exceptions.

        if (chunk.getSkippableExceptionClasses() != null && chunk.getSkippableExceptionClasses().getIncludeList() != null) {
            final List<ExceptionClassFilter.Include> includes = chunk.getSkippableExceptionClasses().getIncludeList();
            for (final ExceptionClassFilter.Include include : includes) {
                config.getIncludes().add(include.getClazz().trim());
            }
        }

        if (chunk.getSkippableExceptionClasses() != null && chunk.getSkippableExceptionClasses().getExcludeList() != null) {
            final List<ExceptionClassFilter.Exclude> excludes = chunk.getSkippableExceptionClasses().getExcludeList();
            for (final ExceptionClassFilter.Exclude exclude : excludes) {
                config.getExcludes().add(exclude.getClazz().trim());
            }
        }
    }

    /**
     * Add the user-defined SkipReadListeners.
     */
    public void addSkipReadListener(List<SkipReadListener> skipReadListener) {
        _skipReadListener = skipReadListener;
    }

    /**
     * Add the user-defined SkipWriteListeners.
     */
    public void addSkipWriteListener(List<SkipWriteListener> skipWriteListener) {
        _skipWriteListener = skipWriteListener;
    }

    /**
     * Add the user-defined SkipReadListeners.
     */
    public void addSkipProcessListener(List<SkipProcessListener> skipProcessListener) {
        _skipProcessListener = skipProcessListener;
    }


    /**
     * Handle exception from a read failure.
     */
    public void handleExceptionRead(Exception e) {
        if (isSkipLimitReached() || !isSkippable(e)) {
            throw new BatchContainerRuntimeException(e);
        }

        // Skip it.  Log it.  Call the SkipListener.
        ++_skipCount;

        if (_skipReadListener != null) {
            for (final SkipReadListener skipReadListenerProxy : _skipReadListener) {
                try {
                    skipReadListenerProxy.onSkipReadItem(e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
        }
    }

    /**
     * Handle exception from a process failure.
     */
    public void handleExceptionWithRecordProcess(final Exception e, final Object w) {
        if (isSkipLimitReached() || !isSkippable(e)) {
            throw new BatchContainerRuntimeException(e);
        }

        // Skip it.  Log it.  Call the SkipProcessListener.
        ++_skipCount;

        if (_skipProcessListener != null) {
            for (SkipProcessListener skipProcessListenerProxy : _skipProcessListener) {
                try {
                    skipProcessListenerProxy.onSkipProcessItem(w, e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
        }
    }

    /**
     * Handle exception from a write failure.
     */
    public void handleExceptionWithRecordListWrite(final Exception e, final List<Object> items) {
        if (isSkipLimitReached() || !isSkippable(e)) {
            throw new BatchContainerRuntimeException(e);
        }

        // Skip it.  Log it.  Call the SkipListener.
        ++_skipCount;

        if (_skipWriteListener != null) {
            for (SkipWriteListener skipWriteListenerProxy : _skipWriteListener) {
                try {
                    skipWriteListenerProxy.onSkipWriteItem(items, e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
        }
    }


    /**
     * Check the skipCount and skippable exception lists to determine whether
     * the given Exception is skippable.
     */
    private boolean isSkippable(final Exception e) {
        return config.accept(e);
    }

    /**
     * Check if the skip limit has been reached.
     * <p/>
     * Note: if skip handling isn't enabled (i.e. not configured in xJCL), then
     * this method will always return TRUE.
     */
    private boolean isSkipLimitReached() {
        // Unlimited skips if it is never defined
        return _skipLimit != Integer.MIN_VALUE && (_skipCount >= _skipLimit);

    }

    public String toString() {
        return "SkipHandler{" + super.toString() + "}count:limit=" + _skipCount + ":" + _skipLimit;
    }
}
