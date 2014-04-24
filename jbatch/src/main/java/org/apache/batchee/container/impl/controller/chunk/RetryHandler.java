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

import javax.batch.api.chunk.listener.RetryProcessListener;
import javax.batch.api.chunk.listener.RetryReadListener;
import javax.batch.api.chunk.listener.RetryWriteListener;

public class RetryHandler {
    private List<RetryProcessListener> _retryProcessListeners = null;
    private List<RetryReadListener> _retryReadListeners = null;
    private List<RetryWriteListener> _retryWriteListeners = null;

    private ExceptionConfig retryNoRBConfig = new ExceptionConfig();
    private ExceptionConfig retryConfig = new ExceptionConfig();
    private int _retryLimit = Integer.MIN_VALUE;
    private long _retryCount = 0;
    private Exception _retryException = null;

    public RetryHandler(final Chunk chunk) {
        try {
            if (chunk.getRetryLimit() != null) {
                _retryLimit = Integer.parseInt(chunk.getRetryLimit());
                if (_retryLimit < 0) {
                    throw new IllegalArgumentException("The retry-limit attribute on a chunk cannot be a negative value");
                }

            }
        } catch (final NumberFormatException nfe) {
            throw new RuntimeException("NumberFormatException reading retry-limit", nfe);
        }

        if (chunk.getRetryableExceptionClasses() != null) {
            if (chunk.getRetryableExceptionClasses().getIncludeList() != null) {
                final List<ExceptionClassFilter.Include> includes = chunk.getRetryableExceptionClasses().getIncludeList();
                for (final ExceptionClassFilter.Include include : includes) {
                    retryConfig.getIncludes().add(include.getClazz().trim());
                }
            }
            if (chunk.getRetryableExceptionClasses().getExcludeList() != null) {
                final List<ExceptionClassFilter.Exclude> excludes = chunk.getRetryableExceptionClasses().getExcludeList();
                for (final ExceptionClassFilter.Exclude exclude : excludes) {
                    retryConfig.getExcludes().add(exclude.getClazz().trim());
                }
            }
        }

        if (chunk.getNoRollbackExceptionClasses() != null) {
            if (chunk.getNoRollbackExceptionClasses().getIncludeList() != null) {
                final List<ExceptionClassFilter.Include> includes = chunk.getNoRollbackExceptionClasses().getIncludeList();
                for (final ExceptionClassFilter.Include include : includes) {
                    retryNoRBConfig.getIncludes().add(include.getClazz().trim());
                }
            }
            if (chunk.getNoRollbackExceptionClasses().getExcludeList() != null) {
                final List<ExceptionClassFilter.Exclude> excludes = chunk.getNoRollbackExceptionClasses().getExcludeList();
                for (final ExceptionClassFilter.Exclude exclude : excludes) {
                    retryNoRBConfig.getExcludes().add(exclude.getClazz().trim());
                }
            }
        }
    }


    /**
     * Add the user-defined RetryProcessListener.
     */
    public void addRetryProcessListener(List<RetryProcessListener> retryProcessListeners) {
        _retryProcessListeners = retryProcessListeners;
    }

    /**
     * Add the user-defined RetryReadListener.
     */
    public void addRetryReadListener(List<RetryReadListener> retryReadListeners) {
        _retryReadListeners = retryReadListeners;
    }

    /**
     * Add the user-defined RetryWriteListener.
     */
    public void addRetryWriteListener(List<RetryWriteListener> retryWriteListeners) {
        _retryWriteListeners = retryWriteListeners;
    }

    public boolean isRollbackException(Exception e) {
        return !isNoRollbackException(e);
    }

    /**
     * Handle exception from a read failure.
     */
    public void handleExceptionRead(final Exception e) {
        if (isRetryLimitReached() || !isRetryable(e)) {
            throw new BatchContainerRuntimeException(e);
        }

        _retryException = e;
        // Retry it.  Log it.  Call the RetryListener.
        ++_retryCount;

        if (_retryReadListeners != null) {
            for (final RetryReadListener retryReadListenerProxy : _retryReadListeners) {
                try {
                    retryReadListenerProxy.onRetryReadException(e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
        }
    }

    /**
     * Handle exception from a process failure.
     */
    public void handleExceptionProcess(final Exception e, final Object w) {
        if (isRetryLimitReached() || !isRetryable(e)) {
            throw new BatchContainerRuntimeException(e);
        }

        _retryException = e;
        // Retry it.  Log it.  Call the RetryListener.
        ++_retryCount;

        if (_retryProcessListeners != null) {
            for (final RetryProcessListener retryProcessListenerProxy : _retryProcessListeners) {
                try {
                    retryProcessListenerProxy.onRetryProcessException(w, e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
        }
    }

    /**
     * Handle exception from a write failure.
     */
    public void handleExceptionWrite(final Exception e, final List<Object> w) {
        if (isRetryLimitReached() || !isRetryable(e)) {
            throw new BatchContainerRuntimeException(e);
        }

        // Retry it.  Log it.  Call the RetryListener.
        _retryException = e;
        ++_retryCount;

        if (_retryWriteListeners != null) {
            for (final RetryWriteListener retryWriteListenerProxy : _retryWriteListeners) {
                try {
                    retryWriteListenerProxy.onRetryWriteException(w, e);
                } catch (Exception e1) {
                    ExceptionConfig.wrapBatchException(e1);
                }
            }
        }
    }


    /**
     * Check the retryable exception lists to determine whether
     * the given Exception is retryable.
     */
    private boolean isRetryable(final Exception e) {
        return retryConfig.accept(e);
    }

    private boolean isNoRollbackException(final Exception e) {
        return retryNoRBConfig.accept(e);
    }

    /**
     * Check if the retry limit has been reached.
     * <p/>
     * Note: if retry handling isn't enabled (i.e. not configured in xJCL), then this method
     * will always return TRUE.
     */
    private boolean isRetryLimitReached() {
        // Unlimited retries if it is never defined
        return _retryLimit != Integer.MIN_VALUE && (_retryCount >= _retryLimit);

    }

    public Exception getException() {
        return _retryException;
    }

    public String toString() {
        return "RetryHandler{" + super.toString() + "}count:limit=" + _retryCount + ":" + _retryLimit;
    }
}
