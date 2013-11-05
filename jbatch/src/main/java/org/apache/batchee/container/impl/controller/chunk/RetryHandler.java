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
import org.apache.batchee.container.proxy.RetryProcessListenerProxy;
import org.apache.batchee.container.proxy.RetryReadListenerProxy;
import org.apache.batchee.container.proxy.RetryWriteListenerProxy;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.ExceptionClassFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RetryHandler {
    private List<RetryProcessListenerProxy> _retryProcessListeners = null;
    private List<RetryReadListenerProxy> _retryReadListeners = null;
    private List<RetryWriteListenerProxy> _retryWriteListeners = null;

    private Set<String> _retryNoRBIncludeExceptions = null;
    private Set<String> _retryNoRBExcludeExceptions = null;
    private Set<String> _retryIncludeExceptions = null;
    private Set<String> _retryExcludeExceptions = null;
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

        // Read the include/exclude exceptions.
        _retryIncludeExceptions = new HashSet<String>();
        _retryExcludeExceptions = new HashSet<String>();
        _retryNoRBIncludeExceptions = new HashSet<String>();
        _retryNoRBExcludeExceptions = new HashSet<String>();

        if (chunk.getRetryableExceptionClasses() != null) {
            if (chunk.getRetryableExceptionClasses().getIncludeList() != null) {
                final List<ExceptionClassFilter.Include> includes = chunk.getRetryableExceptionClasses().getIncludeList();
                for (final ExceptionClassFilter.Include include : includes) {
                    _retryIncludeExceptions.add(include.getClazz().trim());
                }
            }
            if (chunk.getRetryableExceptionClasses().getExcludeList() != null) {
                final List<ExceptionClassFilter.Exclude> excludes = chunk.getRetryableExceptionClasses().getExcludeList();
                for (final ExceptionClassFilter.Exclude exclude : excludes) {
                    _retryExcludeExceptions.add(exclude.getClazz().trim());
                }
            }
        }

        if (chunk.getNoRollbackExceptionClasses() != null) {
            if (chunk.getNoRollbackExceptionClasses().getIncludeList() != null) {
                final List<ExceptionClassFilter.Include> includes = chunk.getNoRollbackExceptionClasses().getIncludeList();
                for (final ExceptionClassFilter.Include include : includes) {
                    _retryNoRBIncludeExceptions.add(include.getClazz().trim());
                }
            }
            if (chunk.getNoRollbackExceptionClasses().getExcludeList() != null) {
                final List<ExceptionClassFilter.Exclude> excludes = chunk.getNoRollbackExceptionClasses().getExcludeList();
                for (final ExceptionClassFilter.Exclude exclude : excludes) {
                    _retryNoRBExcludeExceptions.add(exclude.getClazz().trim());
                }
            }
        }
    }


    /**
     * Add the user-defined RetryProcessListener.
     */
    public void addRetryProcessListener(List<RetryProcessListenerProxy> retryProcessListeners) {
        _retryProcessListeners = retryProcessListeners;
    }

    /**
     * Add the user-defined RetryReadListener.
     */
    public void addRetryReadListener(List<RetryReadListenerProxy> retryReadListeners) {
        _retryReadListeners = retryReadListeners;
    }

    /**
     * Add the user-defined RetryWriteListener.
     */
    public void addRetryWriteListener(List<RetryWriteListenerProxy> retryWriteListeners) {
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
            for (final RetryReadListenerProxy retryReadListenerProxy : _retryReadListeners) {
                retryReadListenerProxy.onRetryReadException(e);
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
            for (final RetryProcessListenerProxy retryProcessListenerProxy : _retryProcessListeners) {
                retryProcessListenerProxy.onRetryProcessException(w, e);
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
            for (final RetryWriteListenerProxy retryWriteListenerProxy : _retryWriteListeners) {
                retryWriteListenerProxy.onRetryWriteException(w, e);
            }
        }
    }


    /**
     * Check the retryable exception lists to determine whether
     * the given Exception is retryable.
     */
    private boolean isRetryable(final Exception e) {
        return containsException(_retryIncludeExceptions, e) && !containsException(_retryExcludeExceptions, e);
    }

    private boolean isNoRollbackException(final Exception e) {
        return containsException(_retryNoRBIncludeExceptions, e) && !containsException(_retryNoRBExcludeExceptions, e);
    }

    /**
     * Check whether given exception is in the specified exception list
     */
    private boolean containsException(final Set<String> retryList, final Exception e) {
        boolean retVal = false;

        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (final String exClassName : retryList) {
            try {
                retVal = classLoader.loadClass(exClassName).isInstance(e);
                if (retVal) {
                    break;
                }
            } catch (final ClassNotFoundException cnf) {
                // no-op
            }
        }

        return retVal;
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
