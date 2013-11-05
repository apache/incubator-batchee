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
import org.apache.batchee.container.proxy.SkipProcessListenerProxy;
import org.apache.batchee.container.proxy.SkipReadListenerProxy;
import org.apache.batchee.container.proxy.SkipWriteListenerProxy;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.ExceptionClassFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SkipHandler {

    /**
     * Logic for handling skipped records.
     */

    private List<SkipProcessListenerProxy> _skipProcessListener = null;
    private List<SkipReadListenerProxy> _skipReadListener = null;
    private List<SkipWriteListenerProxy> _skipWriteListener = null;

    private Set<String> _skipIncludeExceptions = null;
    private Set<String> _skipExcludeExceptions = null;
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

        _skipIncludeExceptions = new HashSet<String>();
        _skipExcludeExceptions = new HashSet<String>();

        if (chunk.getSkippableExceptionClasses() != null && chunk.getSkippableExceptionClasses().getIncludeList() != null) {
            final List<ExceptionClassFilter.Include> includes = chunk.getSkippableExceptionClasses().getIncludeList();
            for (final ExceptionClassFilter.Include include : includes) {
                _skipIncludeExceptions.add(include.getClazz().trim());
            }
        }

        if (chunk.getSkippableExceptionClasses() != null && chunk.getSkippableExceptionClasses().getExcludeList() != null) {
            final List<ExceptionClassFilter.Exclude> excludes = chunk.getSkippableExceptionClasses().getExcludeList();
            for (final ExceptionClassFilter.Exclude exclude : excludes) {
                _skipExcludeExceptions.add(exclude.getClazz().trim());
            }
        }
    }

    /**
     * Add the user-defined SkipReadListeners.
     */
    public void addSkipReadListener(List<SkipReadListenerProxy> skipReadListener) {
        _skipReadListener = skipReadListener;
    }

    /**
     * Add the user-defined SkipWriteListeners.
     */
    public void addSkipWriteListener(List<SkipWriteListenerProxy> skipWriteListener) {
        _skipWriteListener = skipWriteListener;
    }

    /**
     * Add the user-defined SkipReadListeners.
     */
    public void addSkipProcessListener(List<SkipProcessListenerProxy> skipProcessListener) {
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
            for (final SkipReadListenerProxy skipReadListenerProxy : _skipReadListener) {
                skipReadListenerProxy.onSkipReadItem(e);
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
            for (SkipProcessListenerProxy skipProcessListenerProxy : _skipProcessListener) {
                skipProcessListenerProxy.onSkipProcessItem(w, e);
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
            for (SkipWriteListenerProxy skipWriteListenerProxy : _skipWriteListener) {
                skipWriteListenerProxy.onSkipWriteItem(items, e);
            }
        }
    }


    /**
     * Check the skipCount and skippable exception lists to determine whether
     * the given Exception is skippable.
     */
    private boolean isSkippable(final Exception e) {
        return containsSkippable(_skipIncludeExceptions, e) && !containsSkippable(_skipExcludeExceptions, e);
    }

    /**
     * Check whether given exception is in skippable exception list
     */
    private boolean containsSkippable(final Set<String> skipList, final Exception e) {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        for (final String exClassName : skipList) {
            try {
                if (tccl.loadClass(exClassName).isInstance(e)) {
                    return true;
                }
            } catch (final ClassNotFoundException cnf) {
                // no-op
            }
        }

        return false;
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
