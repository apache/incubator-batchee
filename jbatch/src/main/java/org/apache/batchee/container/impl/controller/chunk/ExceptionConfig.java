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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;

public class ExceptionConfig {
    protected final Set<String> includes = new HashSet<String>();
    protected final Set<String> excludes = new HashSet<String>();
    private final ConcurrentMap<Class<? extends Exception>, Boolean> cache  = new ConcurrentHashMap<Class<? extends Exception>, Boolean>();

    /**
     * Helper method to wrap unknown Exceptions into a {@link org.apache.batchee.container.exception.BatchContainerRuntimeException}.
     * This method can be used to handle Exceptions which are actually already catched inside of our proxies.
     */
    public static void wrapBatchException(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new BatchContainerRuntimeException(e);
        }
    }

    public Set<String> getIncludes() {
        return includes;
    }

    public Set<String> getExcludes() {
        return excludes;
    }

    public boolean accept(final Exception e) {
        if (e == null) {
            return false;
        }

        final Class<? extends Exception> key = e.getClass();
        final Boolean computed = cache.get(key);
        if (computed != null) {
            return computed;
        }

        final int includeScore = contains(includes, e);
        final int excludeScore = contains(excludes, e);

        if (excludeScore < 0) {
            final boolean result = includeScore >= 0;
            cache.putIfAbsent(key, result);
            return result;
        }

        final boolean result = includeScore >= 0 && includeScore - excludeScore <= 0;
        cache.putIfAbsent(key, result);
        return result;
    }

    private static int contains(final Set<String> list, final Exception e) {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        int score = -1;
        for (final String exClassName : list) {
            try {
                final Class<?> aClass = tccl.loadClass(exClassName);
                if (aClass.isInstance(e)) {
                    final int thisScore = score(aClass, e.getClass());
                    if (score < 0) {
                        score = thisScore;
                    } else {
                        score = Math.min(thisScore, score);
                    }
                }
            } catch (final ClassNotFoundException cnfe) {
                // no-op
            } catch (final NoClassDefFoundError ncdfe) {
                // no-op
            }
        }

        return score;
    }

    private static int score(final Class<?> config, final Class<?> ex) {
        int score = 0;
        Class<?> current = ex;
        while (current != null && !current.equals(config)) {
            score++;
            current = current.getSuperclass();
        }
        return score;
    }

}
