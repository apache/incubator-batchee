/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.cdi.impl;

import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import java.util.LinkedList;

public abstract class LocationHolder {
    private static final StashThreadLocal<JobContext> JOB = new StashThreadLocal<JobContext>();
    private static final StashThreadLocal<StepContext> STEP = new StashThreadLocal<StepContext>();

    protected static void enterJob(final JobContext jc) {
        JOB.get().add(jc);
    }

    protected static void enterStep(final StepContext sc) {
        STEP.get().add(sc);
    }

    protected static void exitStep(final BaseContext<?> context) {
        cleanUp(context, STEP);
    }

    protected static void exitJob(final BaseContext<?> context) {
        cleanUp(context, JOB);
    }

    public static JobContext currentJob() {
        final LinkedList<JobContext> jobContexts = JOB.get();
        if (jobContexts.isEmpty()) {
            throw new IllegalStateException("No job registered, did you set the job listener?");
        }
        return jobContexts.getLast();
    }

    public static LinkedList<StepContext> currentSteps() {
        return STEP.get();
    }

    private static <T, K> void cleanUp(final BaseContext<K> context, final StashThreadLocal<T> stash) {
        context.endContext();

        final LinkedList<T> stepContexts = stash.get();
        stepContexts.removeLast();
        if (stepContexts.isEmpty()) {
            stash.remove();
        }
    }

    private static class StashThreadLocal<T> extends ThreadLocal<LinkedList<T>> {
        @Override
        public LinkedList<T> initialValue() {
            return new LinkedList<T>();
        }
    }
}
