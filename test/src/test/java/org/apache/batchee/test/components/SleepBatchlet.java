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
package org.apache.batchee.test.components;

import javax.batch.api.BatchProperty;
import javax.batch.api.Batchlet;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;

public class SleepBatchlet implements Batchlet {
    public static final int SLEEP_DURATION = 50;

    @Inject
    @BatchProperty
    private String duration = "50";

    @Inject
    @BatchProperty
    private String exitStatus;

    @Inject
    private StepContext stepContext;

    private volatile boolean stopped = false;

    @Override
    public String process() throws Exception {
        if (exitStatus != null) {
            stepContext.setExitStatus(exitStatus);
            return exitStatus;
        }

        final long pauses = Long.parseLong(duration) / SLEEP_DURATION;
        for (long i = 0; i < pauses; i++) {
            if (stopped) {
                return "STOP";
            }
            Thread.sleep(SLEEP_DURATION);
        }
        return "OK";
    }

    @Override
    public void stop() throws Exception {
        stopped = true;
    }
}
