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
package org.apache.batchee.cli.command;

import org.apache.batchee.cli.command.api.Command;

import javax.batch.operations.JobOperator;
import javax.batch.operations.NoSuchJobException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Command(name = "running", description = "list running batches")
public class Running extends JobOperatorCommand {
    @Override
    public void doRun() {
        final JobOperator operator = operator();
        final Set<String> names = operator.getJobNames();
        if (names == null || names.isEmpty()) {
            info("No job started");
        } else {
            final Map<String, List<Long>> runnings = new HashMap<String, List<Long>>();
            for (final String name : names) {
                try {
                    final List<Long> running = operator.getRunningExecutions(name);
                    if (running != null) {
                        runnings.put(name, running);
                    }
                } catch (final NoSuchJobException nsje) {
                    // no-op
                }
            }

            if (runnings.isEmpty()) {
                info("No job started");
            } else {
                for (final Map.Entry<String, List<Long>> entry : runnings.entrySet()) {
                    final List<Long> running = entry.getValue();
                    info(entry.getKey() + " -> " + Arrays.toString(running.toArray(new Long[running.size()])));
                }
            }
        }
    }
}
