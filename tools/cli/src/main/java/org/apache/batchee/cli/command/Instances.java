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
import org.apache.batchee.cli.command.api.Option;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.JobInstance;
import java.util.List;

@Command(name = "instances", description = "list instances")
public class Instances extends JobOperatorCommand {
    @Option(name = "name", description = "name of the batch to start", required = true)
    private String name;

    @Option(name = "start", description = "start of the list of job instance to query")
    private int start = 0;

    @Option(name = "count", description = "number of instance to query")
    private int count = 100;

    @Override
    public void doRun() {
        final JobOperator operator = operator();
        final long total = operator.getJobInstanceCount(name);
        info(name + " has " + total + " job instances");

        info("");
        info("instance id");
        info("-----------");
        final List<JobInstance> id = operator.getJobInstances(name, start, count);
        if (id != null) {
            for (final JobInstance instance : id) {
                info(Long.toString(instance.getInstanceId()));
            }
            info("-----------");
            info("Current/Total: " + (start + id.size()) + "/" + operator.getJobInstanceCount(name));
        } else {
            info("No instance found.");
        }
    }
}
