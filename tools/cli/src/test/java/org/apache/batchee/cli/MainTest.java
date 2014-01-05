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
package org.apache.batchee.cli;

import org.apache.batchee.cli.lifecycle.Lifecycle;
import org.apache.batchee.util.Batches;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.StandardOutputStreamLog;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

import static org.apache.batchee.cli.BatchEECLI.main;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MainTest {
    @Rule
    public final StandardOutputStreamLog stdout = new StandardOutputStreamLog();

    @Test
    public void start() {
        main(new String[]{ "start", "-name", "sample" });
        assertThat(stdout.getLog(), containsString("Batch 'sample' started with id #"));
        assertThat(stdout.getLog(), containsString("Batch status: COMPLETED"));
    }

    @Test
    public void startAndFail() {
        main(new String[]{ "start", "-name", "sample", "status=fail" });
        assertThat(stdout.getLog(), containsString("Batch 'sample' started with id #"));
        assertThat(stdout.getLog(), containsString("Batch status: FAILED"));
    }

    @Test
    public void restart() {
        main(new String[]{ "start", "-name", "sample", "status=fail" });
        final String str = "Batch 'sample' started with id #";

        assertThat(stdout.getLog(), containsString(str));
        final int idx = stdout.getLog().indexOf(str);
        final int end = stdout.getLog().indexOf(System.getProperty("line.separator"), idx);
        final String id = stdout.getLog().substring(idx + str.length(), end);

        main(new String[]{ "restart", "-id", id, "status=ok" });
        assertThat(stdout.getLog(), containsString(str));
        assertThat(stdout.getLog(), containsString("Batch status: COMPLETED"));
    }

    @Test
    public void emptyRunning() {
        main(new String[]{ "running" });
        assertThat(stdout.getLog(), containsString("No job started"));
    }

    @Test
    public void running() {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        final long id = jobOperator.start("long-sample", null);

        main(new String[]{"running"});
        assertThat(stdout.getLog(), containsString("long-sample -> ["));

        Batches.waitForEnd(jobOperator, id);
    }

    @Test
    public void stop() { // abandon is the same
        final Thread start = new Thread() {
            @Override
            public void run() {
                main(new String[]{ "start", "-name", "long-sample", "-socket", "1236", "-wait", "false" });
            }
        };
        start.run();

        final String str = "Batch 'long-sample' started with id #";

        final String out = stdout.getLog();
        int idx;
        do {
            idx = out.indexOf(str);
        } while (idx < 0);
        final int end = out.indexOf(System.getProperty("line.separator"));
        final long id = Long.parseLong(out.substring(idx + str.length(), end));

        main(new String[]{"stop", "-id", Long.toString(id), "-socket", "1236"});
        assertThat(stdout.getLog(), containsString("Stopped"));

        Batches.waitForEnd(id);
    }

    @Test
    public void status() {
        // ensure we have at least one thing to print
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        final long id = jobOperator.start("sample", null);

        main(new String[]{"status"});

        // output looks like
        //      Name   	|	execution id	|	batch status	|	exit status	|	start time	|	end time
        //      sample	|	           1	|	   COMPLETED	|	  COMPLETED	|	sam. janv. 04 17:22:51 CET 2014	|	sam. janv. 04 17:22:51 CET 2014


        assertThat(stdout.getLog(), containsString("sample\t|"));
        assertThat(stdout.getLog(), containsString("COMPLETED"));

        Batches.waitForEnd(jobOperator, id);
    }

    @Test
    public void instances() {
        // ensure we have at least one thing to print
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        final long id = jobOperator.start("sample", null);

        main(new String[]{"instances", "-name", "sample"});

        // output looks like:
        // sample has 3 job instances
        //
        // instance id
        // -----------
        // 0
        // 1


        assertThat(stdout.getLog(), containsString("sample has"));
        assertThat(stdout.getLog(), containsString("instance id"));

        Batches.waitForEnd(jobOperator, id);
    }

    @Test
    public void executions() {
        // ensure we have at least one thing to print
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        final long id = jobOperator.start("sample", null);

        main(new String[]{"executions", "-id", Long.toString(id)});

        // output looks like:
        // Executions of sample for instance 5
        // execution id	|	batch status	|	exit status	|	start time	|	end time
        //          5	|	   COMPLETED	|	  COMPLETED	|	sam. janv. 04 17:20:24 CET 2014	|	sam. janv. 04 17:20:24 CET 2014


        assertThat(stdout.getLog(), containsString("Executions of sample for instance 5"));
        assertThat(stdout.getLog(), containsString("COMPLETED"));

        Batches.waitForEnd(jobOperator, id);
    }

    @Test
    public void lifecycle() {
        // whatever child of JobOperatorCommand so using running which is simple
        main(new String[]{ "running", "-lifecycle", MyLifecycle.class.getName() });
        assertEquals("start stop", MyLifecycle.result);
    }

    public static class MyLifecycle implements Lifecycle<String> {
        private static String result;

        @Override
        public String start() {
            return "start";
        }

        @Override
        public void stop(final String state) {
            result = state + " stop";
        }
    }
}
