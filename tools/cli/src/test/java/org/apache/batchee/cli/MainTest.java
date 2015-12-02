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
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.StandardErrorStreamLog;
import org.junit.contrib.java.lang.system.StandardOutputStreamLog;
import org.junit.runners.MethodSorters;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

import static java.lang.Thread.sleep;
import static org.apache.batchee.cli.BatchEECLI.main;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MainTest {
    @Rule
    public final StandardOutputStreamLog stdout = new StandardOutputStreamLog();

    @Rule
    public final StandardErrorStreamLog stderr = new StandardErrorStreamLog();

    @Test
    public void argHelp() {
        main(null);
        assertEquals(
            "Available commands:\n\n" +
            "abandon: abandon a batch from its id\n" +
            "evict: remove old data, uses embedded configuration (no JAXRS support yet)\n" +
            "executions: list executions\n" +
            "instances: list instances\n" +
            "names: list known batches\n" +
            "restart: restart a batch\n" +
            "running: list running batches\n" +
            "start: start a batch\n" +
            "status: list last batches statuses\n" +
            "stepExecutions: list step executions for a particular execution\n" +
            "stop: stop a batch from its id\n" +
            "user1\n" +
            "user2\n", stderr.getLog().replace(System.getProperty("line.separator"), "\n"));
    }

    @Test
    public void helpCommand() {
        main(new String[] { "help", "evict" }); // using a simple command to avoid a big block for nothing
        assertEquals(
            "usage: evict -until <arg>\n" +
            "\n" +
            "remove old data, uses embedded configuration (no JAXRS support yet)\n" +
            "\n" +
            " -until <arg>   date until when the eviction will occur (excluded),\n" +
            "                YYYYMMDD format\n", stdout.getLog().replace(System.getProperty("line.separator"), "\n"));
    }

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

        try {
            sleep(100); // ensure it is started
        } catch (final InterruptedException e) {
            Thread.interrupted();
        }
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
        final int end = out.lastIndexOf(System.getProperty("line.separator"));
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

        // output looks like:
        // Executions of sample for instance 5
        // execution id	|	batch status	|	exit status	|	start time	|	end time
        //          5	|	   COMPLETED	|	  COMPLETED	|	sam. janv. 04 17:20:24 CET 2014	|	sam. janv. 04 17:20:24 CET 2014


        Batches.waitForEnd(jobOperator, id);
        main(new String[]{"executions", "-id", Long.toString(id)});

        assertThat(stdout.getLog(), containsString("Executions of sample for instance " + id));
        assertThat(stdout.getLog(), containsString("COMPLETED"));
    }

    @Test
    public void stepExecutions() {
        // ensure we have at least one thing to print
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        final long id = jobOperator.start("sample", null);

        Batches.waitForEnd(jobOperator, id);
        main(new String[]{"stepExecutions", "-id", Long.toString(id)});

        assertThat(stdout.getLog(), containsString(
            "step id\t|\t step name\t|\t    start time   \t|\t     end time    \t|\t" +
            "exit status\t|\tbatch status\t|\t" +
            "READ_COUNT\t|\tWRITE_COUNT\t|\tCOMMIT_COUNT\t|\tROLLBACK_COUNT\t|\tREAD_SKIP_COUNT\t|\t" +
            "PROCESS_SKIP_COUNT\t|\tFILTER_COUNT\t|\tWRITE_SKIP_COUNT"));
        assertThat(stdout.getLog(), containsString("OK"));
        assertThat(stdout.getLog(), containsString("sample-step"));
        assertThat(stdout.getLog(), containsString("COMPLETED"));
    }

    @Test
    public void lifecycle() {
        // whatever child of JobOperatorCommand so using running which is simple
        main(new String[]{ "running", "-lifecycle", MyLifecycle.class.getName() });
        assertEquals("start stop", MyLifecycle.result);
    }

    @Test
    public void user() {
        for (int i = 1; i < 3; i++) {
            main(new String[]{"user" + i});
            assertThat(stdout.getLog(), containsString("User " + i + " called"));
        }
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
