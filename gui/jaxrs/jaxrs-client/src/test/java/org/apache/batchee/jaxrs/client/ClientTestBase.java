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
package org.apache.batchee.jaxrs.client;

import com.github.rmannibucau.featuredmock.http.FeaturedHttpServer;
import com.github.rmannibucau.featuredmock.http.FeaturedHttpServerBuilder;
import com.github.rmannibucau.featuredmock.http.RequestObserver;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.batchee.jaxrs.client.impl.JobInstanceImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;
import javax.ws.rs.ext.RuntimeDelegate;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class ClientTestBase {
    private static int port;
    private static FeaturedHttpServer mockServer;
    private JobOperator client;

    @BeforeClass
    public static void startServer() throws IOException {
        mockServer = new FeaturedHttpServerBuilder().port(-1).observer(new RequestObserver() {
            @Override
            public void onRequest(final FullHttpRequest request) {
                if (request.getUri().endsWith("start/simple")) {
                    final String actual = new String(request.content().array());
                    assertEquals(actual, "{\"entries\":[{\"key\":\"a\",\"value\":\"b\"}]}");
                }
            }
        }).build().start();
        port = mockServer.getPort();
        RuntimeDelegate.setInstance(null); // reset
    }

    @BeforeMethod
    public void createClient() {
        client = newJobOperator(port);
    }

    protected abstract JobOperator newJobOperator(int port);

    @AfterClass
    public static void shutdown() {
        mockServer.stop();
    }

    @Test
    public void start() {
        assertEquals(1L, client.start("simple", new Properties() {{
            setProperty("a", "b");
        }}));
    }

    @Test
    public void jobNames() {
        final Set<String> names = client.getJobNames();
        assertNotNull(names);
        assertEquals(2, names.size());
        assertTrue(names.contains("s2"));
        assertTrue(names.contains("s1"));
    }

    @Test
    public void jobInstancesCount() {
        assertEquals(5, client.getJobInstanceCount("ajob"));
    }

    @Test
    public void getJobInstances() {
        final List<JobInstance> jobInstances = client.getJobInstances("anotherjob", 0, 30);
        assertNotNull(jobInstances);
        assertEquals(2, jobInstances.size());
        assertEquals("owb", jobInstances.get(0).getJobName());
        assertEquals(0, jobInstances.get(0).getInstanceId());
        assertEquals("tomee", jobInstances.get(1).getJobName());
        assertEquals(1, jobInstances.get(1).getInstanceId());
    }

    @Test
    public void getRunningExecutions() {
        final List<Long> running = client.getRunningExecutions("running");
        assertNotNull(running);
        assertEquals(2, running.size());
        assertEquals(Long.valueOf(1), running.get(0));
        assertEquals(Long.valueOf(2), running.get(1));
    }

    @Test
    public void getJobInstance() {
        final JobInstance jobInstance = client.getJobInstance(2);
        assertNotNull(jobInstance);
        assertEquals("ji", jobInstance.getJobName());
        assertEquals(12, jobInstance.getInstanceId());
    }

    @Test
    public void getParameters() {
        final Properties params = client.getParameters(54);
        assertNotNull(params);
        assertEquals(2, params.size());
        assertEquals("d", params.getProperty("c"));
        assertEquals("b", params.getProperty("a"));
    }

    @Test
    public void getJobExecution() {
        final JobExecution execution = client.getJobExecution(159);
        assertEquals(753, execution.getExecutionId());
        assertEquals("COMPLETED", execution.getBatchStatus().name());
        assertEquals("exit", execution.getExitStatus());
        assertEquals("job", execution.getJobName());
    }

    @Test
    public void getJobExecutions() {
        final List<JobExecution> jobExecutions = client.getJobExecutions(new JobInstanceImpl("job", 45896));
        assertNotNull(jobExecutions);
        assertEquals(2, jobExecutions.size());
        assertEquals("FAILED", jobExecutions.get(0).getBatchStatus().name());
        assertEquals("out", jobExecutions.get(0).getExitStatus());
        assertEquals("j0", jobExecutions.get(0).getJobName());
        assertEquals("COMPLETED", jobExecutions.get(1).getBatchStatus().name());
        assertEquals("es", jobExecutions.get(1).getExitStatus());
        assertEquals("j1", jobExecutions.get(1).getJobName());
    }

    @Test
    public void getStepExecutions() {
        final List<StepExecution> stepExecutions = client.getStepExecutions(8946);
        assertNotNull(stepExecutions);
        assertEquals(2, stepExecutions.size());
        assertEquals("FAILED", stepExecutions.get(0).getBatchStatus().name());
        assertEquals("out", stepExecutions.get(0).getExitStatus());
        assertEquals("j0", stepExecutions.get(0).getStepName());
        assertEquals("COMPLETED", stepExecutions.get(1).getBatchStatus().name());
        assertEquals("es", stepExecutions.get(1).getExitStatus());
        assertEquals("j1", stepExecutions.get(1).getStepName());
    }
}
