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
package org.apache.batchee.jaxrs.server;

import org.apache.batchee.jaxrs.common.JBatchResource;
import org.apache.batchee.jaxrs.common.RestEntry;
import org.apache.batchee.jaxrs.common.RestJobExecution;
import org.apache.batchee.jaxrs.common.RestJobInstance;
import org.apache.batchee.jaxrs.common.RestProperties;
import org.apache.batchee.jaxrs.common.RestStepExecution;
import org.apache.batchee.jaxrs.common.johnzon.JohnzonBatcheeProvider;
import org.apache.batchee.jaxrs.server.util.CreateSomeJobs;
import org.apache.cxf.interceptor.LoggingOutInterceptor;
import org.apache.cxf.jaxrs.client.WebClient;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OverProtocol;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.webapp30.WebAppDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.batch.runtime.BatchStatus;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URL;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Arquillian.class)
public class RestTest {
    @ArquillianResource
    private URL base;

    @Test
    public void getJobNames() {
        final String[] jobNames = newClient().path("job-names").get(String[].class);
        assertNotNull(jobNames);
        assertEquals(1, jobNames.length);
        assertEquals("init", jobNames[0]);
    }

    @Test
    public void getJobInstanceCount() {
        final int count = newClient().path("job-instance/count/{id}", "init").accept(MediaType.TEXT_PLAIN_TYPE).get(Integer.class);
        assertEquals(1, count);
    }

    @Test
    public void getJobInstances() {
        final RestJobInstance[] instances = newClient().path("job-instances/{name}", "init").query("start", 0).query("count", 10).get(RestJobInstance[].class);
        assertNotNull(instances);
        assertEquals(1, instances.length);
        assertEquals(0, instances[0].getId());
        assertEquals("init", instances[0].getName());
    }

    @Test
    public void getRunningExecutions() {
        final int status = newClient().path("executions/running/{name}", "init").get().getStatus();
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), status); // no more running so exception
    }

    @Test
    public void getJobInstance() {
        final RestJobInstance instance = newClient().path("job-instance/{id}", 0).get(RestJobInstance.class);
        assertNotNull(instance);
        assertEquals(0, instance.getId());
        assertEquals("init", instance.getName());
    }

    @Test
    public void getParameters() {
        final RestProperties params = newClient().path("execution/parameter/{id}", 0).get(RestProperties.class);
        assertNotNull(params);
        assertEquals(1, params.getEntries().size());

        final RestEntry entry = params.getEntries().iterator().next();
        assertEquals("test", entry.getKey());
        assertEquals("jbatch", entry.getValue());
    }

    @Test
    public void getJobExecutions() {
        final RestJobExecution[] executions = newClient().path("job-executions/{id}/{name}", 0, "init").get(RestJobExecution[].class);
        assertNotNull(executions);
        assertEquals(1, executions.length);
        assertEquals(0, executions[0].getId());
        assertEquals("init", executions[0].getName());
        assertEquals("COMPLETED", executions[0].getExitStatus());
        assertEquals(BatchStatus.COMPLETED, executions[0].getBatchStatus());
    }

    @Test
    public void getJobExecution() {
        final RestJobExecution executions = newClient().path("job-execution/{id}", 0).get(RestJobExecution.class);
        assertNotNull(executions);
        assertEquals(0, executions.getId());
        assertEquals("init", executions.getName());
        assertEquals("COMPLETED", executions.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, executions.getBatchStatus());
    }

    @Test
    public void getStepExecutions() {
        final RestStepExecution[] executions = newClient().path("step-executions/{id}", 0).get(RestStepExecution[].class);
        assertNotNull(executions);
        assertEquals(1, executions.length);
        assertEquals(0, executions[0].getId());
        assertEquals("step1", executions[0].getName());
        assertEquals("OK", executions[0].getExitStatus());
        assertEquals(BatchStatus.COMPLETED, executions[0].getBatchStatus());
    }

    @Deployment(testable = false)
    @OverProtocol("Servlet 2.5") // to use a custom web.xml
    public static Archive<?> war() {
        return ShrinkWrap.create(WebArchive.class, "batchee-gui.war")
            // GUI
            .addPackages(true, JBatchResourceImpl.class.getPackage())
            .addPackages(true, JBatchResource.class.getPackage())
            .addAsWebInfResource(new StringAsset(
                Descriptors.create(WebAppDescriptor.class)
                    .metadataComplete(false)
                    .createServlet()
                    .servletName("CXF")
                    .servletClass("org.apache.cxf.jaxrs.servlet.CXFNonSpringJaxrsServlet")
                    .createInitParam()
                    .paramName("jaxrs.serviceClasses")
                    .paramValue(JBatchResourceImpl.class.getName())
                    .up()
                    .createInitParam()
                    .paramName("jaxrs.providers")
                    .paramValue(JohnzonBatcheeProvider.class.getName() + "," + JBatchExceptionMapper.class.getName())
                    .up()
                    .createInitParam()
                    .paramName("jaxrs.outInterceptors")
                    .paramValue(LoggingOutInterceptor.class.getName())
                    .up()
                    .up()
                    .createServletMapping()
                    .servletName("CXF")
                    .urlPattern("/api/*")
                    .up()
                    .exportAsString()
            ), "web.xml")
            // test data to create some job things to do this test
            .addPackage(CreateSomeJobs.class.getPackage())
            .addAsWebInfResource("META-INF/batch-jobs/init.xml", "classes/META-INF/batch-jobs/init.xml");
    }

    private WebClient newClient() {
        return WebClient.create(base.toExternalForm() + "api/batchee", singletonList(new JohnzonBatcheeProvider())).accept(MediaType.APPLICATION_JSON_TYPE);
    }
}
