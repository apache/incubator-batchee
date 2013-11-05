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
package org.apache.batchee.servlet;

import javax.batch.operations.BatchRuntimeException;
import javax.batch.operations.JobExecutionNotRunningException;
import javax.batch.operations.JobOperator;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JBatchController extends HttpServlet {
    private static final String DEFAULT_MAPPING_SERVLET25 = "/jbatch";
    private static final int DEFAULT_PAGE_SIZE = 30;

    public static final String FORM_JOB_NAME = "___batchee_job_name___";

    private static final String EXECUTIONS_MAPPING = "/executions/";
    private static final String STEP_EXECUTIONS_MAPPING = "/step-executions/";
    private static final String START_MAPPING = "/start/";
    private static final String DO_START_MAPPING = "/doStart/";
    private static final String VIEW_MAPPING = "/view/";

    private JobOperator operator;

    private String context;
    private String mapping = DEFAULT_MAPPING_SERVLET25;
    private int executionByPage = DEFAULT_PAGE_SIZE;

    public JBatchController mapping(final String rawMapping) {
        this.mapping = rawMapping.substring(0, rawMapping.length() - 2); // mapping pattern is /xxx/*
        return this;
    }

    public JBatchController executionByPage(final int byPage) {
        executionByPage = byPage;
        return this;
    }

    @Override
    public void init(final ServletConfig config) throws ServletException {
        this.operator = BatchRuntime.getJobOperator();

        this.context = config.getServletContext().getContextPath();
        if ("/".equals(context)) {
            context = "";
        }

        mapping = context + mapping;
    }

    @Override
    protected void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        resp.setCharacterEncoding("UTF-8");
        req.setAttribute("context", context);
        req.setAttribute("mapping", mapping);

        final String path = req.getPathInfo();
        if (path.startsWith(EXECUTIONS_MAPPING)) {
            final String name = URLDecoder.decode(path.substring(EXECUTIONS_MAPPING.length()), "UTF-8");
            final int start = extractInt(req, "start", -1);
            listExecutions(req, name, executionByPage, start);
        } else if (path.startsWith(STEP_EXECUTIONS_MAPPING)) {
            final int executionId = Integer.parseInt(path.substring(STEP_EXECUTIONS_MAPPING.length()));
            listStepExecutions(req, executionId);
        } else if (path.startsWith(VIEW_MAPPING)) {
            final String name = URLDecoder.decode(path.substring(VIEW_MAPPING.length()), "UTF-8");
            view(req, name);
        } else if (path.startsWith(START_MAPPING)) {
            final String name = URLDecoder.decode(path.substring(START_MAPPING.length()), "UTF-8");
            start(req, name);
        } else if (path.startsWith(DO_START_MAPPING)) {
            String name = URLDecoder.decode(path.substring(DO_START_MAPPING.length()), "UTF-8");
            if (name.isEmpty()) {
                name = req.getParameter(FORM_JOB_NAME);
            }

            doStart(req, name, readProperties(req));
        } else {
            listJobs(req);
        }

        req.getRequestDispatcher("/internal/batchee/layout.jsp").forward(req, resp);
    }

    private void view(final HttpServletRequest req, final String name) {
        req.setAttribute("name", name);
        req.setAttribute("view", "view");

        // we copy the logic from jbatch module since the GUI is not (yet?) linked to our impl
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        final String relativePath = "META-INF/batch-jobs/" + name + ".xml";
        final InputStream stream = tccl.getResourceAsStream(relativePath);
        if (stream == null) {
            throw new BatchRuntimeException(new FileNotFoundException("Cannot find an XML for " + name));
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int r;
        try {
            while ((r = stream.read()) != -1) {
                baos.write(r);
            }
        } catch (final IOException e) {
            throw new BatchRuntimeException(new FileNotFoundException("Cannot find an XML for " + name));
        }

        req.setAttribute("content", new String(baos.toByteArray())
                                    .replace("&","&amp;")
                                    .replace("<", "&lt;")
                                    .replace(">", "&gt;")
                                    .replace("\"", "&quot;")
                                    .replace("'", "&apos;"));
    }

    private void doStart(final HttpServletRequest req, final String name, final Properties properties) {
        req.setAttribute("id", operator.start(name, properties));
        req.setAttribute("name", name);
        req.setAttribute("view", "after-start");
    }

    private void start(final HttpServletRequest req, final String name) {
        req.setAttribute("view", "start");
        req.setAttribute("name", name);
    }

    private void listStepExecutions(final HttpServletRequest req, final int executionId) {
        final List<StepExecution> steps = operator.getStepExecutions(executionId);
        req.setAttribute("view", "step-executions");
        req.setAttribute("steps", steps);
        req.setAttribute("executionId", executionId);
        req.setAttribute("name", operator.getJobExecution(executionId).getJobName());
    }

    private void listExecutions(final HttpServletRequest req, final String name, final int pageSize, final int inStart) {
        { // can be an auto refresh asking for a stop
            final String stopId = req.getParameter("stop");
            if (stopId != null) {
                try {
                    operator.stop(Long.parseLong(stopId));
                } catch (final NoSuchJobExecutionException nsje) {
                    // no-op
                } catch (final JobExecutionNotRunningException nsje) {
                    // no-op
                }
            }
        }

        final int jobInstanceCount = operator.getJobInstanceCount(name);

        int start = inStart;
        if (start == -1) { // first page is last page
            start = Math.max(0, jobInstanceCount - pageSize);
        }

        final List<JobInstance> instances = new ArrayList<JobInstance>(operator.getJobInstances(name, start, pageSize));
        Collections.sort(instances, JobInstanceIdComparator.INSTANCE);

        final Map<JobInstance, List<JobExecution>> executions = new LinkedHashMap<JobInstance, List<JobExecution>>();
        for (final JobInstance instance : instances) {
            executions.put(instance, operator.getJobExecutions(instance));
        }

        req.setAttribute("view", "job-instances");
        req.setAttribute("name", name);
        req.setAttribute("executions", executions);

        int nextStart = start + pageSize;
        if (nextStart > jobInstanceCount) {
            nextStart = -1;
        }
        req.setAttribute("nextStart", nextStart);

        req.setAttribute("previousStart", start - pageSize);

        if (jobInstanceCount > pageSize) {
            req.setAttribute("lastStart", Math.max(0, jobInstanceCount - pageSize));
        } else {
            req.setAttribute("lastStart", -1);
        }
    }

    private void listJobs(final HttpServletRequest req) throws ServletException, IOException {
        Set<String> names = operator.getJobNames();
        if (names == null) {
            names = Collections.emptySet();
        }

        req.setAttribute("view", "jobs");
        req.setAttribute("names", names);
    }

    private static int extractInt(final HttpServletRequest req, final String name, final int defaultValue) {
        final String string = req.getParameter(name);
        if (string != null) {
            return Integer.parseInt(string);
        }
        return defaultValue;
    }

    private static Properties readProperties(final HttpServletRequest req) {
        final Map<String, String> map = new HashMap<String, String>();
        final Enumeration<String> names = req.getParameterNames();
        while (names.hasMoreElements()) {
            final String key = names.nextElement();
            if (FORM_JOB_NAME.equals(key)) {
                continue;
            }

            map.put(key, req.getParameter(key));
        }

        final Properties properties = new Properties();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith("k_")) {
                final String name = key.substring("k_".length());
                properties.setProperty(name, map.get("v_" + name));
            }
        }
        return properties;
    }

    private static class JobInstanceIdComparator implements java.util.Comparator<JobInstance> {
        private static final JobInstanceIdComparator INSTANCE = new JobInstanceIdComparator();

        @Override
        public int compare(final JobInstance o1, final JobInstance o2) {
            return (int) (o2.getInstanceId() - o1.getInstanceId()); // reverse order since users will prefer last first
        }
    }
}
