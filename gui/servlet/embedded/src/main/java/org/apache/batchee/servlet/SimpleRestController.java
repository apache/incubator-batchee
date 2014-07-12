/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

import javax.batch.operations.JobOperator;
import javax.batch.operations.JobStartException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Simple REST api for JBatch
 */
public class SimpleRestController {
    public static final String REST_CONTENT_TYPE = "text/plain";

    public static final String OP_START = "start/";
    public static final String OP_STATUS = "status/";

    public static final long NO_JOB_ID = -1L;

    private final JobOperator jobOperator;

    public SimpleRestController(JobOperator jobOperator) {
        this.jobOperator = jobOperator;
    }


    /**
     * The main entry for the simple REST handling
     */
    public void dispatch(HttpServletRequest req, HttpServletResponse resp, String path) {
        resp.setContentType(REST_CONTENT_TYPE);

        if (path != null && path.startsWith(OP_START)) {
            startBatch(path.substring(OP_START.length()), req, resp);
        } else if (path != null && path.startsWith(OP_STATUS)) {
            batchStatus(path.substring(OP_STATUS.length()), req, resp);
        } else {
            unknownCommand(path, resp);
        }
    }

    private void startBatch(String batchName, HttpServletRequest req, HttpServletResponse resp) {
        Properties jobProperties = new Properties();
        Map<String, String[]> parameterMap = req.getParameterMap();
        for (Map.Entry<String, String[]> paramEntry : parameterMap.entrySet()) {
            String key = paramEntry.getKey();
            if (key == null || key.length() == 0) {
                reportFailure(NO_JOB_ID, resp, "Parameter key must be set");
                return;
            }

            String[] vals = paramEntry.getValue();
            if (vals == null || vals.length != 1) {
                reportFailure(NO_JOB_ID, resp, "Exactly one value must be set for each parameter (parameter name=" + key + ")");
                return;
            }
            String val = vals[0];
            jobProperties.put(key, val);
        }

        try {
            long jobId = jobOperator.start(batchName, jobProperties);
            reportSuccess(jobId, resp, null);
        } catch (JobStartException jobStartException) {
            StringBuilder msg = new StringBuilder("Error while starting job ");
            msg.append(batchName).append('\n');
            appendExceptionMsg(msg, jobStartException);
            reportFailure(NO_JOB_ID, resp, msg.toString());
        }
    }

    private void batchStatus(String batchId, HttpServletRequest req, HttpServletResponse resp)
    {
        if (batchId == null || batchId.isEmpty()) {
            reportFailure(NO_JOB_ID, resp, "no executionId given");
            return;
        }

        long executionId = NO_JOB_ID;

        try {
            executionId = Long.valueOf(batchId);
        } catch(NumberFormatException nfe) {
            reportFailure(NO_JOB_ID, resp, "executionId must be numeric, but is " + batchId);
        }

        try {
            JobExecution jobExecution = jobOperator.getJobExecution(executionId);
            BatchStatus batchStatus = jobExecution.getBatchStatus();
            reportSuccess(executionId, resp, batchStatus.name());
        } catch (NoSuchJobExecutionException noSuchJob) {
            reportFailure(executionId, resp, "NoSuchJob");
        } catch (Exception generalException) {
            StringBuilder msg = new StringBuilder("NoSuchJob");
            appendExceptionMsg(msg, generalException);
            reportFailure(executionId, resp, msg.toString());
        }
    }

    private void unknownCommand(String path, HttpServletResponse resp) {
        StringBuilder msg = new StringBuilder("Unknown command:");
        msg.append(path).append('\n');

        msg.append("\nKnown commands are:\n\n");
        msg.append("* ").append(OP_START).append(" - start a new batch job\n");
        msg.append("  Sample: http://localhost:8080/myapp/jbatch/rest/start/myjobname?param1=x&param2=y\n");
        msg.append("  BatchEE will start the job and immediately return\n\n");

        msg.append("* ").append(OP_STATUS).append(" - query the current status \n");
        msg.append("  Sample: http://localhost:8080/myapp/jbatch/rest/status/23\n");
        msg.append("  will return the state of executionId 23\n\n");

        msg.append("The returned response if of MIME type text/plain and contains the following information\n");
        msg.append("  {jobExecutionId} (or -1 if no executionId was detected)\\n\n");
        msg.append("  OK (or FAILURE)\\n\n");
        msg.append("  followed by command specific information\n");

        reportFailure(NO_JOB_ID, resp, msg.toString());
    }

    private void reportSuccess(long jobId, HttpServletResponse resp, String msg) {
        resp.setStatus(HttpServletResponse.SC_OK);
        writeContent(resp, Long.toString(jobId) + "\n");
        writeContent(resp, "OK\n");
        if (msg != null) {
            writeContent(resp, msg);
        }
    }

    private void reportFailure(long jobId, HttpServletResponse resp, String content) {
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        writeContent(resp, Long.toString(jobId) + "\n");
        writeContent(resp, "FAILURE\n");
        writeContent(resp, content);
    }

    private void writeContent(HttpServletResponse resp, String content) {
        try {
            resp.getWriter().append(content);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private void appendExceptionMsg(StringBuilder msg, Exception exception) {
        msg.append(exception.getMessage()).append('\n');
        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw));
        msg.append(sw.toString());
    }

}
