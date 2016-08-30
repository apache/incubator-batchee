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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;

import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionNotRunningException;
import javax.batch.operations.JobOperator;
import javax.batch.operations.JobStartException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Simple REST api for JBatch
 */
public class SimpleRestController {
    public static final String REST_CONTENT_TYPE = "text/plain";
    public static final String NOT_FOUND_STRING = "-";
    public static final char CSV_DELIMITER = ';';

    public static final String OP_START = "start/";
    public static final String OP_STATUS = "status/";
    public static final String OP_METRICS = "metrics/";

    public static final String OP_STOP = "stop/";
    public static final String OP_RESTART = "restart/";
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
        } else if (path != null && path.startsWith(OP_METRICS)) {
            batchMetrics(path.substring(OP_METRICS.length()), req, resp);
        } else if (path != null && path.startsWith(OP_STOP)) {
            batchStop(path.substring(OP_STOP.length()), req, resp);
        } else if (path != null && path.startsWith(OP_RESTART)) {
            batchRestart(path.substring(OP_RESTART.length()), req, resp);
        } else {
            unknownCommand(path, resp);
        }
    }

    private void startBatch(String batchName, HttpServletRequest req, HttpServletResponse resp) {
        Properties jobProperties = extractJobProperties(req, resp);
        if (jobProperties == null) { return; }

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
        Long executionId = extractExecutionId(batchId, resp);
        if (executionId == null) {
            return;
        }

        try {
            JobExecution jobExecution = jobOperator.getJobExecution(executionId);
            BatchStatus batchStatus = jobExecution.getBatchStatus();
            reportSuccess(executionId, resp, batchStatus.name());
        } catch (NoSuchJobExecutionException noSuchJob) {
            reportFailure(executionId, resp, "NoSuchJob");
        } catch (Exception generalException) {
            StringBuilder msg = new StringBuilder("Failure in BatchExecution");
            appendExceptionMsg(msg, generalException);
            reportFailure(executionId, resp, msg.toString());
        }
    }

    private void batchMetrics(String batchId, HttpServletRequest req, HttpServletResponse resp) {
        Long executionId = extractExecutionId(batchId, resp);
        if (executionId == null) {
            return;
        }

        try {
            JobExecution jobExecution = jobOperator.getJobExecution(executionId);
            BatchStatus batchStatus = jobExecution.getBatchStatus();

            List<StepExecution> stepExecutions = jobOperator.getStepExecutions(executionId);
            reportSuccess(executionId, resp, batchStatus.name());
            reportMetricsCsv(resp, stepExecutions);
        } catch (NoSuchJobExecutionException noSuchJob) {
            reportFailure(executionId, resp, "NoSuchJob");
        } catch (Exception generalException) {
            StringBuilder msg = new StringBuilder("Failure in BatchExecution");
            appendExceptionMsg(msg, generalException);
            reportFailure(executionId, resp, msg.toString());
        }
    }

    private void batchStop(String batchId, HttpServletRequest req, HttpServletResponse resp) {
        Long executionId = extractExecutionId(batchId, resp);
        if (executionId == null) {
            return;
        }

        try {
            jobOperator.stop(executionId);
            reportSuccess(executionId, resp, BatchStatus.STOPPING.toString());
        } catch (NoSuchJobExecutionException noSuchJob) {
            reportFailure(executionId, resp, "NoSuchJob");
        } catch (JobExecutionNotRunningException notRunningException) {
            reportFailure(executionId, resp, "JobExecutionNotRunning");
        } catch (Exception generalException) {
            StringBuilder msg = new StringBuilder("Failure in BatchExecution");
            appendExceptionMsg(msg, generalException);
            reportFailure(executionId, resp, msg.toString());
        }
    }

    private void batchRestart(String batchId, HttpServletRequest req, HttpServletResponse resp) {
        Long executionId = extractExecutionId(batchId, resp);
        if (executionId == null) {
            return;
        }

        Properties jobProperties = extractJobProperties(req, resp);

        try {
            jobOperator.restart(executionId, jobProperties);
        } catch (NoSuchJobExecutionException noSuchJob) {
            reportFailure(executionId, resp, "NoSuchJob");
        } catch (JobExecutionAlreadyCompleteException alreadyCompleted) {
            reportFailure(executionId, resp, "NoSuchJob");
        } catch (Exception generalException) {
            StringBuilder msg = new StringBuilder("Failure in BatchExecution");
            appendExceptionMsg(msg, generalException);
            reportFailure(executionId, resp, msg.toString());
        }
    }


    private void unknownCommand(String path, HttpServletResponse resp) {
        StringBuilder msg = new StringBuilder("Unknown command:");
        msg.append(path).append('\n');

        msg.append("The returned response if of MIME type text/plain and contains the following information\n");
        msg.append("  {jobExecutionId} (or -1 if no executionId was detected)\\n\n");
        msg.append("  OK (or FAILURE)\\n\n");
        msg.append("  followed by command specific information\n");

        msg.append("\nKnown commands are:\n\n");
        msg.append("* ").append(OP_START).append(" - start a new batch job\n");
        msg.append("  Sample: http://localhost:8080/myapp/jbatch/rest/start/myjobname?param1=x&param2=y\n");
        msg.append("  BatchEE will start the job and immediately return\n\n");

        msg.append("* ").append(OP_STATUS).append(" - query the current status \n");
        msg.append("  Sample: http://localhost:8080/myapp/jbatch/rest/status/23\n");
        msg.append("  will return the state of executionId 23\n\n");

        msg.append("* ").append(OP_METRICS).append(" - query the current metrics \n");
        msg.append("  Sample: http://localhost:8080/myapp/jbatch/rest/metrics/23\n");
        msg.append("  will return the metrics of executionId 23\n\n");

        msg.append("* ").append(OP_STOP).append(" - stop the job with the given executionId \n");
        msg.append("  Sample: http://localhost:8080/myapp/jbatch/rest/stop/23\n");
        msg.append("  will stop the job with executionId 23\n\n");

        msg.append("* ").append(OP_RESTART).append(" - restart the job with the given executionId \n");
        msg.append("  Sample: http://localhost:8080/myapp/jbatch/rest/restart/23\n");
        msg.append("  will restart the job with executionId 23\n\n");

        reportFailure(NO_JOB_ID, resp, msg.toString());
    }

    private Properties extractJobProperties(HttpServletRequest req, HttpServletResponse resp) {
        Properties jobProperties = new Properties();
        Map<String, String[]> parameterMap = req.getParameterMap();
        for (Map.Entry<String, String[]> paramEntry : parameterMap.entrySet()) {
            String key = paramEntry.getKey();
            if (key == null || key.length() == 0) {
                reportFailure(NO_JOB_ID, resp, "Parameter key must be set");
                return null;
            }

            String[] vals = paramEntry.getValue();
            if (vals == null || vals.length != 1) {
                reportFailure(NO_JOB_ID, resp, "Exactly one value must be set for each parameter (parameter name=" + key + ")");
                return null;
            }
            String val = vals[0];
            jobProperties.put(key, val);
        }
        return jobProperties;
    }

    private Long extractExecutionId(String batchId, HttpServletResponse resp) {
        if (batchId == null || batchId.isEmpty()) {
            reportFailure(NO_JOB_ID, resp, "no executionId given");
            return null;
        }

        try {
            return Long.valueOf(batchId);
        } catch(NumberFormatException nfe) {
            reportFailure(NO_JOB_ID, resp, "executionId must be numeric, but is " + batchId);
            return null;
        }
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

    private void reportMetricsCsv(HttpServletResponse resp, List<StepExecution> stepExecutions) {
        StringBuilder stringBuilder = new StringBuilder(200);
        stringBuilder.append("\n");

        // append csv header to stringbuilder
        joinCsv(stringBuilder, Arrays.asList("STEP_EXECUTION_ID", "STEP_NAME"));
        stringBuilder.append(CSV_DELIMITER);
        joinCsv(stringBuilder, Arrays.asList(Metric.MetricType.values()));
        stringBuilder.append("\n");

        Collections.sort(stepExecutions, new Comparator<StepExecution>() {
            @Override
            public int compare(StepExecution o1, StepExecution o2) {
                return Long.compare(o1.getStepExecutionId(), o2.getStepExecutionId());
            }
        });
        // append csv values to stringbuilder, one stepExecution per line
        for (StepExecution stepExecution : stepExecutions) {
            stringBuilder.append(stepExecution.getStepExecutionId());
            stringBuilder.append(CSV_DELIMITER);
            stringBuilder.append(stepExecution.getStepName());
            stringBuilder.append(CSV_DELIMITER);

            Metric[] metricsArray = stepExecution.getMetrics();
            Map<Metric.MetricType, Metric> sourceMap = new HashMap<Metric.MetricType, Metric>();
            for (Metric metric : metricsArray) {
                sourceMap.put(metric.getType(), metric);
            }

            List<String> orderedMetricsValues = new ArrayList<String>();
            for (Metric.MetricType type : Metric.MetricType.values()) {
                orderedMetricsValues.add(sourceMap.containsKey(type) ? String.valueOf(sourceMap.get(type).getValue()) : NOT_FOUND_STRING);
            }
            joinCsv(stringBuilder, orderedMetricsValues);

            stringBuilder.append("\n");
        }

        writeContent(resp, stringBuilder.toString());
    }

    private void joinCsv(StringBuilder builder, List values) {
        for (ListIterator iter = values.listIterator(); iter.hasNext(); ) {
            builder.append(iter.next());
            if (iter.hasNext()) {
                builder.append(CSV_DELIMITER);
            }
        }
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
