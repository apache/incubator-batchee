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
package org.apache.batchee.container.services.persistence.jdbc;

import org.apache.batchee.container.impl.controller.PartitionedStepBuilder;
import org.apache.batchee.container.services.persistence.jdbc.database.Database;

public class Dictionary {
    public interface SQL { // needs to be kept aligned with JPA mapping, we can't use reflection to find fields since order can change between executions with java 7
        String CREATE_TABLE = "create table ";
        String INSERT_INTO = "insert into ";
        String SELECT = "select ";
        String FROM = " from ";
        String UPDATE = "update ";
        String WHERE = " where ";
        String DELETE = "delete from ";

        String[] CHECKPOINT_COLUMNS = { "id", "data", "stepName", "type", "INSTANCE_JOBINSTANCEID" };
        String CREATE_CHECKPOINT = CREATE_TABLE + "%s(%s %s %s , %s %s, %s %s, %s %s, %s %s, primary key(id))";
        String INSERT_CHECKPOINT = INSERT_INTO + "%s(%s, %s, %s, %s) values (?, ?, ?, ?)";
        String SELECT_CHECKPOINT = SELECT + "%s" + FROM + "%s" + WHERE + "%s = ? and %s = ? and %s = ?";
        String UPDATE_CHECKPOINT = UPDATE + "%s set %s = ?" + WHERE + "%s = ? and %s = ? and %s = ?";
        String DELETE_CHECKPOINT = DELETE + "%s" + WHERE + "%s = ?";
        String DELETE_CHECKPOINT_UNTIL = DELETE + "%s WHERE %s IN (" + SELECT + " DISTINCT t1.%s FROM %s t1 WHERE (SELECT MAX(t0.%s) FROM %s t0 WHERE t0.%s = t1.%s) < ?)";

        String[] JOB_INSTANCE_COLUMNS = { "jobInstanceId", "batchStatus", "exitStatus", "jobName", "jobXml", "latestExecution", "restartOn", "step" };
        String CREATE_JOB_INSTANCE = CREATE_TABLE + "%s(%s %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, PRIMARY KEY (%s))";
        String JOB_INSTANCE_COUNT_FROM_NAME = SELECT + "count(%s) as jobinstancecount" + FROM + "%s" + WHERE + "%s = ?";
        String JOB_INSTANCE_BY_ID = SELECT + "*" + FROM + "%s" + WHERE + "%s = ?";
        String JOB_INSTANCE_UPDATE_STATUS = UPDATE + "%s set %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ?" + WHERE + "%s = ?";
        String JOB_INSTANCE_IDS = SELECT + "%s" + FROM + "%s" + WHERE + "%s = ? order by %s desc";
        String JOB_INSTANCE_IDS_FROM_NAME = SELECT + "%s" + FROM + "%s" + WHERE + " %s = ? order by %s desc";
        String JOB_NAMES = SELECT + "distinct %s" + FROM + "%s" + WHERE + "%s not like '%s'";
        String EXTERNAL_JOB_INSTANCE = SELECT + "distinct %s, %s" + FROM + "%s" + WHERE + "%s not like '%s'";
        String JOB_INSTANCE_CREATE = INSERT_INTO + "%s" + "(%s) VALUES(?)";
        String JOB_INSTANCE_CREATE_WITH_JOB_XML = INSERT_INTO + "%s" + "(%s, %s) VALUES(?, ?)";
        String DELETE_JOB_INSTANCE = DELETE + "%s" + WHERE + "%s = ?";
        String DELETE_JOB_INSTANCE_UNTIL = DELETE + "%s" + WHERE + "%s IN (" + SELECT + "distinct t1.%s" + FROM + "%s t1" + WHERE + "(" + SELECT + "max(t0.%s)" + FROM + "%s t0" +
            WHERE + "t0.%s = t1.%s) < ?)";

        String[] JOB_EXECUTION_COLUMNS = { "executionId", "batchStatus", "createTime", "endTime", "exitStatus", "jobProperties",
                "startTime", "updateTime", "INSTANCE_JOBINSTANCEID" };
        String CREATE_JOB_EXECUTION = CREATE_TABLE + "%s(%s %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s)";
        String JOB_EXECUTION_TIMESTAMPS = SELECT + " %s, %s, %s, %s" + FROM + "%s" + WHERE + "%s = ?";
        String JOB_EXECUTION_BATCH_STATUS = SELECT + "%s" + FROM + "%s" + WHERE + "%s = ?";
        String JOB_EXECUTION_EXIT_STATUS = SELECT + "%s" + FROM + "%s" + WHERE + "%s = ?";
        String JOB_EXECUTION_PROPERTIES = SELECT + "%s" + FROM + "%s" + WHERE + "%s = ?";
        String JOB_EXECUTION_UPDATE = UPDATE + "%s set %s = ?, %s = ? where %s = ?";
        String JOB_EXECUTION_SET_FINAL_DATA = UPDATE + "%s set %s = ?, %s = ?, %s = ?, %s = ? where %s = ?";
        String JOB_EXECUTION_START = UPDATE + "%s set %s = ?, %s = ?, %s = ? where %s = ?";

        String JOB_EXECUTION_FIND_BY_ID = SELECT + "A.%s, A.%s, A.%s, A.%s, A.%s, A.%s, A.%s, A.%s, B.%s" +
                FROM + "%s as A inner join %s as B on A.%s = B.%s" + WHERE + "%s = ?";

        String JOB_EXECUTION_FROM_INSTANCE = SELECT + "A.%s, A.%s, A.%s, A.%s, A.%s, A.%s, A.%s, A.%s, B.%s " +
                FROM + "%s as A inner join %s as B ON A.%s = B.%s" + WHERE + "B.%s = ?";
        String JOB_EXECUTION_RUNNING = SELECT + "A.%s" + FROM + "%s AS A inner join %s AS B ON A.%s = B.%s WHERE A.%s IN (?,?,?) AND B.%s = ?";
        String JOB_INSTANCE_STATUS = SELECT + "*" + FROM + "%s as A inner join %s as B on A.%s = B.%s " + WHERE + "B.%s = ?";
        String JOB_INSTANCE_FROM_EXECUTION = SELECT + "%s" + FROM + "%s" +  WHERE + "%s = ?";
        String JOB_EXECUTION_CREATE = INSERT_INTO + "%s(%s, %s, %s, %s, %s) VALUES(?, ?, ?, ?, ?)";
        String JOB_EXECUTION_MOST_RECENT = SELECT + "%s" + FROM + "%s" + WHERE + "%s = ? ORDER BY %s DESC";
        String DELETE_JOB_EXECUTION = DELETE + "%s" + WHERE + "%s = ?";
        String DELETE_JOB_EXECUTION_UNTIL = DELETE + "%s" + WHERE + "%s IN (" + SELECT + "distinct t0.%s" + FROM + "%s t0" + WHERE + "t0.%s < ?)";

        String[] STEP_EXECUTION_COLUMNS = { "id", "batchStatus", "exec_commit", "endTime", "exitStatus", "exec_filter", "lastRunStepExecutionId", "numPartitions",
                "persistentData", "exec_processskip", "exec_read", "exec_readskip", "exec_rollback", "startCount", "startTime", "stepName",
                "exec_write", "exec_writeskip", "EXECUTION_EXECUTIONID" };

        String CREATE_STEP_EXECUTION = CREATE_TABLE +
                "%s(%s %s %s" + ", %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, PRIMARY KEY (id))";
        String STEPS_FROM_EXECUTION = SELECT + "*" + FROM + "%s" + WHERE + "%s = ?";
        String STEP_FROM_ID = SELECT + "*" + FROM + "%s" + WHERE + "%s = ?";
        String STEP_EXECUTION_CREATE = INSERT_INTO + "%s" + "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String STEP_EXECUTION_UPDATE_STATUS = UPDATE + "%s SET %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ? WHERE %s = ?";

        String STEP_EXECUTION_UPDATE = UPDATE +
                "%s SET %s = ?, %s = ?, %s = ?, %s = ?,  %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = ? WHERE %s = ?";

        String STEP_EXECUTION_BY_INSTANCE_AND_STEP = SELECT +
                "B.%s, B.%s, B.%s, B.%s, B.%s, B.%s, B.%s, B.%s" + FROM + "%s A inner join %s B ON A.%s = B.%s " + WHERE + "A.%s = ? and B.%s = ?";

        String DELETE_STEP_EXECUTION = DELETE + "%s A inner join %s B ON A.%s = B.%s " + WHERE + "A.%s = ?";
        String DELETE_STEP_EXECUTION_UNTIL = DELETE + "%s" + WHERE + "%s in (" + SELECT + "distinct t0.%s" + FROM + "%s t0 inner join %s t1 ON t0.%s=t1.%s" + WHERE + "t1.%s < ?)";
    }

    private final String checkpointTable;
    private final String createCheckpointTable;
    private final String insertCheckpoint;
    private final String selectCheckpoint;
    private final String updateCheckpoint;
    private final String deleteCheckpoint;
    private final String deleteCheckpointUntil;
    private final String jobInstanceTable;
    private final String createJobInstanceTable;
    private final String countJobInstanceByName;
    private final String findJobInstance;
    private final String updateJobInstanceStatus;
    private final String findJoBInstanceIds;
    private final String findJobInstanceIdsByName;
    private final String findJobNames;
    private final String findExternalJobInstances;
    private final String createJobInstance;
    private final String createJobInstanceWithJobXml;
    private final String deleteJobInstance;
    private final String deleteJobInstanceUntil;
    private final String jobExecutionTable;
    private final String createJobExecutionTable;
    private final String findJobExecutionTimestamps;
    private final String findJobExecutionBatchStatus;
    private final String findJobExecutionExitStatus;
    private final String findJobExecutionJobProperties;
    private final String updateJobExecution;
    private final String setJobExecutionFinalData;
    private final String updateStartedJobExecution;
    private final String findJobExecutionById;
    private final String findJobExecutionByInstance;
    private final String findRunningJobExecutions;
    private final String findJobStatus;
    private final String findJobInstanceFromJobExecution;
    private final String createJobExecution;
    private final String findMostRecentJobExecution;
    private final String deleteJobExecution;
    private final String deleteJobExecutionUntil;
    private final String stepExecutionTable;
    private final String createStepExecutionTable;
    private final String finStepExecutionFromJobExecution;
    private final String findStepExecutionFromId;
    private final String createStepExecution;
    private final String updateStepExecutionStatus;
    private final String updateStepExecution;
    private final String findStepExecutionByJobInstanceAndStepName;
    private final String deleteStepExecution;
    private final String deleteStepExecutionUntil;

    private final String[] checkpointColumns;
    private final String[] jobInstanceColumns;
    private final String[] jobExecutionColumns;
    private final String[] stepExecutionColumns;

    public Dictionary(final String checkpointTable, final String jobInstanceTable, final String jobExecutionTable,
                      final String stepExecutionTable, final Database database) {
        this.checkpointTable = checkpointTable;
        this.jobInstanceTable = jobInstanceTable;
        this.jobExecutionTable = jobExecutionTable;
        this.stepExecutionTable = stepExecutionTable;

        { // ensure to be able to build jointures prebuilding columns
            checkpointColumns = columns(database, SQL.CHECKPOINT_COLUMNS);
            jobExecutionColumns = columns(database, SQL.JOB_EXECUTION_COLUMNS);
            jobInstanceColumns = columns(database, SQL.JOB_INSTANCE_COLUMNS);
            stepExecutionColumns = columns(database, SQL.STEP_EXECUTION_COLUMNS);
        }

        { // checkpoint
            this.createCheckpointTable = String.format(SQL.CREATE_CHECKPOINT, checkpointTable,
                                                        checkpointColumns[0], database.bigint(), database.autoIncrementId(),
                                                        checkpointColumns[1], database.blob(),
                                                        checkpointColumns[2], database.varchar255(),
                                                        checkpointColumns[3], database.varchar20(),
                                                        checkpointColumns[4], database.bigint());
            this.insertCheckpoint = String.format(SQL.INSERT_CHECKPOINT, checkpointTable, checkpointColumns[1], checkpointColumns[2], checkpointColumns[3], checkpointColumns[4]);
            this.selectCheckpoint = String.format(SQL.SELECT_CHECKPOINT, checkpointColumns[1], checkpointTable, checkpointColumns[4], checkpointColumns[3], checkpointColumns[2]);
            this.updateCheckpoint = String.format(SQL.UPDATE_CHECKPOINT, checkpointTable, checkpointColumns[1], checkpointColumns[4], checkpointColumns[3], checkpointColumns[2]);
            this.deleteCheckpoint = String.format(SQL.DELETE_CHECKPOINT, checkpointTable, checkpointColumns[4]);
            // String DELETE_CHECKPOINT_UNTIL = DELETE + "%s WHERE %s IN (" + SELECT + " DISTINCT t1.%s FROM %s t1 WHERE (SELECT MAX(t0.%s) FROM %s t0 WHERE t0.%s = t1.%s) < ?)";
            this.deleteCheckpointUntil = String.format(SQL.DELETE_CHECKPOINT_UNTIL, checkpointTable, checkpointColumns[0], checkpointColumns[0], checkpointTable,
                jobExecutionColumns[3], jobExecutionTable, checkpointColumns[4], jobExecutionColumns[8]);
        }

        { // jobInstance
            this.createJobInstanceTable = String.format(SQL.CREATE_JOB_INSTANCE, jobInstanceTable,
                                                        jobInstanceColumns[0], database.bigint(), database.autoIncrementId(),
                                                        jobInstanceColumns[1], database.varchar20(),
                                                        jobInstanceColumns[2], database.varchar255(),
                                                        jobInstanceColumns[3], database.varchar255(),
                                                        jobInstanceColumns[4], database.blob(),
                                                        jobInstanceColumns[5], database.bigint(),
                                                        jobInstanceColumns[6], database.varchar255(),
                                                        jobInstanceColumns[7], database.varchar255(),
                                                        jobInstanceColumns[0]);
            this.countJobInstanceByName = String.format(SQL.JOB_INSTANCE_COUNT_FROM_NAME, jobInstanceColumns[0], jobInstanceTable, jobInstanceColumns[3]);
            this.findJobInstance = String.format(SQL.JOB_INSTANCE_BY_ID, jobInstanceTable, jobInstanceColumns[0]);
            this.updateJobInstanceStatus = String.format(SQL.JOB_INSTANCE_UPDATE_STATUS, jobInstanceTable, jobInstanceColumns[1], jobInstanceColumns[2], jobInstanceColumns[5],
                    jobInstanceColumns[6], jobInstanceColumns[7], jobInstanceColumns[3], jobInstanceColumns[0]);
            this.findJoBInstanceIds = String.format(SQL.JOB_INSTANCE_IDS, jobInstanceColumns[0], jobInstanceTable, jobInstanceColumns[3], jobInstanceColumns[0]);
            this.findJobInstanceIdsByName = String.format(SQL.JOB_INSTANCE_IDS_FROM_NAME, jobInstanceColumns[0], jobInstanceTable, jobInstanceColumns[3], jobInstanceColumns[0]);
            this.findJobNames = String.format(SQL.JOB_NAMES, jobInstanceColumns[3], jobInstanceTable, jobInstanceColumns[3], PartitionedStepBuilder.JOB_ID_SEPARATOR + "%");
            this.findExternalJobInstances = String.format(SQL.EXTERNAL_JOB_INSTANCE, jobInstanceColumns[0], jobInstanceColumns[3], jobInstanceTable,
                    jobInstanceColumns[3], PartitionedStepBuilder.JOB_ID_SEPARATOR + "%");
            this.createJobInstance = String.format(SQL.JOB_INSTANCE_CREATE, jobInstanceTable, jobInstanceColumns[3]);
            this.createJobInstanceWithJobXml = String.format(SQL.JOB_INSTANCE_CREATE_WITH_JOB_XML, jobInstanceTable, jobInstanceColumns[3], jobInstanceColumns[4]);
            this.deleteJobInstance = String.format(SQL.DELETE_JOB_INSTANCE, jobInstanceTable, jobInstanceColumns[0]);
            this.deleteJobInstanceUntil = String.format(SQL.DELETE_JOB_INSTANCE_UNTIL, jobInstanceTable, jobInstanceColumns[0], jobInstanceColumns[0], jobInstanceTable,
                jobExecutionColumns[3], jobExecutionTable, jobExecutionColumns[8], jobInstanceColumns[0]);
        }

        { // jobExecution
            this.createJobExecutionTable = String.format(SQL.CREATE_JOB_EXECUTION, jobExecutionTable,
                                                            jobExecutionColumns[0], database.bigint(), database.autoIncrementId(),
                                                            jobExecutionColumns[1], database.varchar20(),
                                                            jobExecutionColumns[2], database.timestamp(),
                                                            jobExecutionColumns[3], database.timestamp(),
                                                            jobExecutionColumns[4], database.varchar255(),
                                                            jobExecutionColumns[5], database.blob(),
                                                            jobExecutionColumns[6], database.timestamp(),
                                                            jobExecutionColumns[7], database.timestamp(),
                                                            jobExecutionColumns[8], database.bigint());
            this.findJobExecutionTimestamps = String.format(SQL.JOB_EXECUTION_TIMESTAMPS, jobExecutionColumns[2], jobExecutionColumns[3], jobExecutionColumns[7],
                    jobExecutionColumns[6], jobExecutionTable, jobExecutionColumns[0]);
            this.findJobExecutionBatchStatus = String.format(SQL.JOB_EXECUTION_BATCH_STATUS, jobExecutionColumns[1], jobExecutionTable, jobExecutionColumns[0]);
            this.findJobExecutionExitStatus = String.format(SQL.JOB_EXECUTION_EXIT_STATUS, jobExecutionColumns[4], jobExecutionTable, jobExecutionColumns[0]);
            this.findJobExecutionJobProperties = String.format(SQL.JOB_EXECUTION_PROPERTIES, jobExecutionColumns[5], jobExecutionTable, jobExecutionColumns[0]);
            this.updateJobExecution = String.format(SQL.JOB_EXECUTION_UPDATE, jobExecutionTable, jobExecutionColumns[1], jobExecutionColumns[7], jobExecutionColumns[0]);
            this.setJobExecutionFinalData = String.format(SQL.JOB_EXECUTION_SET_FINAL_DATA, jobExecutionTable, jobExecutionColumns[1], jobExecutionColumns[4],
                    jobExecutionColumns[3], jobExecutionColumns[7], jobExecutionColumns[0]);
            this.updateStartedJobExecution = String.format(SQL.JOB_EXECUTION_START, jobExecutionTable, jobExecutionColumns[1], jobExecutionColumns[6],
                    jobExecutionColumns[7], jobExecutionColumns[0]);
            this.findJobExecutionById = String.format(SQL.JOB_EXECUTION_FIND_BY_ID, jobExecutionColumns[2], jobExecutionColumns[6], jobExecutionColumns[3],
                    jobExecutionColumns[7], jobExecutionColumns[5], jobExecutionColumns[8], jobExecutionColumns[1], jobExecutionColumns[4], jobInstanceColumns[3],
                    jobExecutionTable, jobInstanceTable, jobExecutionColumns[8], jobInstanceColumns[0], jobExecutionColumns[0]);
            this.findRunningJobExecutions = String.format(SQL.JOB_EXECUTION_RUNNING, jobExecutionColumns[0], jobExecutionTable, jobInstanceTable, jobExecutionColumns[8],
                    jobInstanceColumns[0], jobExecutionColumns[1], jobInstanceColumns[3]);
            this.findJobExecutionByInstance = String.format(SQL.JOB_EXECUTION_FROM_INSTANCE, jobExecutionColumns[0], jobExecutionColumns[2], jobExecutionColumns[6],
                jobExecutionColumns[3], jobExecutionColumns[7], jobExecutionColumns[5], jobExecutionColumns[1], jobExecutionColumns[4], jobInstanceColumns[3],
                jobExecutionTable, jobInstanceTable, jobExecutionColumns[8], jobInstanceColumns[0], jobInstanceColumns[0]);
            this.findJobStatus = String.format(SQL.JOB_INSTANCE_STATUS, jobInstanceTable, jobExecutionTable, jobInstanceColumns[0], jobExecutionColumns[8],
                    jobExecutionColumns[0]);
            this.findJobInstanceFromJobExecution = String.format(SQL.JOB_INSTANCE_FROM_EXECUTION, jobExecutionColumns[8], jobExecutionTable, jobExecutionColumns[0]);
            this.createJobExecution = String.format(SQL.JOB_EXECUTION_CREATE, jobExecutionTable, jobExecutionColumns[8], jobExecutionColumns[2], jobExecutionColumns[7],
                    jobExecutionColumns[1], jobExecutionColumns[5]);
            this.findMostRecentJobExecution = String.format(SQL.JOB_EXECUTION_MOST_RECENT, jobExecutionColumns[0], jobExecutionTable,
                    jobExecutionColumns[8], jobExecutionColumns[2]);
            this.deleteJobExecution = String.format(SQL.DELETE_JOB_EXECUTION, jobExecutionTable, jobExecutionColumns[8]);
            this.deleteJobExecutionUntil = String.format(SQL.DELETE_JOB_EXECUTION_UNTIL, jobExecutionTable, jobExecutionColumns[0], jobExecutionColumns[0],
                jobExecutionTable, jobExecutionColumns[3]);
        }

        { // step execution
            this.createStepExecutionTable = String.format(SQL.CREATE_STEP_EXECUTION, stepExecutionTable,
                                                            stepExecutionColumns[0], database.bigint(), database.autoIncrementId(),
                                                            stepExecutionColumns[1], database.varchar20(),
                                                            stepExecutionColumns[2], database.bigint(),
                                                            stepExecutionColumns[3], database.timestamp(),
                                                            stepExecutionColumns[4], database.varchar255(),
                                                            stepExecutionColumns[5], database.bigint(),
                                                            stepExecutionColumns[6], database.bigint(),
                                                            stepExecutionColumns[7], database.integer(),
                                                            stepExecutionColumns[8], database.blob(),
                                                            stepExecutionColumns[9], database.bigint(),
                                                            stepExecutionColumns[10], database.bigint(),
                                                            stepExecutionColumns[11], database.bigint(),
                                                            stepExecutionColumns[12], database.bigint(),
                                                            stepExecutionColumns[13], database.integer(),
                                                            stepExecutionColumns[14], database.timestamp(),
                                                            stepExecutionColumns[15], database.varchar255(),
                                                            stepExecutionColumns[16], database.bigint(),
                                                            stepExecutionColumns[17], database.bigint(),
                                                            stepExecutionColumns[18], database.bigint());
            this.finStepExecutionFromJobExecution = String.format(SQL.STEPS_FROM_EXECUTION, stepExecutionTable, stepExecutionColumns[18]);
            this.findStepExecutionFromId = String.format(SQL.STEP_FROM_ID, stepExecutionTable, stepExecutionColumns[0]);
            this.createStepExecution = String.format(SQL.STEP_EXECUTION_CREATE, stepExecutionTable, stepExecutionColumns[18], stepExecutionColumns[1],
                    stepExecutionColumns[4], stepExecutionColumns[15],
                stepExecutionColumns[10], stepExecutionColumns[16], stepExecutionColumns[2], stepExecutionColumns[12], stepExecutionColumns[11],
                    stepExecutionColumns[9], stepExecutionColumns[5],
                stepExecutionColumns[17], stepExecutionColumns[14], stepExecutionColumns[3], stepExecutionColumns[8]);
            this.updateStepExecutionStatus = String.format(SQL.STEP_EXECUTION_UPDATE_STATUS, stepExecutionTable, stepExecutionColumns[8], stepExecutionColumns[1],
                    stepExecutionColumns[4], stepExecutionColumns[6],
                stepExecutionColumns[7], stepExecutionColumns[13], stepExecutionColumns[0]);
            this.updateStepExecution = String.format(SQL.STEP_EXECUTION_UPDATE, stepExecutionTable, stepExecutionColumns[18], stepExecutionColumns[1],
                    stepExecutionColumns[4], stepExecutionColumns[15],
                stepExecutionColumns[10], stepExecutionColumns[16], stepExecutionColumns[2], stepExecutionColumns[12], stepExecutionColumns[11],
                    stepExecutionColumns[9], stepExecutionColumns[5],
                stepExecutionColumns[17], stepExecutionColumns[14], stepExecutionColumns[3], stepExecutionColumns[8], stepExecutionColumns[0]);
            this.findStepExecutionByJobInstanceAndStepName = String.format(SQL.STEP_EXECUTION_BY_INSTANCE_AND_STEP, stepExecutionColumns[0],
                    stepExecutionColumns[13], stepExecutionColumns[1],
                stepExecutionColumns[4], stepExecutionColumns[8], stepExecutionColumns[6], stepExecutionColumns[7], stepExecutionColumns[18], jobExecutionTable, stepExecutionTable,
                jobExecutionColumns[0], stepExecutionColumns[18], jobExecutionColumns[8], stepExecutionColumns[15]);
            this.deleteStepExecution = String.format(SQL.DELETE_STEP_EXECUTION, jobExecutionTable, stepExecutionTable, jobExecutionColumns[0],
                    stepExecutionColumns[18], jobExecutionColumns[8]);
            this.deleteStepExecutionUntil = String.format(SQL.DELETE_STEP_EXECUTION_UNTIL, stepExecutionTable, stepExecutionColumns[0], stepExecutionColumns[0],
                stepExecutionTable, jobExecutionTable, stepExecutionColumns[18], jobExecutionColumns[0], jobExecutionColumns[3]);
        }
    }

    private static String[] columns(final Database database, final String[] cols) {
        final String[] out = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            out[i] = database.columnName(cols[i]);
        }
        return out;
    }

    public String getCheckpointTable() {
        return checkpointTable;
    }

    public String getCreateCheckpointTable() {
        return createCheckpointTable;
    }

    public String getInsertCheckpoint() {
        return insertCheckpoint;
    }

    public String getSelectCheckpoint() {
        return selectCheckpoint;
    }

    public String getUpdateCheckpoint() {
        return updateCheckpoint;
    }

    public String getDeleteCheckpoint() {
        return deleteCheckpoint;
    }

    public String getJobInstanceTable() {
        return jobInstanceTable;
    }

    public String getCreateJobInstanceTable() {
        return createJobInstanceTable;
    }

    public String getCountJobInstanceByName() {
        return countJobInstanceByName;
    }

    public String getFindJobInstance() {
        return findJobInstance;
    }

    public String getUpdateJobInstanceStatus() {
        return updateJobInstanceStatus;
    }

    public String getFindJoBInstanceIds() {
        return findJoBInstanceIds;
    }

    public String getFindJobInstanceIdsByName() {
        return findJobInstanceIdsByName;
    }

    public String getFindExternalJobInstances() {
        return findExternalJobInstances;
    }

    public String getFindJobNames() {
        return findJobNames;
    }

    public String getCreateJobInstance() {
        return createJobInstance;
    }

    public String getCreateJobInstanceWithJobXml() {
        return createJobInstanceWithJobXml;
    }

    public String getDeleteJobInstance() {
        return deleteJobInstance;
    }

    public String getJobExecutionTable() {
        return jobExecutionTable;
    }

    public String getCreateJobExecutionTable() {
        return createJobExecutionTable;
    }

    public String getFindJobExecutionTimestamps() {
        return findJobExecutionTimestamps;
    }

    public String getFindJobExecutionBatchStatus() {
        return findJobExecutionBatchStatus;
    }

    public String getFindJobExecutionExitStatus() {
        return findJobExecutionExitStatus;
    }

    public String getFindJobExecutionJobProperties() {
        return findJobExecutionJobProperties;
    }

    public String getUpdateJobExecution() {
        return updateJobExecution;
    }

    public String getSetJobExecutionFinalData() {
        return setJobExecutionFinalData;
    }

    public String getUpdateStartedJobExecution() {
        return updateStartedJobExecution;
    }

    public String getFindJobExecutionById() {
        return findJobExecutionById;
    }

    public String getFindJobExecutionByInstance() {
        return findJobExecutionByInstance;
    }

    public String getFindRunningJobExecutions() {
        return findRunningJobExecutions;
    }

    public String getFindJobStatus() {
        return findJobStatus;
    }

    public String getFindJobInstanceFromJobExecution() {
        return findJobInstanceFromJobExecution;
    }

    public String getCreateJobExecution() {
        return createJobExecution;
    }

    public String getFindMostRecentJobExecution() {
        return findMostRecentJobExecution;
    }

    public String getDeleteJobExecution() {
        return deleteJobExecution;
    }

    public String getStepExecutionTable() {
        return stepExecutionTable;
    }

    public String getCreateStepExecutionTable() {
        return createStepExecutionTable;
    }

    public String getFinStepExecutionFromJobExecution() {
        return finStepExecutionFromJobExecution;
    }

    public String getFindStepExecutionFromId() {
        return findStepExecutionFromId;
    }

    public String getCreateStepExecution() {
        return createStepExecution;
    }

    public String getUpdateStepExecutionStatus() {
        return updateStepExecutionStatus;
    }

    public String getUpdateStepExecution() {
        return updateStepExecution;
    }

    public String getFindStepExecutionByJobInstanceAndStepName() {
        return findStepExecutionByJobInstanceAndStepName;
    }

    public String getDeleteStepExecution() {
        return deleteStepExecution;
    }

    public String checkpointColumn(final int idx) {
        return checkpointColumns[idx];
    }

    public String jobInstanceColumns(final int idx) {
        return jobInstanceColumns[idx];
    }

    public String jobExecutionColumns(final int idx) {
        return jobExecutionColumns[idx];
    }

    public String stepExecutionColumns(final int idx) {
        return stepExecutionColumns[idx];
    }

    public String getDeleteCheckpointUntil() {
        return deleteCheckpointUntil;
    }

    public String getDeleteJobInstanceUntil() {
        return deleteJobInstanceUntil;
    }

    public String getDeleteJobExecutionUntil() {
        return deleteJobExecutionUntil;
    }

    public String getDeleteStepExecutionUntil() {
        return deleteStepExecutionUntil;
    }
}

