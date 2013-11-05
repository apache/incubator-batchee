/*
 * Copyright 2012 International Business Machines Corp.
 * 
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.container.services.persistence;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.exception.PersistenceException;
import org.apache.batchee.container.impl.JobExecutionImpl;
import org.apache.batchee.container.impl.JobInstanceImpl;
import org.apache.batchee.container.impl.MetricImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.StepExecutionImpl;
import org.apache.batchee.container.impl.controller.chunk.CheckpointData;
import org.apache.batchee.container.impl.controller.chunk.CheckpointDataKey;
import org.apache.batchee.container.impl.controller.chunk.PersistentDataWrapper;
import org.apache.batchee.container.impl.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.persistence.jdbc.Dictionary;
import org.apache.batchee.container.services.persistence.jdbc.database.Database;
import org.apache.batchee.container.services.persistence.jdbc.database.DerbyDatabase;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.container.status.StepStatus;
import org.apache.batchee.container.util.TCCLObjectInputStream;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.batchee.container.util.Serializations.deserialize;
import static org.apache.batchee.container.util.Serializations.serialize;

public class JDBCPersistenceManager implements PersistenceManagerService {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    static interface Defaults {
        final String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
        final String JDBC_URL = "jdbc:derby:memory:batchee;create=true";
        final String JDBC_USER = "app";
        final String JDBC_PASSWORD = "app";
        final String SCHEMA = "BATCHEE";
    }

    private Dictionary dictionary;

    protected DataSource dataSource = null;
    protected String jndiName = null;

    protected String driver = "";
    protected String schema = "";
    protected String url = "";
    protected String user = "";
    protected String pwd = "";

    @Override
    public void init(final Properties batchConfig) throws BatchContainerServiceException {
        final boolean hasSchema = "true".equalsIgnoreCase(batchConfig.getProperty("persistence.database.has-schema", "true"));
        if (hasSchema) {
            schema = batchConfig.getProperty("persistence.database.schema", Defaults.SCHEMA);
        } else {
            schema = null;
        }

        if (batchConfig.containsKey("persistence.database.jndi")) {
            jndiName = batchConfig.getProperty("persistence.database.jndi", "");
            if (jndiName.isEmpty()) {
                throw new BatchContainerServiceException("JNDI name is not defined.");
            }

            try {
                final Context ctx = new InitialContext();
                dataSource = DataSource.class.cast(ctx.lookup(jndiName));
            } catch (final NamingException e) {
                throw new BatchContainerServiceException(e);
            }

        } else {
            driver = batchConfig.getProperty("persistence.database.driver", Defaults.JDBC_DRIVER);
            url = batchConfig.getProperty("persistence.database.url", Defaults.JDBC_URL);
            user = batchConfig.getProperty("persistence.database.user", Defaults.JDBC_USER);
            pwd = batchConfig.getProperty("persistence.database.password", Defaults.JDBC_PASSWORD);
        }

        try {
            initDictionary(batchConfig);

            if ("create".equalsIgnoreCase(batchConfig.getProperty("persistence.database.ddl", "create"))) {
                if (hasSchema && !isSchemaValid()) {
                    createSchema();
                }
                checkAllTables();
            }
        } catch (final SQLException e) {
            throw new BatchContainerServiceException(e);
        }
    }

    private void initDictionary(final Properties batchConfig) throws BatchContainerServiceException, SQLException {
        final String type = batchConfig.getProperty("persistence.database.db-dictionary", guessDictionary());
        if (type == null) {
            throw new IllegalArgumentException("You need to provide a db dictionary for this database");
        }

        try {
            final Database database = Database.class.cast(Thread.currentThread().getContextClassLoader().loadClass(type).newInstance());
            dictionary = new Dictionary(
                batchConfig.getProperty("persistence.database.tables.checkpoint", "checkpointentity"),
                batchConfig.getProperty("persistence.database.tables.job-instance", "jobinstanceentity"),
                batchConfig.getProperty("persistence.database.tables.job-execution", "jobexecutionentity"),
                batchConfig.getProperty("persistence.database.tables.step-execution", "stepexecutionentity"),
                database);
        } catch (final Exception e) {
            throw new BatchContainerServiceException(e);
        }
    }

    /**
     * Checks if the default schema JBATCH or the schema defined in batch-config exists.
     *
     * @return true if the schema exists, false otherwise.
     * @throws SQLException
     */
    private boolean isSchemaValid() throws SQLException {
        final Connection conn = getConnectionToDefaultSchema();
        final DatabaseMetaData dbmd = conn.getMetaData();
        final ResultSet rs = dbmd.getSchemas();
        while (rs.next()) {
            if (schema.equalsIgnoreCase(rs.getString("TABLE_SCHEM"))) {
                cleanupConnection(conn, rs, null);
                return true;
            }
        }
        cleanupConnection(conn, rs, null);
        return false;
    }

    private String guessDictionary() throws SQLException {
        final Connection conn = getConnectionToDefaultSchema();
        final String pn;
        try {
            final DatabaseMetaData dbmd = conn.getMetaData();
            pn = dbmd.getDatabaseProductName().toLowerCase();
        } finally {
            conn.close();
        }

        if (pn.contains("derby")) {
            return DerbyDatabase.class.getName();
        }
        /* TODO
        if (pn.contains("mysql")) {
            return "mysql";
        }
        if (pn.contains("oracle")) {
            return "oracle";
        }
        if (pn.contains("hsql")) {
            return "hsql";
        }
        */
        return null;
    }

    private void createSchema() throws SQLException {
        final Connection conn = getConnectionToDefaultSchema();
        final PreparedStatement ps = conn.prepareStatement("CREATE SCHEMA " + schema);
        ps.execute();
        cleanupConnection(conn, null, ps);
    }

    /**
     * Checks if all the runtime batch table exists. If not, it creates them.
     *
     * @throws SQLException
     */
    private void checkAllTables() throws SQLException {
        createIfNotExists(dictionary.getCheckpointTable(), dictionary.getCreateCheckpointTable());
        createIfNotExists(dictionary.getJobExecutionTable(), dictionary.getCreateJobExecutionTable());
        createIfNotExists(dictionary.getJobInstanceTable(), dictionary.getCreateJobInstanceTable());
        createIfNotExists(dictionary.getStepExecutionTable(), dictionary.getCreateStepExecutionTable());
    }

    protected Connection getConnection() throws SQLException {
        final Connection connection;
        if (dataSource != null) {
            connection = dataSource.getConnection();
        } else {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                throw new PersistenceException(e);
            }
            connection = DriverManager.getConnection(url, user, pwd);
        }
        setSchemaOnConnection(connection);

        return connection;
    }

    private void createIfNotExists(final String tableName, final String createTableStatement) throws SQLException {
        final Connection conn = getConnection();
        final DatabaseMetaData dbmd = conn.getMetaData();
        final ResultSet rs = dbmd.getTables(null, schema, tableName, null);

        PreparedStatement ps = null;
        if (!rs.next()) {
            ps = conn.prepareStatement(createTableStatement);
            ps.executeUpdate();
        }

        cleanupConnection(conn, rs, ps);
    }

    public void createCheckpointData(final CheckpointDataKey key, final CheckpointData value) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getInsertCheckpoint());

            statement.setBytes(1, value.getRestartToken());
            statement.setString(2, key.getStepName());
            statement.setString(3, key.getType().name());
            statement.setLong(4, key.getJobInstanceId());
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public CheckpointData getCheckpointData(final CheckpointDataKey key) {
        return queryCheckpointData(key);
    }

    @Override
    public void setCheckpointData(final CheckpointDataKey key, final CheckpointData value) {
        if (queryCheckpointData(key) != null) {
            updateCheckpointData(key, value);
        } else {
            createCheckpointData(key, value);
        }
    }

    /**
     * @return the database connection. The schema is set to whatever default its used by the underlying database.
     * @throws SQLException
     */
    protected Connection getConnectionToDefaultSchema() throws SQLException {
        final Connection connection;
        if (dataSource != null) {
            try {
                connection = dataSource.getConnection();
            } catch (final SQLException e) {
                throw new PersistenceException(e);
            }
        } else {
            try {
                Class.forName(driver);
            } catch (final ClassNotFoundException e) {
                final StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                throw new PersistenceException(e);
            }
            try {
                connection = DriverManager.getConnection(url, user, pwd);
            } catch (final SQLException e) {
                throw new PersistenceException(e);
            }
        }
        return connection;
    }

    private void setSchemaOnConnection(final Connection connection) throws SQLException {
        if (schema == null) {
            return;
        }

        final PreparedStatement ps = connection.prepareStatement("set schema ?");
        ps.setString(1, schema);
        ps.executeUpdate();
        ps.close();
    }

    /**
     * select data from DB table
     *
     * @param key - the IPersistenceDataKey object
     * @return List of serializable objects store in the DB table
     * <p/>
     * Ex. select id, obj from tablename where id = ?
     */
    private CheckpointData queryCheckpointData(final CheckpointDataKey key) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getSelectCheckpoint());
            statement.setLong(1, key.getJobInstanceId());
            statement.setString(2, key.getType().name());
            statement.setString(3, key.getStepName());
            rs = statement.executeQuery();
            if (rs.next()) {
                final CheckpointData data = new CheckpointData(key.getJobInstanceId(), key.getStepName(), key.getType());
                data.setRestartToken(rs.getBytes(dictionary.checkpointColumn(1)));
                return data;
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    /**
     * update data in DB table
     *
     * @param value - serializable object to store
     * @param key   - the IPersistenceDataKey object
     *              <p/>
     *              Ex. update tablename set obj = ? where id = ?
     */
    private void updateCheckpointData(final CheckpointDataKey key, final CheckpointData value) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getUpdateCheckpoint());
            statement.setBytes(1, value.getRestartToken());
            statement.setLong(2, key.getJobInstanceId());
            statement.setString(3, key.getType().name());
            statement.setString(4, key.getStepName());
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }


    /**
     * closes connection, result set and statement
     *
     * @param conn      - connection object to close
     * @param rs        - result set object to close
     * @param statement - statement object to close
     */
    private void cleanupConnection(final Connection conn, final ResultSet rs, final PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (final SQLException e) {
                throw new PersistenceException(e);
            }
        }

        if (rs != null) {
            try {
                rs.close();
            } catch (final SQLException e) {
                throw new PersistenceException(e);
            }
        }

        if (conn != null) {
            Exception thrown = null;
            try {
                conn.close();
            } catch (final SQLException e) {
                throw new PersistenceException(e);
            } finally {
                try {
                    conn.close();
                } catch (final SQLException e) {
                    thrown = e;
                }
            }
            if (thrown != null) {
                throw new PersistenceException(thrown);
            }
        }
    }

    @Override
    public int jobOperatorGetJobInstanceCount(final String jobName, final String appTag) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getCountJobInstanceByNameAndTag());
            statement.setString(1, jobName);
            statement.setString(2, appTag);
            rs = statement.executeQuery();
            rs.next();
            return rs.getInt("jobinstancecount"); // not a column name so ok
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public int jobOperatorGetJobInstanceCount(final String jobName) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getCountJobInstanceByName());
            statement.setString(1, jobName);
            rs = statement.executeQuery();
            rs.next();
            return rs.getInt("jobinstancecount"); // not a column name so ok
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }


    @Override
    public List<Long> jobOperatorGetJobInstanceIds(final String jobName, final String appTag, final int start, final int count) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        final List<Long> data = new ArrayList<Long>();
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJoBInstanceIds());
            statement.setObject(1, jobName);
            statement.setObject(2, appTag);
            rs = statement.executeQuery();
            while (rs.next()) {
                data.add(rs.getLong(dictionary.jobInstanceColumns(0)));
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        if (!data.isEmpty()) {
            try {
                return data.subList(start, start + count);
            } catch (final IndexOutOfBoundsException oobEx) {
                return data.subList(start, data.size());
            }
        }
        return data;
    }

    @Override
    public List<Long> jobOperatorGetJobInstanceIds(final String jobName, final int start, final int count) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        final List<Long> data = new ArrayList<Long>();

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobInstanceIdsByName());
            statement.setObject(1, jobName);
            rs = statement.executeQuery();
            while (rs.next()) {
                data.add(rs.getLong(dictionary.jobInstanceColumns(0)));
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        if (data.size() > 0) {
            try {
                return data.subList(start, start + count);
            } catch (final IndexOutOfBoundsException oobEx) {
                return data.subList(start, data.size());
            }
        }
        return data;
    }

    @Override
    public Map<Long, String> jobOperatorGetExternalJobInstanceData() {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        final Map<Long, String> data = new HashMap<Long, String>();

        try {
            conn = getConnection();

            // Filter out 'subjob' parallel execution entries which start with the special character
            statement = conn.prepareStatement(dictionary.getFindExternalJobInstances());
            rs = statement.executeQuery();
            while (rs.next()) {
                data.put(rs.getLong(dictionary.jobInstanceColumns(0)), rs.getString(dictionary.jobInstanceColumns(3)));
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return data;
    }

    @Override
    public Timestamp jobOperatorQueryJobExecutionTimestamp(final long key, final TimestampType timestampType) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Timestamp createTimestamp = null;
        Timestamp endTimestamp = null;
        Timestamp updateTimestamp = null;
        Timestamp startTimestamp = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobExecutionTimestamps());
            statement.setLong(1, key);
            rs = statement.executeQuery();
            while (rs.next()) {
                createTimestamp = rs.getTimestamp(1);
                endTimestamp = rs.getTimestamp(2);
                updateTimestamp = rs.getTimestamp(3);
                startTimestamp = rs.getTimestamp(4);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        if (timestampType.equals(TimestampType.CREATE)) {
            return createTimestamp;
        } else if (timestampType.equals(TimestampType.END)) {
            return endTimestamp;
        } else if (timestampType.equals(TimestampType.LAST_UPDATED)) {
            return updateTimestamp;
        } else if (timestampType.equals(TimestampType.STARTED)) {
            return startTimestamp;
        }
        throw new IllegalArgumentException("Unexpected enum value.");
    }

    @Override
    public String jobOperatorQueryJobExecutionBatchStatus(final long key) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobExecutionBatchStatus());
            statement.setLong(1, key);
            rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }


    @Override
    public String jobOperatorQueryJobExecutionExitStatus(final long key) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobExecutionExitStatus());
            statement.setLong(1, key);
            rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public Properties getParameters(final long executionId) throws NoSuchJobExecutionException {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobExecutionJobProperties());
            statement.setLong(1, executionId);
            rs = statement.executeQuery();

            if (rs.next()) {
                final byte[] buf = rs.getBytes(dictionary.jobExecutionColumns(5));
                return Properties.class.cast(deserialize(buf));
            }
            throw new NoSuchJobExecutionException("Did not find table entry for executionID =" + executionId);
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public List<StepExecution> getStepExecutionsForJobExecution(final long execid) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        long jobexecid;
        long stepexecid;
        String stepname;
        String batchstatus;
        String exitstatus;
        long readCount;
        long writeCount;
        long commitCount;
        long rollbackCount;
        long readSkipCount;
        long processSkipCount;
        long filterCount;
        long writeSkipCount;
        Timestamp startTS;
        Timestamp endTS;
        StepExecutionImpl stepEx;
        ObjectInputStream objectIn;

        final List<StepExecution> data = new ArrayList<StepExecution>();

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFinStepExecutionFromJobExecution());
            statement.setLong(1, execid);
            rs = statement.executeQuery();

            while (rs.next()) {
                jobexecid = rs.getLong(dictionary.stepExecutionColumns(18));
                stepexecid = rs.getLong(dictionary.stepExecutionColumns(0));
                stepname = rs.getString(dictionary.stepExecutionColumns(15));
                batchstatus = rs.getString(dictionary.stepExecutionColumns(1));
                exitstatus = rs.getString(dictionary.stepExecutionColumns(4));
                readCount = rs.getLong(dictionary.stepExecutionColumns(10));
                writeCount = rs.getLong(dictionary.stepExecutionColumns(16));
                commitCount = rs.getLong(dictionary.stepExecutionColumns(2));
                rollbackCount = rs.getLong(dictionary.stepExecutionColumns(12));
                readSkipCount = rs.getLong(dictionary.stepExecutionColumns(11));
                processSkipCount = rs.getLong(dictionary.stepExecutionColumns(9));
                filterCount = rs.getLong(dictionary.stepExecutionColumns(5));
                writeSkipCount = rs.getLong(dictionary.stepExecutionColumns(17));
                startTS = rs.getTimestamp(dictionary.stepExecutionColumns(14));
                endTS = rs.getTimestamp(dictionary.stepExecutionColumns(3));

                // get the object based data
                Serializable persistentData = null;
                final byte[] pDataBytes = rs.getBytes(dictionary.stepExecutionColumns(8));
                if (pDataBytes != null) {
                    objectIn = new TCCLObjectInputStream(new ByteArrayInputStream(pDataBytes));
                    persistentData = (Serializable) objectIn.readObject();
                }

                stepEx = new StepExecutionImpl(jobexecid, stepexecid);

                stepEx.setBatchStatus(BatchStatus.valueOf(batchstatus));
                stepEx.setExitStatus(exitstatus);
                stepEx.setStepName(stepname);
                stepEx.setReadCount(readCount);
                stepEx.setWriteCount(writeCount);
                stepEx.setCommitCount(commitCount);
                stepEx.setRollbackCount(rollbackCount);
                stepEx.setReadSkipCount(readSkipCount);
                stepEx.setProcessSkipCount(processSkipCount);
                stepEx.setFilterCount(filterCount);
                stepEx.setWriteSkipCount(writeSkipCount);
                stepEx.setStartTime(startTS);
                stepEx.setEndTime(endTS);
                stepEx.setPersistentUserData(persistentData);

                data.add(stepEx);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return data;
    }


    @Override
    public StepExecution getStepExecutionByStepExecutionId(final long stepExecId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindStepExecutionFromId());
            statement.setLong(1, stepExecId);
            rs = statement.executeQuery();
            if (rs.next()) {
                final long jobexecid = rs.getLong(dictionary.stepExecutionColumns(18));
                final long stepExecutionId = rs.getLong(dictionary.stepExecutionColumns(0));
                final String stepName = rs.getString(dictionary.stepExecutionColumns(15));
                final String batchstatus = rs.getString(dictionary.stepExecutionColumns(1));
                final String exitstatus = rs.getString(dictionary.stepExecutionColumns(4));
                final long readCount = rs.getLong(dictionary.stepExecutionColumns(10));
                final long writeCount = rs.getLong(dictionary.stepExecutionColumns(16));
                final long commitCount = rs.getLong(dictionary.stepExecutionColumns(2));
                final long rollbackCount = rs.getLong(dictionary.stepExecutionColumns(12));
                final long readSkipCount = rs.getLong(dictionary.stepExecutionColumns(11));
                final long processSkipCount = rs.getLong(dictionary.stepExecutionColumns(9));
                final long filterCount = rs.getLong(dictionary.stepExecutionColumns(5));
                final long writeSkipCount = rs.getLong(dictionary.stepExecutionColumns(17));
                final Timestamp startTS = rs.getTimestamp(dictionary.stepExecutionColumns(14));
                final Timestamp endTS = rs.getTimestamp(dictionary.stepExecutionColumns(3));

                // get the object based data
                Serializable persistentData = null;
                final byte[] pDataBytes = rs.getBytes(dictionary.stepExecutionColumns(8));
                if (pDataBytes != null) {
                    persistentData = Serializable.class.cast(new TCCLObjectInputStream(new ByteArrayInputStream(pDataBytes)).readObject());
                }

                final StepExecutionImpl stepEx = new StepExecutionImpl(jobexecid, stepExecutionId);

                stepEx.setBatchStatus(BatchStatus.valueOf(batchstatus));
                stepEx.setExitStatus(exitstatus);
                stepEx.setStepName(stepName);
                stepEx.setReadCount(readCount);
                stepEx.setWriteCount(writeCount);
                stepEx.setCommitCount(commitCount);
                stepEx.setRollbackCount(rollbackCount);
                stepEx.setReadSkipCount(readSkipCount);
                stepEx.setProcessSkipCount(processSkipCount);
                stepEx.setFilterCount(filterCount);
                stepEx.setWriteSkipCount(writeSkipCount);
                stepEx.setStartTime(startTS);
                stepEx.setEndTime(endTS);
                stepEx.setPersistentUserData(persistentData);
                return stepEx;
            }
            return null;
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } catch (IOException e) {
            throw new PersistenceException(e);
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public void updateBatchStatusOnly(final long key, final BatchStatus batchStatus, final Timestamp updatets) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getUpdateJobExecution());
            statement.setString(1, batchStatus.name());
            statement.setTimestamp(2, updatets);
            statement.setLong(3, key);
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            e.printStackTrace();
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public void updateWithFinalExecutionStatusesAndTimestamps(final long key, final BatchStatus batchStatus,
                                                              final String exitStatus, final Timestamp updatets) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getSetJobExecutionFinalData());

            statement.setString(1, batchStatus.name());
            statement.setString(2, exitStatus);
            statement.setTimestamp(3, updatets);
            statement.setTimestamp(4, updatets);
            statement.setLong(5, key);

            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            e.printStackTrace();
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    public void markJobStarted(final long key, final Timestamp startTS) {
        Connection conn = null;
        PreparedStatement statement = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getUpdateStartedJobExecution());

            statement.setString(1, BatchStatus.STARTED.name());
            statement.setTimestamp(2, startTS);
            statement.setTimestamp(3, startTS);
            statement.setLong(4, key);

            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }


    @Override
    public InternalJobExecution jobOperatorGetJobExecution(long jobExecutionId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobExecutionById());
            statement.setLong(1, jobExecutionId);
            rs = statement.executeQuery();
            if (rs.next()) {
                final Timestamp createtime = rs.getTimestamp(dictionary.jobExecutionColumns(2));
                final Timestamp starttime = rs.getTimestamp(dictionary.jobExecutionColumns(6));
                final Timestamp endtime = rs.getTimestamp(dictionary.jobExecutionColumns(3));
                final Timestamp updatetime = rs.getTimestamp(dictionary.jobExecutionColumns(7));
                final long instanceId = rs.getLong(dictionary.jobExecutionColumns(8));

                // get the object based data
                final String batchStatus = rs.getString(dictionary.jobExecutionColumns(1));
                final String exitStatus = rs.getString(dictionary.jobExecutionColumns(4));

                // get the object based data
                final byte[] buf = rs.getBytes(dictionary.jobExecutionColumns(5));
                final Properties params = Properties.class.cast(deserialize(buf));

                final String jobName = rs.getString(dictionary.jobInstanceColumns(3));

                final JobExecutionImpl jobEx = new JobExecutionImpl(jobExecutionId, instanceId);
                jobEx.setCreateTime(createtime);
                jobEx.setStartTime(starttime);
                jobEx.setEndTime(endtime);
                jobEx.setJobParameters(params);
                jobEx.setLastUpdateTime(updatetime);
                jobEx.setBatchStatus(batchStatus);
                jobEx.setExitStatus(exitStatus);
                jobEx.setJobName(jobName);
                return jobEx;
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public List<InternalJobExecution> jobOperatorGetJobExecutions(final long jobInstanceId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        final List<InternalJobExecution> data = new ArrayList<InternalJobExecution>();
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobExecutionByInstance());
            statement.setLong(1, jobInstanceId);
            rs = statement.executeQuery();
            while (rs.next()) {
                final long jobExecutionId = rs.getLong(dictionary.jobExecutionColumns(0));
                final Timestamp createtime = rs.getTimestamp(dictionary.jobExecutionColumns(2));
                final Timestamp starttime = rs.getTimestamp(dictionary.jobExecutionColumns(6));
                final Timestamp endtime = rs.getTimestamp(dictionary.jobExecutionColumns(3));
                final Timestamp updatetime = rs.getTimestamp(dictionary.jobExecutionColumns(7));
                final String batchStatus = rs.getString(dictionary.jobExecutionColumns(1));
                final String exitStatus = rs.getString(dictionary.jobExecutionColumns(4));
                final String jobName = rs.getString(dictionary.jobInstanceColumns(3));

                final JobExecutionImpl jobEx = new JobExecutionImpl(jobExecutionId, jobInstanceId);
                jobEx.setCreateTime(createtime);
                jobEx.setStartTime(starttime);
                jobEx.setEndTime(endtime);
                jobEx.setLastUpdateTime(updatetime);
                jobEx.setBatchStatus(batchStatus);
                jobEx.setExitStatus(exitStatus);
                jobEx.setJobName(jobName);

                data.add(jobEx);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return data;
    }

    @Override
    public Set<Long> jobOperatorGetRunningExecutions(String jobName) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        final Set<Long> executionIds = new HashSet<Long>();
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindRunningJobExecutions());
            statement.setString(1, BatchStatus.STARTED.name());
            statement.setString(2, BatchStatus.STARTING.name());
            statement.setString(3, BatchStatus.STOPPING.name());
            statement.setString(4, jobName);
            rs = statement.executeQuery();
            while (rs.next()) {
                executionIds.add(rs.getLong(dictionary.jobExecutionColumns(0)));
            }

        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return executionIds;
    }

    @Override
    public JobStatus getJobStatusFromExecution(final long executionId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobStatus());
            statement.setLong(1, executionId);
            rs = statement.executeQuery();
            if (rs.next()) {
                final long jobInstanceId = rs.getLong(dictionary.jobExecutionColumns(8));
                final String batchStatus = rs.getString(dictionary.jobExecutionColumns(1));
                final byte[] jobXmls = rs.getBytes(dictionary.jobInstanceColumns(4));
                final JobInstanceImpl jobInstance;
                if (jobXmls != null) {
                    jobInstance = new JobInstanceImpl(jobInstanceId, new String(jobXmls, UTF_8));
                } else {
                    jobInstance = new JobInstanceImpl(jobInstanceId);
                }
                jobInstance.setJobName(rs.getString(dictionary.jobInstanceColumns(3)));

                final JobStatus status = new JobStatus(jobInstanceId);
                status.setExitStatus(rs.getString(dictionary.jobInstanceColumns(2)));
                status.setLatestExecutionId(rs.getLong(dictionary.jobInstanceColumns(5)));
                status.setRestartOn(rs.getString(dictionary.jobInstanceColumns(6)));
                status.setCurrentStepId(rs.getString(dictionary.jobInstanceColumns(7)));
                status.setJobInstance(jobInstance);
                if (batchStatus != null) {
                    status.setBatchStatus(BatchStatus.valueOf(batchStatus));
                }

                return status;
            }
            return null;
        } catch (final Exception e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    public long getJobInstanceIdByExecutionId(final long executionId) throws NoSuchJobExecutionException {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobInstanceFromJobExecution());
            statement.setObject(1, executionId);
            rs = statement.executeQuery();
            if (!rs.next()) {
                throw new NoSuchJobExecutionException("Did not find job instance associated with executionID =" + executionId);
            }
            return rs.getLong(dictionary.jobExecutionColumns(8));
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public JobInstance createSubJobInstance(final String name, final String apptag) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        JobInstanceImpl jobInstance = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getCreateJobInstance(), Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, name);
            statement.setString(2, apptag);
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                final long jobInstanceID = rs.getLong(1);
                jobInstance = new JobInstanceImpl(jobInstanceID);
                jobInstance.setJobName(name);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return jobInstance;
    }

    @Override
    public JobInstance createJobInstance(final String name, final String apptag, final String jobXml) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getCreateJobInstanceWithJobXml(), Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, name);
            statement.setString(2, apptag);
            statement.setBytes(3, jobXml.getBytes(UTF_8));
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                long jobInstanceID = rs.getLong(1);
                final JobInstanceImpl jobInstance = new JobInstanceImpl(jobInstanceID, jobXml);
                jobInstance.setJobName(name);
                return jobInstance;
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public RuntimeJobExecution createJobExecution(final JobInstance jobInstance, final Properties jobParameters, final BatchStatus batchStatus) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        final long newExecutionId = createRuntimeJobExecutionEntry(jobInstance, jobParameters, batchStatus, now);
        final RuntimeJobExecution jobExecution = new RuntimeJobExecution(jobInstance, newExecutionId);
        jobExecution.setBatchStatus(batchStatus.name());
        jobExecution.setCreateTime(now);
        jobExecution.setLastUpdateTime(now);
        return jobExecution;
    }

    private long createRuntimeJobExecutionEntry(final JobInstance jobInstance, final Properties jobParameters, final BatchStatus batchStatus, final Timestamp timestamp) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getCreateJobExecution(), Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, jobInstance.getInstanceId());
            statement.setTimestamp(2, timestamp);
            statement.setTimestamp(3, timestamp);
            statement.setString(4, batchStatus.name());
            statement.setObject(5, serialize(jobParameters));
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
            return -1;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public RuntimeFlowInSplitExecution createFlowInSplitExecution(final JobInstance jobInstance, final BatchStatus batchStatus) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        final long newExecutionId = createRuntimeJobExecutionEntry(jobInstance, null, batchStatus, now);
        final RuntimeFlowInSplitExecution flowExecution = new RuntimeFlowInSplitExecution(jobInstance, newExecutionId);
        flowExecution.setBatchStatus(batchStatus.name());
        flowExecution.setCreateTime(now);
        flowExecution.setLastUpdateTime(now);
        return flowExecution;
    }

    @Override
    public StepExecutionImpl createStepExecution(final long rootJobExecId, final StepContextImpl stepContext) {
        final String batchStatus = stepContext.getBatchStatus() == null ? BatchStatus.STARTING.name() : stepContext.getBatchStatus().name();
        final String exitStatus = stepContext.getExitStatus();
        final String stepName = stepContext.getStepName();

        long readCount = 0;
        long writeCount = 0;
        long commitCount = 0;
        long rollbackCount = 0;
        long readSkipCount = 0;
        long processSkipCount = 0;
        long filterCount = 0;
        long writeSkipCount = 0;
        Timestamp startTime = stepContext.getStartTimeTS();
        Timestamp endTime = stepContext.getEndTimeTS();

        final Metric[] metrics = stepContext.getMetrics();
        for (final Metric metric : metrics) {
            if (metric.getType().equals(MetricImpl.MetricType.READ_COUNT)) {
                readCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_COUNT)) {
                writeCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.PROCESS_SKIP_COUNT)) {
                processSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.COMMIT_COUNT)) {
                commitCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.ROLLBACK_COUNT)) {
                rollbackCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.READ_SKIP_COUNT)) {
                readSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.FILTER_COUNT)) {
                filterCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_SKIP_COUNT)) {
                writeSkipCount = metric.getValue();
            }
        }
        final Serializable persistentData = stepContext.getPersistentUserData();

        return createStepExecution(rootJobExecId, batchStatus, exitStatus, stepName, readCount,
            writeCount, commitCount, rollbackCount, readSkipCount, processSkipCount, filterCount, writeSkipCount, startTime,
            endTime, persistentData);
    }


    private StepExecutionImpl createStepExecution(final long rootJobExecId, final String batchStatus, final String exitStatus, final String stepName, final long readCount,
                                                  final long writeCount, final long commitCount, final long rollbackCount, final long readSkipCount, final long processSkipCount, final long filterCount,
                                                  final long writeSkipCount, final Timestamp startTime, final Timestamp endTime, final Serializable persistentData) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getCreateStepExecution(), Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, rootJobExecId);
            statement.setString(2, batchStatus);
            statement.setString(3, exitStatus);
            statement.setString(4, stepName);
            statement.setLong(5, readCount);
            statement.setLong(6, writeCount);
            statement.setLong(7, commitCount);
            statement.setLong(8, rollbackCount);
            statement.setLong(9, readSkipCount);
            statement.setLong(10, processSkipCount);
            statement.setLong(11, filterCount);
            statement.setLong(12, writeSkipCount);
            statement.setTimestamp(13, startTime);
            statement.setTimestamp(14, endTime);
            statement.setObject(15, serialize(persistentData));

            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }

            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                final long stepExecutionId = rs.getLong(1);
                final StepExecutionImpl stepExecution = new StepExecutionImpl(rootJobExecId, stepExecutionId);
                stepExecution.setStepName(stepName);
                return stepExecution;
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public void updateStepExecution(final long rootJobExecId, final StepContextImpl stepContext) {
        final long stepExecutionId = stepContext.getStepExecutionId();
        final String batchStatus = stepContext.getBatchStatus() == null ? BatchStatus.STARTING.name() : stepContext.getBatchStatus().name();
        final String exitStatus = stepContext.getExitStatus();
        final String stepName = stepContext.getStepName();
        final Timestamp startTime = stepContext.getStartTimeTS();
        final Timestamp endTime = stepContext.getEndTimeTS();

        long readCount = 0;
        long writeCount = 0;
        long commitCount = 0;
        long rollbackCount = 0;
        long readSkipCount = 0;
        long processSkipCount = 0;
        long filterCount = 0;
        long writeSkipCount = 0;

        final Metric[] metrics = stepContext.getMetrics();
        for (final Metric metric : metrics) {
            if (metric.getType().equals(MetricImpl.MetricType.READ_COUNT)) {
                readCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_COUNT)) {
                writeCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.PROCESS_SKIP_COUNT)) {
                processSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.COMMIT_COUNT)) {
                commitCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.ROLLBACK_COUNT)) {
                rollbackCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.READ_SKIP_COUNT)) {
                readSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.FILTER_COUNT)) {
                filterCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_SKIP_COUNT)) {
                writeSkipCount = metric.getValue();
            }
        }
        final Serializable persistentData = stepContext.getPersistentUserData();

        updateStepExecution(stepExecutionId, rootJobExecId, batchStatus, exitStatus, stepName, readCount,
            writeCount, commitCount, rollbackCount, readSkipCount, processSkipCount, filterCount,
            writeSkipCount, startTime, endTime, persistentData);

    }


    private void updateStepExecution(final long stepExecutionId, final long jobExecId, final String batchStatus, final String exitStatus, final String stepName, final long readCount,
                                     final long writeCount, final long commitCount, final long rollbackCount, final long readSkipCount, final long processSkipCount, final long filterCount,
                                     final long writeSkipCount, final Timestamp startTime, final Timestamp endTime, final Serializable persistentData) {
        Connection conn = null;
        PreparedStatement statement = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getUpdateStepExecution());
            statement.setLong(1, jobExecId);
            statement.setString(2, batchStatus);
            statement.setString(3, exitStatus);
            statement.setString(4, stepName);
            statement.setLong(5, readCount);
            statement.setLong(6, writeCount);
            statement.setLong(7, commitCount);
            statement.setLong(8, rollbackCount);
            statement.setLong(9, readSkipCount);
            statement.setLong(10, processSkipCount);
            statement.setLong(11, filterCount);
            statement.setLong(12, writeSkipCount);
            statement.setTimestamp(13, startTime);
            statement.setTimestamp(14, endTime);
            statement.setObject(15, serialize(persistentData));
            statement.setLong(16, stepExecutionId);
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public JobStatus createJobStatus(final long jobInstanceId) {
        return new JobStatus(jobInstanceId); // instance already created
    }

    @Override
    public JobStatus getJobStatus(final long instanceId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindJobInstance());
            statement.setLong(1, instanceId);
            rs = statement.executeQuery();
            if (rs.next()) {
                final JobStatus status = new JobStatus(instanceId);
                status.setCurrentStepId(dictionary.jobExecutionColumns(7));
                status.setExitStatus(dictionary.jobExecutionColumns(2));

                final byte[] jobXmls = rs.getBytes(dictionary.jobInstanceColumns(4));
                final JobInstanceImpl instance;
                if (jobXmls != null) {
                    instance = new JobInstanceImpl(instanceId, new String(jobXmls, UTF_8));
                } else {
                    instance = new JobInstanceImpl(instanceId);
                }
                instance.setJobName(rs.getString(dictionary.jobInstanceColumns(3)));

                status.setJobInstance(instance);

                status.setLatestExecutionId(rs.getLong(dictionary.jobInstanceColumns(5)));
                status.setRestartOn(rs.getString(dictionary.jobInstanceColumns(6)));

                final String batchStatus = rs.getString(dictionary.stepExecutionColumns(1));
                if (batchStatus != null) {
                    status.setBatchStatus(BatchStatus.valueOf(batchStatus));
                }

                return status;
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#updateJobStatus(long, org.apache.batchee.container.status.JobStatus)
     */
    @Override
    public void updateJobStatus(final long instanceId, final JobStatus jobStatus) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getUpdateJobInstanceStatus());
            if (jobStatus.getBatchStatus() != null) {
                statement.setString(1, jobStatus.getBatchStatus().name());
            } else {
                statement.setString(1, null);
            }
            statement.setString(2, jobStatus.getExitStatus());
            statement.setLong(3, jobStatus.getLatestExecutionId());
            statement.setString(4, jobStatus.getRestartOn());
            statement.setString(5, jobStatus.getCurrentStepId());
            if (jobStatus.getJobInstance() != null) {
                statement.setString(6, jobStatus.getJobInstance().getJobName());
            } else {
                statement.setString(6, null);
            }
            statement.setLong(7, instanceId);
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public StepStatus createStepStatus(final long stepExecId) {
        return new StepStatus(stepExecId); // instance already created
    }

    @Override
    public StepStatus getStepStatus(final long instanceId, final String stepName) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindStepExecutionByJobInstanceAndStepName());
            statement.setLong(1, instanceId);
            statement.setString(2, stepName);
            rs = statement.executeQuery();
            if (rs.next()) {
                final int startCount = rs.getInt(dictionary.stepExecutionColumns(13));
                final long id = rs.getLong(dictionary.stepExecutionColumns(0));
                final StepStatus stepStatus = new StepStatus(id, startCount);
                final int numPartitions = rs.getInt(dictionary.stepExecutionColumns(7));
                final byte[] persistentDatas = rs.getBytes(dictionary.stepExecutionColumns(8));
                if (numPartitions >= 0) {
                    stepStatus.setNumPartitions(numPartitions);
                }
                if (persistentDatas != null) {
                    stepStatus.setPersistentUserData(new PersistentDataWrapper(persistentDatas));
                }
                stepStatus.setBatchStatus(BatchStatus.valueOf(rs.getString(dictionary.stepExecutionColumns(1))));
                stepStatus.setExitStatus(rs.getString(dictionary.stepExecutionColumns(4)));
                stepStatus.setLastRunStepExecutionId(rs.getLong(dictionary.stepExecutionColumns(6)));
                stepStatus.setStepExecutionId(id);
                return stepStatus;
            }
            return null;
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
    }

    @Override
    public void updateStepStatus(final long stepExecutionId, final StepStatus stepStatus) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getUpdateStepExecutionStatus());
            statement.setBytes(1, stepStatus.getRawPersistentUserData());
            if (stepStatus.getBatchStatus() != null) {
                statement.setString(2, stepStatus.getBatchStatus().name());
            } else {
                statement.setString(2, null);
            }
            statement.setString(3, stepStatus.getExitStatus());
            statement.setLong(4, stepStatus.getLastRunStepExecutionId());
            if (stepStatus.getNumPartitions() != null) {
                statement.setInt(5, stepStatus.getNumPartitions());
            } else {
                statement.setInt(5, -1);
            }
            statement.setInt(6, stepStatus.getStartCount());
            statement.setLong(7, stepStatus.getStepExecutionId());
            statement.executeUpdate();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public long getMostRecentExecutionId(final long jobInstanceId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(dictionary.getFindMostRecentJobExecution());
            statement.setLong(1, jobInstanceId);
            rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return -1;
    }

    @Override
    public void cleanUp(final long instanceId) {
        Connection conn = null;
        try {
            conn = getConnection();
            deleteFromInstanceId(instanceId, conn, dictionary.getDeleteStepExecution());
            deleteFromInstanceId(instanceId, conn, dictionary.getDeleteCheckpoint());
            deleteFromInstanceId(instanceId, conn, dictionary.getDeleteJobExecution());
            deleteFromInstanceId(instanceId, conn, dictionary.getDeleteJobInstance());
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, null);
        }
    }

    private static void deleteFromInstanceId(final long instanceId, final Connection conn, final String delete) throws SQLException {
        final PreparedStatement statement = conn.prepareStatement(delete);
        statement.setLong(1, instanceId);
        statement.executeUpdate();
        statement.close();
    }
}
