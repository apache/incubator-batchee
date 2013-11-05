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
package org.apache.batchee.extras;

import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class JdbcBatchletTest {
    @Test
    public void read() throws Exception {
        {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            final Connection c = newConnection();

            PreparedStatement statement = c.prepareStatement("CREATE TABLE FOO("
                + "id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) CONSTRAINT FOO_PK PRIMARY KEY,"
                + "name VARCHAR(512))");
            statement.executeUpdate();
            statement.close();

            statement = c.prepareStatement("INSERT INTO FOO (name) VALUES(?)");
            statement.setString(1, "toto");
            statement.executeUpdate();
            statement.close();

            statement = c.prepareStatement("INSERT INTO FOO (name) VALUES(?)");
            statement.setString(1, "titi");
            statement.executeUpdate();
            statement.close();

            statement = c.prepareStatement("INSERT INTO FOO (name) VALUES(?)");
            statement.setString(1, "titi");
            statement.executeUpdate();
            statement.close();

            c.close();
        }

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("jdbc-batchlet", new Properties()));

        final Connection c = newConnection();
        final ResultSet resultSet = c.prepareStatement("select count(*) as total from FOO").executeQuery();
        resultSet.next();
        try {
            assertEquals(1, resultSet.getInt("total"));
        } finally {
            c.close();
        }
    }

    private static Connection newConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:derby:memory:jdbcbatchlet;create=true", "app", "app");
    }
}
