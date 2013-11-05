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
package org.apache.batchee.test.tck.lifecycle;

import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.openejb.testng.PropertiesBuilder;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import javax.batch.operations.BatchRuntimeException;
import javax.ejb.embeddable.EJBContainer;
import java.util.logging.Logger;

// forces the execution in embedded container
public class ContainerLifecycle implements ITestListener {
    private EJBContainer container;
    private Logger logger = null;

    @Override
    public void onTestStart(final ITestResult iTestResult) {
        logger.info("====================================================================================================");
        logger.info(iTestResult.getMethod().getRealClass().getName() + "#" + iTestResult.getMethod().getMethodName());
        logger.info("----------------------------------------------------------------------------------------------------");
    }

    @Override
    public void onTestSuccess(final ITestResult iTestResult) {
        logger.info(">>> SUCCESS");
    }

    @Override
    public void onTestFailure(final ITestResult iTestResult) {
        logger.severe(">>> FAILURE");
    }

    @Override
    public void onTestSkipped(final ITestResult iTestResult) {
        logger.warning(">>> SKIPPED");
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(final ITestResult iTestResult) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onStart(final ITestContext iTestContext) {
        final String loggerName = "test-lifecycle";

        container = EJBContainer.createEJBContainer(new PropertiesBuilder()
            .p("openejb.jul.forceReload", Boolean.TRUE.toString())
            .p("openejb.log.color", Boolean.toString(!System.getProperty("os.name").toLowerCase().contains("win")))
            .p(loggerName + ".level", "INFO")
            .p("openejb.jdbc.log", Boolean.FALSE.toString()) // with jdbc set it to TRUE to get sql queries

            .p("jdbc/orderDB", "new://Resource?type=DataSource")
            .p("jdbc/orderDB.JdbcDriver", EmbeddedDriver.class.getName())
            .p("jdbc/orderDB.JdbcUrl", "jdbc:derby:memory:orderDB" + ";create=true")
            .p("jdbc/orderDB.UserName", "app")
            .p("jdbc/orderDB.Password", "app")
            .p("jdbc/orderDB.JtaManaged", Boolean.TRUE.toString())

            .p("jdbc/batchee", "new://Resource?type=DataSource")
            .p("jdbc/batchee.JdbcDriver", EmbeddedDriver.class.getName())
            .p("jdbc/batchee.JdbcUrl", "jdbc:derby:memory:batchee" + ";create=true")
            .p("jdbc/batchee.UserName", "app")
            .p("jdbc/batchee.Password", "app")
            .p("jdbc/batchee.JtaManaged", Boolean.FALSE.toString())
            .build());

        logger = Logger.getLogger(loggerName);
    }

    @Override
    public void onFinish(final ITestContext iTestContext) {
        if (container != null) {
            try {
                container.close();
            } catch (final Exception e) {
                throw new BatchRuntimeException(e);
            }
        }
    }
}
