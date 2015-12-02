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
package org.apache.batchee.cli.command;

import org.apache.batchee.cli.command.api.Arguments;
import org.apache.batchee.cli.command.api.Option;
import org.apache.batchee.util.Batches;
import org.apache.commons.io.IOUtils;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.StepExecution;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public abstract class StartableCommand extends SocketConfigurableCommand {
    private static final String LINE = "=========================";

    @Arguments(description = "properties to pass to the batch")
    protected List<String> properties;

    // some unix systems dont support negative systems.
    @Option(name = "errorExitCode", description = "exit code if any error occurs, should be > 0 or ignored")
    protected int errorExitCode = -1;

    @Option(name = "failureExitCode", description = "exit code if the batch result is not completed, should be > 0 and wait should be true or ignored")
    protected int failureExitCode = -1;

    @Override
    public void doRun() {
        final JobOperator operator = operator();

        final AdminThread adminThread;
        if (adminSocket > 0) {
            adminThread = new AdminThread(operator, adminSocket);
            adminThread.setName("batchee-admin-thread");
            adminThread.start();
        } else {
            info("Admin mode deactivated, use -socket to activate it");
            adminThread = null;
        }

        final long id;
        try {
            id = doStart(operator);
        } catch (final Exception e) {
            if (adminThread != null && adminThread.getServerSocket() != null) {
                IOUtils.closeQuietly(adminThread.getServerSocket());
            }
            if (errorExitCode >= 0) {
                System.exit(errorExitCode);
            }
            e.printStackTrace(); // ensure it is traced
            return;
        }

        try {
            if (wait) {
                finishBatch(operator, id);
            }
        } finally {
            stopAdminThread(adminThread, id);
        }
    }

    private void finishBatch(final JobOperator operator, final long id) {
        Batches.waitForEnd(operator, id);
        if (report(operator, id).getBatchStatus() == BatchStatus.FAILED) {
            if (failureExitCode >= 0) {
                System.exit(failureExitCode);
            }
        }
    }

    private void stopAdminThread(final AdminThread adminThread, final long id) {
        if (adminThread != null) {
            adminThread.setId(id);
            if (wait) {
                try {
                    try {
                        adminThread.serverSocket.close();
                    } catch (final IOException e) {
                        // no-op
                    }
                    adminThread.join();
                } catch (final InterruptedException e) {
                    Thread.interrupted();
                }
            } // else let it live
        }
    }

    protected abstract long doStart(JobOperator operator);

    private JobExecution report(final JobOperator operator, final long id) {
        final JobExecution execution = operator.getJobExecution(id);

        info("");
        info(LINE);

        info("Batch status: " + statusToString(execution.getBatchStatus()));
        info("Exit status:  " + execution.getExitStatus());
        if (execution.getEndTime() != null && execution.getStartTime() != null) {
            info("Duration:     " + TimeUnit.MILLISECONDS.toSeconds(execution.getEndTime().getTime() - execution.getStartTime().getTime()) + "s");
        }

        if (BatchStatus.FAILED.equals(execution.getBatchStatus())) {
            final Collection<StepExecution> stepExecutions = operator.getStepExecutions(id);
            for (final StepExecution stepExecution : stepExecutions) {
                if (BatchStatus.FAILED.equals(stepExecution.getBatchStatus())) {
                    info("");
                    info("Step name       : " + stepExecution.getStepName());
                    info("Step status     : " + statusToString(stepExecution.getBatchStatus()));
                    info("Step exit status: " + stepExecution.getExitStatus());
                    break;
                }
            }
        }

        info(LINE);
        return execution;
    }

    private static String statusToString(final BatchStatus status) {
        return (status != null ? status.name() : "null");
    }

    protected static Properties toProperties(final List<String> properties) {
        final Properties props = new Properties();
        if (properties != null) {
            for (final String kv : properties) {
                final String[] split = kv.split("=");
                if (split.length > 1) {
                    props.setProperty(split[0], split[1]);
                } else {
                    props.setProperty(split[0], "");
                }
            }
        }
        return props;
    }

    private static class AdminThread extends Thread {
        private final JobOperator operator;
        private final int adminSocketPort;
        private ServerSocket serverSocket = null;
        private long id = Integer.MIN_VALUE;

        public AdminThread(final JobOperator operator, final int adminSocket) {
            this.operator = operator;
            this.adminSocketPort = adminSocket;
        }

        @Override
        public void run() {
            try {
                serverSocket = new ServerSocket(adminSocketPort);
                while (Integer.MIN_VALUE == id || !Batches.isDone(operator, id)) {
                    final Socket client = serverSocket.accept();
                    final OutputStream outputStream = client.getOutputStream();
                    synchronized (this) { // no need to support N clients
                        try {
                            final String[] command = IOUtils.toString(client.getInputStream()).trim().split(" ");
                            if (command.length >= 2) {
                                final long id = Long.parseLong(command[1]);
                                try {
                                    if ("stop".equals(command[0])) {
                                        operator.stop(id);
                                    } else if ("abandon".equals(command[0])) {
                                        operator.abandon(id);
                                    }
                                } catch (final Exception e) {
                                    // no-op
                                }

                                if (command.length >= 3 && Boolean.parseBoolean(command[2])) {
                                    Batches.waitForEnd(id);
                                }

                                // let the client close if waiting
                                outputStream.write(0);
                            } else { // error
                                outputStream.write(-1);
                            }
                            outputStream.flush();
                        } finally {
                            IOUtils.closeQuietly(client);
                        }
                    }
                }
            } catch (final IOException e) {
                if (!serverSocket.isClosed()) {
                    e.printStackTrace();
                }
            } finally {
                if (!serverSocket.isClosed()) {
                    IOUtils.closeQuietly(serverSocket);
                }
            }
        }

        public ServerSocket getServerSocket() {
            return serverSocket;
        }

        public void setId(final long id) {
            this.id = id;
        }
    }
}
