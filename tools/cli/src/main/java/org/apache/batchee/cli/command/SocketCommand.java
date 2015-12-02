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

import org.apache.batchee.cli.command.api.Option;
import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class SocketCommand extends SocketConfigurableCommand {
    @Option(name = "timeout", description = "timeout for socket case")
    private int timeout = 60000;

    protected boolean shouldUseSocket() {
        return baseUrl == null;
    }

    protected abstract String command();
    protected abstract void defaultRun();

    protected void sendCommand() {
        if (adminSocket < 0) {
            throw new BatchContainerRuntimeException("specify -socket to be able to run this command");
        }

        Socket socket = null;
        try {
            socket = new Socket("localhost", adminSocket);
            socket.setSoTimeout(timeout);

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Integer> answer = new AtomicReference<Integer>();
            new AnswerThread(socket, answer, latch).start();

            final OutputStream outputStream = socket.getOutputStream();
            outputStream.write(command().getBytes());
            outputStream.flush();
            socket.shutdownOutput();

            try {
                latch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                info("no answer after " + timeout + "ms");
                return;
            }
            if (answer.get() != 0) {
                info("unexpected answer: " + answer.get());
            }
        } catch (final IOException e) {
            throw new BatchContainerRuntimeException(e);
        } finally {
            IOUtils.closeQuietly(socket);
        }
    }

    @Override
    public void doRun() {
        if (shouldUseSocket()) {
            sendCommand();
        } else {
            defaultRun();
        }
        postCommand();
    }

    protected abstract void postCommand();

    private static class AnswerThread extends Thread {
        private final Socket socket;
        private final AtomicReference<Integer> answer;
        private final CountDownLatch latch;

        public AnswerThread(final Socket socket, final AtomicReference<Integer> answer, final CountDownLatch latch) {
            this.socket = socket;
            this.answer = answer;
            this.latch = latch;
            setName("batchee-answer-thread");
        }

        @Override
        public void run() {
            try {
                answer.set(socket.getInputStream().read());
            } catch (IOException e) {
                answer.set(-1);
            }
            latch.countDown();
        }
    }
}
