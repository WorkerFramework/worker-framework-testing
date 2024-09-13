/*
 * Copyright 2022-2024 Open Text.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hpe.caf.worker.testing.sqs;

import com.hpe.caf.api.Codec;
import com.hpe.caf.api.CodecException;
import com.hpe.caf.api.DecodeMethod;
import com.hpe.caf.api.worker.TaskCallback;
import com.hpe.caf.api.worker.TaskInformation;
import com.hpe.caf.api.worker.TaskMessage;
import com.hpe.caf.worker.testing.ResultHandler;

import java.util.Map;

public class ResultHandlerCallback implements TaskCallback
{
    private final ResultHandler resultHandler;
    private final Codec codec;
    private static final Object syncLock = new Object();

    public ResultHandlerCallback(final ResultHandler resultHandler, final Codec codec)
    {
        this.resultHandler = resultHandler;
        this.codec = codec;
    }

    @Override
    public void registerNewTask(
            final TaskInformation taskInformation,
            final byte[] bytes,
            final Map<String, Object> headers
    )
    {
        try {
            final TaskMessage taskMessage = codec.deserialise(bytes, TaskMessage.class, DecodeMethod.LENIENT);
            System.out.println(taskMessage.getTaskId() + ", status: " + taskMessage.getTaskStatus());
            synchronized (syncLock) {
                resultHandler.handleResult(taskMessage);
            }
        } catch (CodecException e) {
            e.printStackTrace();
            throw new AssertionError("Failed: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AssertionError("Failed: " + e.getMessage());
        }
    }

    @Override
    public void abortTasks()
    {

    }
}
