/*
 * Copyright 2022 Micro Focus or one of its affiliates.
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
package com.hpe.caf.worker.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.google.common.base.Strings;
import com.hpe.caf.api.CodecException;
import com.hpe.caf.api.worker.QueueTaskMessage;
import com.hpe.caf.api.worker.TaskMessage;

/**
 * Created by ploch on 08/11/2015.
 */
public class ProcessorDeliveryHandler<T> implements ResultHandler<T>
{
    private final ResultProcessor resultProcessor;
    private ExecutionContext context;
    private QueueManager queueManager;

    public ProcessorDeliveryHandler(ResultProcessor resultProcessor, ExecutionContext context, QueueManager queueManager)
    {
        this.resultProcessor = resultProcessor;
        this.context = context;
        this.queueManager = queueManager;
    }

    @Override
    public void handleResult(final T input)
    {

        final TaskMessage taskMessage = convertIntoTaskMessage(input);
        if (this.queueManager.isDebugEnabled()) {
            try {
                queueManager.publishDebugOutput(taskMessage);
            } catch (CodecException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("New delivery: task id: " + taskMessage.getTaskId() + ", status: " + taskMessage.getTaskStatus());

        TestItem testItem;

        String inputIdentifier = resultProcessor.getInputIdentifier(taskMessage);
        if (Strings.isNullOrEmpty(inputIdentifier)) {
            testItem = context.getItemStore().find(taskMessage.getTaskId());
        } else {
            testItem = context.getItemStore().find(inputIdentifier);
        }
        if (testItem == null) {
            System.out.println("Item with id " + taskMessage.getTaskId() + " was not found. Skipping.");
            checkForFinished();
            return;
        }

        try {
            boolean success = resultProcessor.process(testItem, taskMessage);
            System.out.println("Item " + testItem.getTag() + ": Result processor success: " + success);
            if (!success) {
                context.failed(testItem, "Item " + testItem.getTag() + ": Result processor didn't return success. Result processor name: " + resultProcessor.getClass().getName() + "\nNo detailed message returned.");
                testItem.setCompleted(true);
            } else {
                context.succeeded(testItem);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            context.failed(testItem, buildFailedMessage(testItem, e));
            testItem.setCompleted(true);
        }

        if (testItem.isCompleted()) {
            context.getItemStore().remove(testItem.getTag());
        }
        checkForFinished();
    }

    private TaskMessage convertIntoTaskMessage(final T input)
    {
        final TaskMessage taskMessage;
        if (input instanceof QueueTaskMessage) {
            QueueTaskMessage qtm = (QueueTaskMessage)input;
            taskMessage = new TaskMessage();
            taskMessage.setContext(qtm.getContext());
            taskMessage.setCorrelationId(qtm.getCorrelationId());
            taskMessage.setPriority(qtm.getPriority());
            taskMessage.setSourceInfo(qtm.getSourceInfo());
            taskMessage.setTaskClassifier(qtm.getTaskClassifier());
            taskMessage.setTaskApiVersion(qtm.getTaskApiVersion());
            taskMessage.setTaskData(qtm.getTaskData().toString().getBytes(StandardCharsets.UTF_8));
            taskMessage.setTaskStatus(qtm.getTaskStatus());
            taskMessage.setTracking(qtm.getTracking());
            taskMessage.setTaskId(qtm.getTaskId());
            taskMessage.setVersion(qtm.getVersion());
        } else {
            taskMessage = (TaskMessage)input;
        }
        return taskMessage;
    }

    private String buildFailedMessage(TestItem testItem, Throwable throwable)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Test case failed.");
        TestCaseInfo info = testItem.getTestCaseInformation();
        if (info != null) {
            sb.append(" Test case id: " + info.getTestCaseId());
            sb.append("\nTest case description: " + info.getDescription());
            sb.append("\nTest case comments: " + info.getComments());
            sb.append("\nTest case associated tickets: " + info.getAssociatedTickets());
            sb.append("\n");
        }
        sb.append("Message: " + ExceptionUtils.getMessage(throwable));
        sb.append("\n");
        sb.append("Root cause message: " + ExceptionUtils.getRootCauseMessage(throwable));

        sb.append("\nStack trace:\n");
        sb.append(ExceptionUtils.getStackTrace(throwable));
        return sb.toString();
    }

    private void checkForFinished()
    {
        if (context.getItemStore().size() == 0) {
            System.out.println("Test case has no remaining items awaiting processing.");
            context.finishedSuccessfully();
        } else {
            System.out.println("Test case has " + context.getItemStore().size() + " result items yet to be processed.");
        }
    }
}
