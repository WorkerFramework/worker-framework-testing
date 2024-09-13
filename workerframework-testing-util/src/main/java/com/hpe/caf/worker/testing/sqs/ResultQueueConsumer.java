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

import com.hpe.caf.api.worker.TaskCallback;
import com.hpe.caf.worker.queue.sqs.QueueInfo;
import com.hpe.caf.worker.queue.sqs.SQSTaskInformation;
import com.hpe.caf.worker.queue.sqs.config.SQSWorkerQueueConfiguration;
import com.hpe.caf.worker.queue.sqs.consumer.QueueConsumer;
import com.hpe.caf.worker.queue.sqs.util.SQSUtil;
import com.hpe.caf.worker.queue.sqs.visibility.VisibilityTimeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ResultQueueConsumer implements Runnable
{
    protected final SqsClient sqsClient;
    protected final QueueInfo queueInfo;
    protected final SQSWorkerQueueConfiguration queueCfg;
    protected final TaskCallback callback;
    protected final AtomicBoolean running = new AtomicBoolean(true);

    private static final Logger LOG = LoggerFactory.getLogger(QueueConsumer.class);

    public ResultQueueConsumer(
            final SqsClient sqsClient,
            final QueueInfo queueInfo,
            final SQSWorkerQueueConfiguration queueCfg,
            final TaskCallback callback)
    {
        this.sqsClient = sqsClient;
        this.queueInfo = queueInfo;
        this.queueCfg = queueCfg;
        this.callback = callback;
    }

    @Override
    public void run()
    {
        final var receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueInfo.url())
                .maxNumberOfMessages(queueCfg.getMaxNumberOfMessages())
                .waitTimeSeconds(queueCfg.getLongPollInterval())
                .messageSystemAttributeNames(MessageSystemAttributeName.ALL)
                .messageAttributeNames(SQSUtil.ALL_ATTRIBUTES)
                .build();
        while (running.get()) {
            receiveMessages(receiveRequest);
        }
    }

    protected void receiveMessages(final ReceiveMessageRequest receiveRequest)
    {
        final var receiveMessageResult = sqsClient.receiveMessage(receiveRequest).messages();
        if (!receiveMessageResult.isEmpty()) {
            LOG.debug("Received {} messages from queue {} \n{}",
                    receiveMessageResult.size(),
                    queueInfo.name(),
                    receiveMessageResult.stream().map(Message::receiptHandle).collect(Collectors.toSet()));
        }

        for(final var message : receiveMessageResult) {
            registerNewTask(message);
        }
    }

    protected Map<String, Object> createHeadersFromMessageAttributes(final Message message)
    {
        final var headers = new HashMap<String, Object>();
        for(final Map.Entry<String, MessageAttributeValue> entry : message.messageAttributes().entrySet()) {
            if (entry.getValue().dataType().equals("String")) {
                headers.put(entry.getKey(), entry.getValue().stringValue());
            }
        }
        return headers;
    }

    protected void registerNewTask(final Message message)
    {
        final var becomesVisible = Instant.now().getEpochSecond() + queueCfg.getVisibilityTimeout();
        final var taskInfo = new SQSTaskInformation(
                message.messageId(),
                new VisibilityTimeout(queueInfo, becomesVisible, message.receiptHandle()),
                false
        );

        final var headers = createHeadersFromMessageAttributes(message);
        try {
            callback.registerNewTask(taskInfo, message.body().getBytes(StandardCharsets.UTF_8), headers);
        } catch (final Exception e) {
            LOG.warn("Message {} rejected as a task at this time, will be redelivered by SQS",
                    taskInfo, e);
        }
    }

    public void shutdown()
    {
        running.set(false);
    }
}
