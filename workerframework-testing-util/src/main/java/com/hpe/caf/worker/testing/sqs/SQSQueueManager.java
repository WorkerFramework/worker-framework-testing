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

import com.hpe.caf.api.CodecException;
import com.hpe.caf.api.worker.TaskMessage;
import com.hpe.caf.worker.queue.sqs.QueueInfo;
import com.hpe.caf.worker.queue.sqs.config.SQSWorkerQueueConfiguration;
import com.hpe.caf.worker.queue.sqs.util.SQSUtil;
import com.hpe.caf.worker.testing.QueueManager;
import com.hpe.caf.worker.testing.ResultHandler;
import com.hpe.caf.worker.testing.WorkerServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SQSQueueManager implements QueueManager
{
    private static final Logger LOG = LoggerFactory.getLogger(SQSQueueManager.class);

    private final SQSWorkerQueueConfiguration queueCfg;
    private SqsClient sqsClient;
    private QueueInfo inputQueueInfo;
    private QueueInfo resultsQueueInfo;
    private QueueInfo debugInputQueueInfo;
    private QueueInfo debugResultQueueInfo;
    private ResultQueueConsumer resultQueueConsumer;
    private final WorkerServices workerServices;
    private final String debugInputQueueName;
    private final String resultsQueueName;
    private final String debugResultsQueueName;
    private final boolean debugEnabled;

    public SQSQueueManager(
            final SQSWorkerQueueConfiguration queueCfg,
            final WorkerServices workerServices,
            final String resultsQueue,
            final boolean debugEnabled)
    {
        this.queueCfg = queueCfg;
        this.resultsQueueName = resultsQueue;
        this.workerServices = workerServices;
        this.debugInputQueueName = queueCfg.getInputQueue() + "-debug";
        this.debugResultsQueueName = resultsQueue + "-debug";
        this.debugEnabled = debugEnabled;
    }

    @Override
    public Thread start(final ResultHandler resultHandler) throws Exception
    {
        sqsClient = SQSUtil.getSqsClient(queueCfg.getSqsConfiguration());

        LOG.info("SQS url: {}", queueCfg.getSqsConfiguration().getURIString());

        inputQueueInfo = SQSUtil.createQueue(sqsClient, queueCfg.getInputQueue(), queueCfg);
        resultsQueueInfo = SQSUtil.createQueue(sqsClient, resultsQueueName, queueCfg);

        final var resultHandlerCallback = new ResultHandlerCallback(resultHandler, workerServices.getCodec());

        resultQueueConsumer = new ResultQueueConsumer(
                sqsClient,
                resultsQueueInfo,
                queueCfg,
                resultHandlerCallback
        );

        purgeQueues();


        if (debugEnabled) {
            debugInputQueueInfo = SQSUtil.createQueue(sqsClient, debugInputQueueName, queueCfg);
            debugResultQueueInfo = SQSUtil.createQueue(sqsClient, debugResultsQueueName, queueCfg);
            purgeQueue(debugInputQueueInfo);
            purgeQueue(debugResultQueueInfo);
        }

        Thread resultQueueThread = new Thread(resultQueueConsumer);
        resultQueueThread.start();

        return resultQueueThread;
    }

    private void purgeQueue(final QueueInfo queueInfo)
    {
        final var purgeRequest = PurgeQueueRequest.builder()
                .queueUrl(queueInfo.url())
                .build();
        sqsClient.purgeQueue(purgeRequest);
    }

    @Override
    public void purgeQueues() throws IOException
    {
        purgeQueue(inputQueueInfo);
        purgeQueue(resultsQueueInfo);
    }

    @Override
    public void publish(TaskMessage message) throws CodecException, IOException
    {
        byte[] data = workerServices.getCodec().serialise(message);
        sendMessage(inputQueueInfo.url(), new HashMap<>(), new String(data, StandardCharsets.UTF_8));
        if (debugEnabled) {
            sendMessage(debugInputQueueInfo.url(), new HashMap<>(), new String(data, StandardCharsets.UTF_8));
        }
    }

    @Override
    public void publishDebugOutput(TaskMessage message) throws CodecException, IOException
    {
        byte[] data = workerServices.getCodec().serialise(message);
        sendMessage(debugResultQueueInfo.url(), new HashMap<>(), new String(data, StandardCharsets.UTF_8));
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }

    @Override
    public String getWorkerInputQueueName()
    {
        return queueCfg.getInputQueue();
    }

    @Override
    public void close() throws IOException
    {
        resultQueueConsumer.shutdown();
    }

    public void sendMessage(
            final String url,
            final Map<String, MessageAttributeValue> messageAttributes,
            final String message)
    {
        final var sendRequest = SendMessageRequest.builder()
                .queueUrl(url)
                .messageBody(message)
                .messageAttributes(messageAttributes)
                .build();
        sqsClient.sendMessage(sendRequest);
    }
}
