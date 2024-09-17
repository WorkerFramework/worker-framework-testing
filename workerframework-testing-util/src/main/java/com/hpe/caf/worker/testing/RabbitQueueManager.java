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
package com.hpe.caf.worker.testing;

import com.hpe.caf.api.CodecException;
import com.hpe.caf.api.worker.TaskMessage;
import com.hpe.caf.util.rabbitmq.DefaultRabbitConsumer;
import com.hpe.caf.util.rabbitmq.Event;
import com.hpe.caf.util.rabbitmq.QueueConsumer;
import com.hpe.caf.util.rabbitmq.QueueCreator;
import com.hpe.caf.util.rabbitmq.RabbitUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * Created by ploch on 08/11/2015.
 */
public class RabbitQueueManager implements QueueManager
{
    private final QueueServices queueServices;
    private final WorkerServices workerServices;
    private String consumerTag;

    private Channel pubChan;
    private Channel debugPubChan;
    private Channel conChan;
    private Channel debugConChan;

    private DefaultRabbitConsumer rabbitConsumer;
    private Connection connection;

    private String debugInputQueueName;
    private String debugOutputQueueName;
    private boolean debugEnabled;

    public RabbitQueueManager(QueueServices queueServices, WorkerServices workerServices, boolean debugEnabled)
    {

        this.queueServices = queueServices;
        this.workerServices = workerServices;
        this.debugInputQueueName = this.queueServices.getWorkerInputQueue() + "-debug";
        this.debugOutputQueueName = this.queueServices.getWorkerResultsQueue() + "-debug";
        this.debugEnabled = debugEnabled;
    }

    @Override
    public Thread start(ResultHandler resultHandler) throws IOException
    {
        connection = queueServices.getConnection();
        pubChan = connection.createChannel();
        conChan = connection.createChannel();
        RabbitUtil.declareWorkerQueue(
                pubChan, queueServices.getWorkerInputQueue(), queueServices.getMaxPriority(),
                QueueCreator.RABBIT_PROP_QUEUE_TYPE_QUORUM
        );
        if(StringUtils.isNotEmpty(queueServices.getWorkerResultsQueue())) {
            RabbitUtil.declareWorkerQueue(
                    conChan, queueServices.getWorkerResultsQueue(), queueServices.getMaxPriority(),
                    QueueCreator.RABBIT_PROP_QUEUE_TYPE_QUORUM
            );
        }
        purgeQueues();

        if (debugEnabled) {
            debugPubChan = connection.createChannel();
            debugConChan = connection.createChannel();
            RabbitUtil.declareWorkerQueue(
                    debugPubChan, debugInputQueueName, queueServices.getMaxPriority(),
                    QueueCreator.RABBIT_PROP_QUEUE_TYPE_QUORUM
            );
            RabbitUtil.declareWorkerQueue(
                    debugConChan, debugOutputQueueName, queueServices.getMaxPriority(),
                    QueueCreator.RABBIT_PROP_QUEUE_TYPE_QUORUM
            );
            debugPubChan.queuePurge(debugInputQueueName);
            debugConChan.queuePurge(debugOutputQueueName);
        }

        BlockingQueue<Event<QueueConsumer>> conEvents = new LinkedBlockingQueue<>();
        SimpleQueueConsumerImpl queueConsumer = new SimpleQueueConsumerImpl(
            conEvents, conChan, resultHandler, workerServices.getCodec());
        rabbitConsumer = new DefaultRabbitConsumer(conEvents, queueConsumer);
        if(StringUtils.isNotEmpty(queueServices.getWorkerResultsQueue())) {
            consumerTag = conChan.basicConsume(queueServices.getWorkerResultsQueue(), true, rabbitConsumer);
        }
        Thread consumerThread = new Thread(rabbitConsumer);
        consumerThread.start();
        return consumerThread;
    }

    @Override
    public void purgeQueues() throws IOException
    {
        pubChan.queuePurge(queueServices.getWorkerInputQueue());
        if(StringUtils.isNotEmpty(queueServices.getWorkerResultsQueue())) {
            conChan.queuePurge(queueServices.getWorkerResultsQueue());
        }
    }

    @Override
    public void publish(TaskMessage message) throws CodecException, IOException
    {
        byte[] data = workerServices.getCodec().serialise(message);
        pubChan.basicPublish("", queueServices.getWorkerInputQueue(), MessageProperties.TEXT_PLAIN, data);
        if (debugEnabled) {
            debugPubChan.basicPublish("", debugInputQueueName, MessageProperties.TEXT_PLAIN, data);
        }
    }

    @Override
    public void publishDebugOutput(TaskMessage message) throws CodecException, IOException
    {
        byte[] data = workerServices.getCodec().serialise(message);
        debugConChan.basicPublish("", debugOutputQueueName, MessageProperties.TEXT_PLAIN, data);
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }

    @Override
    public String getWorkerInputQueueName()
    {
        return queueServices.getWorkerInputQueue();
    }

    @Override
    public void close() throws IOException
    {
        if (consumerTag != null) {
            try {
                conChan.basicCancel(consumerTag);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (rabbitConsumer != null) {
            rabbitConsumer.shutdown();
        }
        if (conChan != null) {
            try {
                System.out.println("Closing queue connection");
                conChan.close();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
        if (pubChan != null) {
            try {
                pubChan.close();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}