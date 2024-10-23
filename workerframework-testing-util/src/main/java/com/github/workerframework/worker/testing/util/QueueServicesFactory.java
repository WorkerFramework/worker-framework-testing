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
package com.github.workerframework.worker.testing.util;

import com.github.cafapi.common.api.Codec;
import com.github.workerframework.util.rabbitmq.QueueCreator;
import com.github.workerframework.util.rabbitmq.RabbitUtil;
import com.github.workerframework.worker.api.InvalidTaskException;
import com.github.workerframework.worker.api.TaskCallback;
import com.github.workerframework.worker.api.TaskInformation;
import com.github.workerframework.worker.api.TaskRejectedException;
import com.github.workerframework.worker.configs.RabbitConfiguration;
import com.github.workerframework.worker.queues.rabbit.RabbitWorkerQueueConfiguration;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by ploch on 08/11/2015.
 */
public class QueueServicesFactory
{
    private static class NoOpCallback implements TaskCallback
    {

        @Override
        public void registerNewTask(TaskInformation taskInformation, byte[] bytes, Map<String, Object> headers) throws TaskRejectedException, InvalidTaskException
        {
        }

        @Override
        public void abortTasks()
        {
        }
    }

    public static QueueServices create(final RabbitWorkerQueueConfiguration configuration, final String resultsQueueName, final Codec codec)
            throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException
    {
        Connection connection = createConnection(configuration, new NoOpCallback());
        Channel pubChan = connection.createChannel();
        Channel conChan = connection.createChannel();

        RabbitUtil.declareWorkerQueue(
                pubChan, configuration.getInputQueue(), configuration.getMaxPriority(), QueueCreator.RABBIT_PROP_QUEUE_TYPE_QUORUM
        );
        if(StringUtils.isNotEmpty(resultsQueueName)) {
            RabbitUtil.declareWorkerQueue(
                    conChan, resultsQueueName, configuration.getMaxPriority(), QueueCreator.RABBIT_PROP_QUEUE_TYPE_QUORUM
            );
        }

        return new QueueServices(connection, pubChan, configuration.getInputQueue(), conChan, resultsQueueName, codec, configuration.getMaxPriority());
    }

    private static Connection createConnection(RabbitWorkerQueueConfiguration configuration, final TaskCallback callback)
            throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException
    {
        final RabbitConfiguration rabbitConfiguration = configuration.getRabbitConfiguration();
        return RabbitUtil.createRabbitConnection(rabbitConfiguration);
    }
}
