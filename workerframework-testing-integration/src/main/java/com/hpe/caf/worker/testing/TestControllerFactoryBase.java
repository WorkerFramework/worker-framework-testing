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

import com.hpe.caf.api.ConfigurationSource;
import com.hpe.caf.worker.core.MessageSystemConfiguration;
import com.hpe.caf.worker.queue.rabbit.RabbitWorkerQueueConfiguration;
import com.hpe.caf.worker.queue.sqs.config.SQSWorkerQueueConfiguration;
import com.hpe.caf.worker.testing.sqs.SQSQueueManager;
import com.hpe.caf.worker.testing.validation.ValuePropertyValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Created by oloughli on 06/06/2016.
 */
public abstract class TestControllerFactoryBase<T>
{

    private static final Logger LOG = LoggerFactory.getLogger(TestControllerFactoryBase.class);
    private static String SQS_IMPLEMENTATION = "sqs";
    public T createDefault(
        String outputQueue,
        TestItemProvider itemProvider,
        WorkerTaskFactory workerTaskFactory,
        ResultProcessor resultProcessor) throws Exception
    {
        WorkerServices workerServices = WorkerServices.getDefault();

        return create(workerServices, outputQueue, itemProvider, workerTaskFactory, resultProcessor);
    }

    public <TConfig> T createDefault(
        Class<TConfig> configClass, Function<TConfig, String> queueNameFunc,
        TestItemProvider itemProvider,
        WorkerTaskFactory workerTaskFactory,
        ResultProcessor resultProcessor) throws Exception
    {
        WorkerServices workerServices = WorkerServices.getDefault();
        ConfigurationSource configurationSource = workerServices.getConfigurationSource();

        TConfig workerConfiguration = configurationSource.getConfiguration(configClass);
        String queueName = queueNameFunc.apply(workerConfiguration);

        return create(workerServices, queueName, itemProvider, workerTaskFactory, resultProcessor);
    }

    private T create(WorkerServices workerServices,
                     String queueName,
                     TestItemProvider itemProvider,
                     WorkerTaskFactory workerTaskFactory,
                     ResultProcessor resultProcessor) throws Exception
    {
        ConfigurationSource configurationSource = workerServices.getConfigurationSource();

        final var queueManager = getQueueManager(configurationSource, workerServices, queueName);

        boolean stopOnError = SettingsProvider.defaultProvider.getBooleanSetting(SettingNames.stopOnError, false);

        T controller = createController(workerServices, itemProvider, queueManager, workerTaskFactory, resultProcessor, stopOnError);

        return controller;
    }

    private QueueManager getQueueManager(
            final ConfigurationSource configurationSource,
            final WorkerServices workerServices,
            final String queueName
    ) throws Exception
    {
        boolean debugEnabled = SettingsProvider.defaultProvider.getBooleanSetting(SettingNames.createDebugMessage, false);
        final String messagingImplementation = SettingsProvider.defaultProvider.getSetting(SettingNames.messagingImplementation);
        if (SQS_IMPLEMENTATION.equals(messagingImplementation)) {
            final SQSWorkerQueueConfiguration queueCfg = configurationSource.getConfiguration(SQSWorkerQueueConfiguration.class);
            queueCfg.getSqsConfiguration().setAwsHost(SettingsProvider.defaultProvider.getSetting(SettingNames.dockerHostAddress));
            queueCfg.getSqsConfiguration().setAwsPort(Integer.parseInt(SettingsProvider.defaultProvider.getSetting(SettingNames.sqsCtrlPort)));
            return new SQSQueueManager(queueCfg, workerServices, queueName, debugEnabled);
        }

        RabbitWorkerQueueConfiguration rabbitConfiguration = configurationSource.getConfiguration(RabbitWorkerQueueConfiguration.class);

        rabbitConfiguration.getRabbitConfiguration().setRabbitHost(SettingsProvider.defaultProvider.getSetting(SettingNames.dockerHostAddress));
        rabbitConfiguration.getRabbitConfiguration().setRabbitPort(Integer.parseInt(SettingsProvider.defaultProvider.getSetting(SettingNames.rabbitmqNodePort)));

        QueueServices queueServices = QueueServicesFactory.create(rabbitConfiguration, queueName, workerServices.getCodec());

        return new RabbitQueueManager(queueServices, workerServices, debugEnabled);
    }

    public abstract T createController(WorkerServices workerServices,
                                       TestItemProvider itemProvider,
                                       QueueManager queueManager,
                                       WorkerTaskFactory workerTaskFactory,
                                       ResultProcessor resultProcessor, boolean stopOnError) throws Exception;
}
