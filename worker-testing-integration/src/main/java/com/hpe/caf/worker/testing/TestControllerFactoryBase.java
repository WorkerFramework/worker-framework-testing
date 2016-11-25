package com.hpe.caf.worker.testing;

import com.hpe.caf.api.ConfigurationSource;
import com.hpe.caf.worker.queue.rabbit.RabbitWorkerQueueConfiguration;

import java.util.function.Function;

/**
 * Created by oloughli on 06/06/2016.
 */
public abstract class TestControllerFactoryBase<T> {
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
        RabbitWorkerQueueConfiguration rabbitConfiguration = configurationSource.getConfiguration(RabbitWorkerQueueConfiguration.class);

        rabbitConfiguration.getRabbitConfiguration().setRabbitHost(SettingsProvider.defaultProvider.getSetting(SettingNames.dockerHostAddress));
        rabbitConfiguration.getRabbitConfiguration().setRabbitPort(Integer.parseInt(SettingsProvider.defaultProvider.getSetting(SettingNames.rabbitmqNodePort)));

        QueueServices queueServices = QueueServicesFactory.create(rabbitConfiguration, queueName, workerServices.getCodec());
        boolean debugEnabled = SettingsProvider.defaultProvider.getBooleanSetting(SettingNames.createDebugMessage,false);
        QueueManager queueManager = new QueueManager(queueServices, workerServices,debugEnabled);

        boolean stopOnError = SettingsProvider.defaultProvider.getBooleanSetting(SettingNames.stopOnError, false);

        T controller = createController(workerServices, itemProvider, queueManager, workerTaskFactory, resultProcessor, stopOnError);

        return controller;
    }

    public abstract T createController (WorkerServices workerServices,
                              TestItemProvider itemProvider,
                                        QueueManager queueManager,
                              WorkerTaskFactory workerTaskFactory,
                              ResultProcessor resultProcessor, boolean stopOnError) throws Exception;

}