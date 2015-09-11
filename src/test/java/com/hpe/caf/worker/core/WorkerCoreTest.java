package com.hpe.caf.worker.core;


import com.hpe.caf.api.Codec;
import com.hpe.caf.api.CodecException;
import com.hpe.caf.api.ConfigurationException;
import com.hpe.caf.api.ConfigurationSource;
import com.hpe.caf.api.HealthResult;
import com.hpe.caf.api.ServicePath;
import com.hpe.caf.api.worker.QueueException;
import com.hpe.caf.api.worker.TaskCallback;
import com.hpe.caf.api.worker.TaskMessage;
import com.hpe.caf.api.worker.TaskStatus;
import com.hpe.caf.api.worker.Worker;
import com.hpe.caf.api.worker.WorkerException;
import com.hpe.caf.api.worker.WorkerFactory;
import com.hpe.caf.api.worker.WorkerQueue;
import com.hpe.caf.api.worker.WorkerQueueMetricsReporter;
import com.hpe.caf.api.worker.WorkerQueueProvider;
import com.hpe.caf.api.worker.WorkerResponse;
import com.hpe.caf.codec.JsonCodec;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.naming.InvalidNameException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class WorkerCoreTest
{
    private static final byte[] SUCCESS_BYTES = "success".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EXCEPTION_BYTES = "exception".getBytes(StandardCharsets.UTF_8);
    private static final String WORKER_NAME = "testWorker";
    private static final int WORKER_API_VER = 1;
    private static final String QUEUE_MSG_ID = "test1";
    private static final String QUEUE_OUT = "outQueue";
    private static final String SERVICE_PATH = "/test/group";


    /** Send a message all the way through WorkerCore and verify the result output message **/
    @Test
    public void testWorkerCore()
        throws CodecException, InterruptedException, WorkerException, ConfigurationException, QueueException, InvalidNameException
    {
        BlockingQueue<byte[]> q = new LinkedBlockingQueue<>();
        Codec codec = new JsonCodec();
        ThreadPoolExecutor tpe = WorkerApplication.getDefaultThreadPoolExecutor(5);
        ConfigurationSource config = Mockito.mock(ConfigurationSource.class);
        ServicePath path = new ServicePath(SERVICE_PATH);
        TestWorkerQueue queue = new TestWorkerQueueProvider(q).getWorkerQueue(config, 50);
        WorkerCore core = new WorkerCore(codec, tpe, queue, getWorkerFactory(), path);
        core.start();
        // at this point, the queue should hand off the task to the app, the app should get a worker from the mocked WorkerFactory,
        // and the Worker itself is a mock wrapped in a WorkerWrapper, which should return success and the appropriate result data
        byte[] stuff = codec.serialise(getTaskMessage(codec, WORKER_NAME));
        queue.submitTask(QUEUE_MSG_ID, stuff);
        // the worker's task result should eventually be passed back to our dummy WorkerQueue and onto our blocking queue
        byte[] result = q.poll(5000, TimeUnit.MILLISECONDS);
        // if the result didn't get back to us, then result will be null
        Assert.assertNotNull(result);
        // deserialise and verify result data
        TaskMessage taskMessage = codec.deserialise(result, TaskMessage.class);
        Assert.assertEquals(TaskStatus.RESULT_SUCCESS, taskMessage.getTaskStatus());
        Assert.assertEquals(WORKER_NAME, taskMessage.getTaskClassifier());
        Assert.assertEquals(WORKER_API_VER, taskMessage.getTaskApiVersion());
        Assert.assertArrayEquals(SUCCESS_BYTES, taskMessage.getTaskData());
        Assert.assertTrue(taskMessage.getContext().containsKey(path.toString()));
        Assert.assertArrayEquals(SUCCESS_BYTES, taskMessage.getContext().get(path.toString()));
    }


    /** Send three tasks into a WorkerCore with only two threads, abort them all, check the running ones are interrupted and the other one never starts **/
    @Test
    public void testAbortTasks()
        throws CodecException, InterruptedException, WorkerException, ConfigurationException, QueueException, InvalidNameException
    {
        BlockingQueue<byte[]> q = new LinkedBlockingQueue<>();
        Codec codec = new JsonCodec();
        ThreadPoolExecutor tpe = WorkerApplication.getDefaultThreadPoolExecutor(2);
        ConfigurationSource config = Mockito.mock(ConfigurationSource.class);
        ServicePath path = new ServicePath(SERVICE_PATH);
        CountDownLatch latch = new CountDownLatch(2);
        TestWorkerQueue queue = new TestWorkerQueueProvider(q).getWorkerQueue(config, 20);
        WorkerCore core = new WorkerCore(codec, tpe, queue, getSlowWorkerFactory(latch), path);
        core.start();
        byte[] task1 = codec.serialise(getTaskMessage(codec, UUID.randomUUID().toString()));
        byte[] task2 = codec.serialise(getTaskMessage(codec, UUID.randomUUID().toString()));
        byte[] task3 = codec.serialise(getTaskMessage(codec, UUID.randomUUID().toString()));
        queue.submitTask("task1", task1);
        queue.submitTask("task2", task2);
        queue.submitTask("task3", task3);   // there are only 2 threads, so this task should not even start
        Thread.sleep(500);  // give the test a little breathing room
        queue.triggerAbort();
        latch.await(1, TimeUnit.SECONDS);
        Thread.sleep(100);
        Assert.assertEquals(3, core.getStats().getTasksReceived());
        Assert.assertEquals(3, core.getStats().getTasksAborted());
        Assert.assertEquals(0, core.getBacklogSize());
    }


    private TaskMessage getTaskMessage(final Codec codec, final String taskId)
        throws CodecException
    {
        TaskMessage tm = new TaskMessage();
        tm.setTaskId(taskId);
        tm.setTaskStatus(TaskStatus.NEW_TASK);
        tm.setTaskClassifier(WORKER_NAME);
        tm.setTaskApiVersion(WORKER_API_VER);
        tm.setTaskData(codec.serialise(new TestWorkerJob()));
        return tm;
    }


    private WorkerFactory getWorkerFactory()
            throws WorkerException
    {
        WorkerFactory factory = Mockito.mock(WorkerFactory.class);
        Worker mockWorker = getWorker();
        Mockito.when(factory.getWorker(Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(mockWorker);
        return factory;
    }


    private WorkerFactory getSlowWorkerFactory(final CountDownLatch latch)
        throws WorkerException
    {
        WorkerFactory factory = Mockito.mock(WorkerFactory.class);
        Worker mockWorker = new SlowWorker(QUEUE_OUT, latch);
        Mockito.when(factory.getWorker(Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(mockWorker);
        return factory;
    }


    private Worker getWorker()
    {
        return new Worker(QUEUE_OUT)
        {
            @Override
            public WorkerResponse doWork()
            {
                return createSuccessResult(SUCCESS_BYTES, SUCCESS_BYTES);
            }


            @Override
            public String getWorkerIdentifier()
            {
                return WORKER_NAME;
            }


            @Override
            public int getWorkerApiVersion()
            {
                return WORKER_API_VER;
            }


            @Override
            protected byte[] getGeneralFailureData()
            {
                return EXCEPTION_BYTES;
            }
        };
    }


    private class TestWorkerQueueProvider implements WorkerQueueProvider
    {
        private final BlockingQueue<byte[]> results;


        public TestWorkerQueueProvider(final BlockingQueue<byte[]> results)
        {
            this.results = results;
        }


        @Override
        public final TestWorkerQueue getWorkerQueue(final ConfigurationSource configurationSource, final int maxTasks)
        {
            return new TestWorkerQueue(maxTasks, this.results);
        }
    }


    private class TestWorkerQueue extends WorkerQueue
    {
        private TaskCallback callback;
        private final BlockingQueue<byte[]> results;



        public TestWorkerQueue(final int maxTasks, final BlockingQueue<byte[]> results)
        {
            super(maxTasks);
            this.results = results;
        }


        @Override
        public void start(final TaskCallback callback)
            throws QueueException
        {
            this.callback = Objects.requireNonNull(callback);
        }


        @Override
        public void publish(String acknowledgeId, byte[] taskMessage, String targetQueue)
            throws QueueException
        {
            results.offer(taskMessage);
        }


        @Override
        public void rejectTask(final String taskId)
        {
        }


        @Override
        public void shutdownIncoming()
        {
        }


        @Override
        public void shutdown()
        {
        }


        @Override
        public WorkerQueueMetricsReporter getMetrics()
        {
            return Mockito.mock(WorkerQueueMetricsReporter.class);
        }


        @Override
        public HealthResult healthCheck()
        {
            return HealthResult.RESULT_HEALTHY;
        }


        public void triggerAbort()
        {
            callback.abortTasks();
        }


        public void submitTask(final String taskId, final byte[] stuff)
            throws WorkerException
        {
            callback.registerNewTask(taskId, stuff);
        }

    }


    private class TestWorkerJob
    {
        private String data = "test123";


        public TestWorkerJob() { }


        public TestWorkerJob(final String input)
        {
            this.data = input;
        }


        public String getData()
        {
            return data;
        }


        public void setData(final String data)
        {
            this.data = data;
        }
    }


    private static class SlowWorker extends Worker
    {
        private final CountDownLatch latch;


        public SlowWorker(final String resultQueue, final CountDownLatch latch)
        {
            super(resultQueue);
            this.latch = Objects.requireNonNull(latch);
        }


        @Override
        public WorkerResponse doWork()
            throws WorkerException, InterruptedException
        {
            try {
                System.out.println("Starting test work");
                Thread.sleep(10000);
                return createSuccessResult(SUCCESS_BYTES, SUCCESS_BYTES);
            } catch (InterruptedException e) {
                System.out.println("Test work interrupted");
                latch.countDown();
                throw e;
            }
        }


        @Override
        public String getWorkerIdentifier()
        {
            return WORKER_NAME;
        }


        @Override
        public int getWorkerApiVersion()
        {
            return WORKER_API_VER;
        }


        @Override
        protected byte[] getGeneralFailureData()
        {
            return EXCEPTION_BYTES;
        }
    }
}
