# worker-framework-testing

The **Worker Framework Testing** is a set of tools that facilitate the implementation of testing for modules created on the basis of 
the [Worker framework](https://workerframework.github.io/worker-framework/)

The framework connects to a RabbitMQ messaging system by default, but has the ability to connect to a containerized SQS 
implementation [localstack](https://hub.docker.com/r/localstack/localstack) by adding configuration in the target 
project pom.

1. Add the localstack image to the projects docker-versions-maven-plugin.
```
<image>
    <repository>${dockerHubPublic}/localstack/localstack</repository>
    <tag>latest</tag>
</image>
```

2. Add the localstack image to the projects docker-maven-plugin.
```
<image>
    <alias>sqs</alias>
    <name>${projectDockerRegistry}/localstack/localstack</name>
    <run>
        <ports>
            <port>${sqs.ctrl.port}:4566</port>
        </ports>
        <env>
            <!--https://docs.localstack.cloud/references/logging/-->
            <LS_LOG>warn</LS_LOG>
        </env>
        <wait>
            <log>Ready.</log>
            <time>120000</time>
            <shutdown>500</shutdown>
        </wait>
        <log>
            <enabled>true</enabled>
        </log>
    </run>
</image>
```

3. Add system property variables for the failsafe test execution
```
sqs.ctrl.port = the externally mapped port for the running container.
messaging.implementation = sqs
```

4. Add test-configuration to confgure SQS for the test framework.
See [here](https://github.com/CAFDataProcessing/worker-languagedetection/tree/develop/worker-languagedetection-container/test-configs) 
for example test framework cfg.  

   
5. See [here](https://github.com/CAFDataProcessing/worker-languagedetection/blob/develop/worker-languagedetection-container/pom.xml) 
for example failsafe executions to run both rabbit and sqs tests and have failures reported in jenkins.
