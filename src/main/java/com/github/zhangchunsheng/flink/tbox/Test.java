package com.github.zhangchunsheng.flink.tbox;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) throws Exception {
        System.out.println("Starting the JobListener Example Code");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String outPut = File.createTempFile("test", "txt").getParent();
        System.out.println("Output will be store at folder " + outPut);

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        env.fromCollection(integers)
                .map(x -> x * 2) // multiply numbers by 2
                .writeAsText(outPut + "/flink-job-listener-" + UUID.randomUUID());

        // env.fromElements(1, 2, 3, 4, 5).output(new DiscardingOutputFormat<>());

        System.out.println("Registering the JobListener");
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
                if (throwable != null) {
                    logger.error("Job failed to submit", throwable);
                    return;
                }
                System.out.println("Job submitted successfully");
                // do something
                // push notification
                // or Call an API
                // or Insert something in DB
            }

            @Override
            public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
                if (throwable != null) {
                    logger.error("Job failed to finish ", throwable);
                    return;
                }
                System.out.println("Job completed successfully");
                // do something
                // push notification
                // or Call an API
                // or Insert something in DB
            }
        });

        env.execute();
    }
}
