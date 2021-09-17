package com.github.zhangchunsheng.flink.status;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

public class ReadLineSource implements SourceFunction<String> {
    private String filePath;
    private boolean canceled = false;

    public ReadLineSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        while (!canceled && reader.ready()) {
            String line = reader.readLine();
            sourceContext.collect(line);
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        canceled = true;
    }
}
