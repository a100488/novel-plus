package com.javanb.novel.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.java2nb.novel.core.utils.HttpUtil;
import com.java2nb.novel.core.utils.RestTemplateUtil;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author songanwei
 * @description todo
 * @date 2022/1/18
 */
public class HttpTest {
    private static final RestTemplate restTemplate = RestTemplateUtil.getInstance("utf-8");
    private static ThreadPoolExecutor executor;
    static {
        executor= new ThreadPoolExecutor(100, 100,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        List<Logger> loggerList = loggerContext.getLoggerList();
        loggerList.forEach(logger -> {
            logger.setLevel(Level.INFO);
        });
    }
    public static void main(String[] args) {
        for(int i=0;i<300;i++) {

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String str = HttpUtil.getByHttpClientWithChrome("https://www.shuquge.com/txt/73779/23006686.html");
                }
            });
        }
        executor.shutdown();
    }
}
