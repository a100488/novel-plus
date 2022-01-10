package com.java2nb.novel.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

/**
 * @author Administrator
 */
public class HttpUtil {
    private static Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    private static RestTemplate restTemplate = RestTemplateUtil.getInstance("utf-8");


    public static String getByHttpClient(String url) {
        try {

            ResponseEntity<String> forEntity = restTemplate.getForEntity(url, String.class);
            if (forEntity.getStatusCode() == HttpStatus.OK) {
                return forEntity.getBody();
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String getByHttpClientWithChrome(String url) {
        long time=System.currentTimeMillis();
        try {

            HttpHeaders headers = new HttpHeaders();
            headers.add("user-agent","Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.67 Safari/537.36");
            HttpEntity<String> requestEntity = new HttpEntity<>(null, headers);
            ResponseEntity<String> forEntity = restTemplate.exchange(url.toString(), HttpMethod.GET, requestEntity, String.class);

            if (forEntity.getStatusCode() == HttpStatus.OK) {
                return forEntity.getBody();
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("请求失败:"+url+"耗时"+(System.currentTimeMillis()-time),e);
            return null;
        }
    }
}
