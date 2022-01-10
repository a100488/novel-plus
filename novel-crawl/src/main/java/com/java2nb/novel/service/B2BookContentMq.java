package com.java2nb.novel.service;

import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.core.utils.FileUtil;
import com.java2nb.novel.entity.BookContent;
import com.java2nb.novel.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.java2nb.novel.mapper.BookContentDynamicSqlSupport.bookContent;
import static org.mybatis.dynamic.sql.SqlBuilder.*;

/**
 * @author songanwei
 * @description todo
 * @date 2022/1/7
 */
@Slf4j
@Component
public class B2BookContentMq {
    final BlockingQueue<BookContent> blockingQueue = new ArrayBlockingQueue<>(100000);
    public void producerBookContent(BookContent bookContent, Long bookId){
        bookContent.setBookId(bookId);
        blockingQueue.add(bookContent);
    }

    @PostConstruct
    public void consumerBookContent(){
        Consumer c2 = new Consumer(blockingQueue);
        new Thread(c2, "c2").start();
        new Thread(c2).start();
        new Thread(c2).start();
        new Thread(c2).start();
    }

    @Value("${content.save.b2path}")
    private String fileSavePath;
    @Value("${content.save.storage}")
    private String storage;
    @Autowired
    private  B2FileUtil b2FileUtil;

    @Autowired
    private BookIndexMapper bookIndexMapper;
    /**
     * 消费者
     */
    class Consumer implements Runnable {

        private final BlockingQueue<BookContent> queue;

        public Consumer(BlockingQueue<BookContent> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    consume(queue.take());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void consume(BookContent bookContent) {
            try {
                Integer errorIndex=bookContent.getErrorIndex();
                if(errorIndex==null){
                    errorIndex=0;
                }

                if(errorIndex>=3){
                    log.error("数据库不存在,次数过多 一抛弃"+bookContent.getBookId()+"--"+bookContent.getIndexId());
                    return;
                }
               long count= bookIndexMapper.count(countFrom(BookIndexDynamicSqlSupport.bookIndex).where(BookIndexDynamicSqlSupport.id, isEqualTo(bookContent.getIndexId())).build()
                        .render(RenderingStrategies.MYBATIS3));
               if(count<1){
                   log.error("数据库不存在,把它放到队列后面"+bookContent.getBookId()+"--"+bookContent.getIndexId());
                   Thread.sleep(1000);
                   errorIndex++;
                   bookContent.setErrorIndex(errorIndex);
                   blockingQueue.add(bookContent);
                   return;
               }
                Long bookId=bookContent.getBookId();
                //消费逻辑
                String fileSrc=bookId+"/"+bookContent.getIndexId()+".txt";
                FileUtil.writeContentToFile(fileSavePath,fileSrc,bookContent.getContent());
                File file = new File(fileSavePath + fileSrc);
                try {
                    //上传到oss 减少磁盘空间
                    if (file.exists()) {
                        b2FileUtil.uploadSmallFile(file, fileSrc);
                        log.info("上传b2成功"+fileSrc);

                    }
                }finally {
                    file.delete();
                }



            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
