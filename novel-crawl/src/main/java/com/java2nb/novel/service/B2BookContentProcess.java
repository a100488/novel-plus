package com.java2nb.novel.service;

import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.core.utils.FileUtil;
import com.java2nb.novel.entity.BookContent;
import com.java2nb.novel.entity.BookIndex;
import com.java2nb.novel.mapper.BookContentDynamicSqlSupport;
import com.java2nb.novel.mapper.BookContentMapper;
import com.java2nb.novel.mapper.BookIndexDynamicSqlSupport;
import com.java2nb.novel.mapper.BookIndexMapper;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.java2nb.novel.mapper.BookContentDynamicSqlSupport.bookContent;
import static com.java2nb.novel.mapper.BookIndexDynamicSqlSupport.bookIndex;
import static org.mybatis.dynamic.sql.SqlBuilder.*;
import static org.mybatis.dynamic.sql.select.SelectDSL.select;

/**
 * @author songanwei
 * @description todo
 * @date 2022/1/7
 */
@Slf4j
@Component
public class B2BookContentProcess {


    @Value("${content.save.b2path}")
    private String fileSavePath;
    @Value("${content.save.storage}")
    private String storage;
    @Autowired
    private B2FileUtil b2FileUtil;
    @Autowired
    private BookContentMapper bookContentMapper;
    @Autowired
    private BookIndexMapper bookIndexMapper;

    private static ThreadPoolExecutor executor;

    static {
        executor = new ThreadPoolExecutor(30, 30,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    @PostConstruct
    public void consumerBookContent() {
        new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {


                    int page = 400;

                    SelectStatementProvider selectStatement2 = select(BookContentDynamicSqlSupport.id, BookContentDynamicSqlSupport.content, BookContentDynamicSqlSupport.indexId)
                            .from(bookContent)
                            .limit(page)
                            .build()
                            .render(RenderingStrategies.MYBATIS3);
                    List<BookContent> bookContents = bookContentMapper.selectMany(selectStatement2);

                    while (bookContents != null && bookContents.size() > 0) {
                        CountDownLatch countDownLatch = new CountDownLatch(bookContents.size());
                        List<Long> notinlist=new ArrayList<>();
                        for (BookContent bookContent : bookContents) {
                            notinlist.add(bookContent.getIndexId());
                            executor.submit(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        SelectStatementProvider selectStatement = select(BookIndexDynamicSqlSupport.id, BookIndexDynamicSqlSupport.bookId, BookIndexDynamicSqlSupport.indexNum, BookIndexDynamicSqlSupport.indexName, BookIndexDynamicSqlSupport.updateTime, BookIndexDynamicSqlSupport.isVip)
                                                .from(bookIndex)
                                                .where(BookIndexDynamicSqlSupport.id, isEqualTo(bookContent.getIndexId()))
                                                .limit(1)
                                                .build()
                                                .render(RenderingStrategies.MYBATIS3);

                                        List<BookIndex> bookIndexList = bookIndexMapper.selectMany(selectStatement);
                                        if (bookIndexList.size() > 0) {
                                            BookIndex bookIndex2 = bookIndexList.get(0);
                                            Long bookId = bookIndex2.getBookId();
                                            //消费逻辑
                                            String fileSrc = bookId + "/" + bookContent.getIndexId() + ".txt";
                                            FileUtil.writeContentToFile(fileSavePath, fileSrc, bookContent.getContent());
                                            File file = new File(fileSavePath + fileSrc);
                                            try {
                                                //上传到oss 减少磁盘空间
                                                if (file.exists()) {
                                                    b2FileUtil.uploadSmallFile(file, fileSrc);
                                                    log.info("上传b2成功" + fileSrc);
                                                    try {
                                                        bookIndex2.setStorageType("b2");
                                                        bookIndexMapper.updateByPrimaryKeySelective(bookIndex2);
                                                       int r= bookContentMapper.delete(
                                                                deleteFrom(BookContentDynamicSqlSupport.bookContent)
                                                                        .where(BookContentDynamicSqlSupport.indexId, isEqualTo(bookContent.getIndexId())
                                                                        ).build().render(RenderingStrategies.MYBATIS3));
                                                        System.out.println(r);

                                                    } catch (Exception e) {

                                                    }
                                                }
                                            } finally {
                                                file.delete();
                                            }

                                        }
                                    } catch (Exception e) {

                                    } finally {
                                        countDownLatch.countDown();
                                    }
                                }
                            });
                        }
                        if(notinlist.size()>0) {
                            selectStatement2 = select(BookContentDynamicSqlSupport.id, BookContentDynamicSqlSupport.content, BookContentDynamicSqlSupport.indexId)
                                    .from(bookContent)
                                    .where(BookContentDynamicSqlSupport.indexId, isNotIn(notinlist))
                                    .limit(page)
                                    .build()
                                    .render(RenderingStrategies.MYBATIS3);
                        }else{
                            selectStatement2 = select(BookContentDynamicSqlSupport.id, BookContentDynamicSqlSupport.content, BookContentDynamicSqlSupport.indexId)
                                    .from(bookContent)
                                    .limit(page)
                                    .build()
                                    .render(RenderingStrategies.MYBATIS3);
                        }
                        List<BookContent> bookContentstmp = bookContentMapper.selectMany(selectStatement2);
                        log.info("预热加载完成"+bookContentstmp.size());
                        try {
                            countDownLatch.await();
                            log.info("休息3秒");
                            TimeUnit.SECONDS.sleep(3);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        bookContents = bookContentstmp;//bookContentMapper.selectMany(selectStatement2);
                    }
                    try {
                        log.info("休息1分钟");
                        TimeUnit.MINUTES.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }}).start();


    }

}
