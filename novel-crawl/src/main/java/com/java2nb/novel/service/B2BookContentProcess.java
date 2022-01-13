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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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
    private  B2FileUtil b2FileUtil;
    @Autowired
    private  BookContentMapper bookContentMapper;
    @Autowired
    private BookIndexMapper bookIndexMapper;


    @PostConstruct
    public void consumerBookContent(){


        SelectStatementProvider selectStatement = select(BookIndexDynamicSqlSupport.id, BookIndexDynamicSqlSupport.bookId, BookIndexDynamicSqlSupport.indexNum, BookIndexDynamicSqlSupport.indexName, BookIndexDynamicSqlSupport.updateTime, BookIndexDynamicSqlSupport.isVip)
                .from(bookIndex)
                .where(BookIndexDynamicSqlSupport.storageType, isEqualTo("b2"))
                .orderBy(BookIndexDynamicSqlSupport.createTime)
                .limit(1000)
                .build()
                .render(RenderingStrategies.MYBATIS3);

        List<BookIndex>  list =  bookIndexMapper.selectMany(selectStatement);
        while (list!=null&&list.size()>0){
            for(BookIndex bookIndex: list){
                SelectStatementProvider selectStatement2 = select(BookContentDynamicSqlSupport.id, BookContentDynamicSqlSupport.content)
                        .from(bookContent)
                        .where(BookContentDynamicSqlSupport.indexId, isEqualTo(bookIndex.getId()))
                        .limit(1)
                        .build()
                        .render(RenderingStrategies.MYBATIS3);
                List<BookContent> bookContents=bookContentMapper.selectMany(selectStatement2);
                if(bookContents.size()>0){
                    BookContent bookContent=bookContents.get(0);
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
                            try {
                                bookContentMapper.delete(
                                        deleteFrom(BookContentDynamicSqlSupport.bookContent)
                                                .where(BookContentDynamicSqlSupport.indexId, isEqualTo(bookContent.getIndexId())
                                                ).build().render(RenderingStrategies.MYBATIS3));
                            }catch (Exception e){

                            }
                        }
                    }finally {
                        file.delete();
                    }

                }

            }
        }

    }

}
