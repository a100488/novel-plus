package com.java2nb.novel.service.impl;

import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.core.utils.FileUtil;
import com.java2nb.novel.entity.BookContent;
import com.java2nb.novel.mapper.BookContentDynamicSqlSupport;
import com.java2nb.novel.mapper.BookContentMapper;
import com.java2nb.novel.service.B2BookContentMq;
import com.java2nb.novel.service.BookContentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;

import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.update;


@Slf4j
@Service(value = "b2")
@RequiredArgsConstructor
public class B2FileBookContentServiceImpl implements BookContentService {



    @Value("${content.save.b2path}")
    private String fileSavePath;
    @Value("${content.save.storage}")
    private String storage;
    @Autowired
    private  B2FileUtil b2FileUtil;

    private final B2BookContentMq b2BookContentMq;
    @Override
    public void saveBookContent(List<BookContent> bookContentList,Long bookId) {


        bookContentList.forEach(bookContent -> saveBookContent(bookContent,bookId));

    }

    @Override
    public void saveBookContent(BookContent bookContent,Long bookId) {


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

    }

    @Override
    public void updateBookContent(BookContent bookContent,Long bookId) {
        saveBookContent(bookContent,bookId);
    }
}
