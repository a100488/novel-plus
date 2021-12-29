package com.java2nb.novel.service.impl;

import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.core.utils.FileUtil;
import com.java2nb.novel.entity.BookContent;
import com.java2nb.novel.service.BookContentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;


@Slf4j
@Service(value = "b2")
@RequiredArgsConstructor
public class B2FileBookContentServiceImpl implements BookContentService {

    @Value("${content.save.b2path}")
    private String fileSavePath;
    @Value("${content.save.storage}")
    private String storage;

    private final B2FileUtil b2FileUtil;
    @Override
    public void saveBookContent(List<BookContent> bookContentList,Long bookId) {

        bookContentList.forEach(bookContent -> saveBookContent(bookContent,bookId));

    }

    @Override
    public void saveBookContent(BookContent bookContent,Long bookId) {

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
            //本地只存200字  减少缓存  并且也可被百度爬虫抓取
            FileUtil.writeContentToFile(fileSavePath,fileSrc,bookContent.getContent().substring(0,200));
        }
    }

    @Override
    public void updateBookContent(BookContent bookContent,Long bookId) {
        saveBookContent(bookContent,bookId);
    }
}
