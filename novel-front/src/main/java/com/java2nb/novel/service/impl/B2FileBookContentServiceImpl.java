package com.java2nb.novel.service.impl;

import com.java2nb.novel.core.cache.CacheKey;
import com.java2nb.novel.core.cache.CacheService;
import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.entity.BookContent;
import com.java2nb.novel.service.BookContentService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;

import static com.java2nb.novel.core.cache.CacheKey.BOOK_B2_TXT_CACHE;

@Service(value = "b2")
@RequiredArgsConstructor
public class B2FileBookContentServiceImpl implements BookContentService {

    @Value("${content.save.b2path}")
    private String fileSavePath;

    B2FileUtil b2FileUtil;

    CacheService cacheService;
    @SneakyThrows
    @Override
    public BookContent queryBookContent(Long bookId, Long bookIndexId) {
        String fileSrc="/" + bookId + "/" + bookIndexId + ".txt";
        File file=new File(fileSavePath +fileSrc);
        if(!file.exists()){
            b2FileUtil.downloadByFileName(fileSrc,file);
            Date date = new Date();
            //设置缓存10天 如果文本超过10万则删除访问时间最早的
            cacheService.set(BOOK_B2_TXT_CACHE+fileSrc,""+date.getTime(),60*60*24*10);
        }
        BufferedReader in = new BufferedReader(new FileReader(fileSavePath + "/" + bookId + "/" + bookIndexId + ".txt"));
        StringBuffer sb = new StringBuffer();
        String str;
        while ((str = in.readLine()) != null) {
            sb.append(str);
        }
        in.close();
        return new BookContent() {{
            setIndexId(bookIndexId);
            setContent(sb.toString());
        }};
    }
}
