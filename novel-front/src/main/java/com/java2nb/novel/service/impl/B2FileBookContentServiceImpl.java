package com.java2nb.novel.service.impl;

import com.java2nb.novel.core.cache.CacheKey;
import com.java2nb.novel.core.cache.CacheService;
import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.core.utils.FileUtil;
import com.java2nb.novel.entity.BookContent;
import com.java2nb.novel.service.BookContentService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

@Service(value = "b2")
@RequiredArgsConstructor
public class B2FileBookContentServiceImpl implements BookContentService {

    @Value("${content.save.b2path}")
    private String fileSavePath;

    private final B2FileUtil b2FileUtil;

    @SneakyThrows
    @Override
    public BookContent queryBookContent(Long bookId, Long bookIndexId) {
        long time=System.currentTimeMillis();
        String fileSrc= bookId + "/" + bookIndexId + ".txt";
//        File file=new File(fileSavePath +fileSrc);
//        if(!file.exists()){
//            b2FileUtil.downloadByFileName(fileSrc,file);
//        }
        BufferedReader in = new BufferedReader(new FileReader(fileSavePath +fileSrc));
        StringBuffer sb = new StringBuffer();
        String str;
        while ((str = in.readLine()) != null) {
            sb.append(str);
        }
        in.close();
        System.out.println("耗时"+(System.currentTimeMillis()-time));
        return new BookContent() {{
            setContentUrl("https://txt.xs6.org/file/xs6org/"+fileSrc);
            setIndexId(bookIndexId);
            setContent("");
        }};
    }
}
