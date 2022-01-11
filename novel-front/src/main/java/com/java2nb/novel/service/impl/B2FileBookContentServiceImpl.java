package com.java2nb.novel.service.impl;

import com.java2nb.novel.core.cache.CacheKey;
import com.java2nb.novel.core.cache.CacheService;
import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.core.utils.FileUtil;
import com.java2nb.novel.core.utils.HttpUtil;
import com.java2nb.novel.core.utils.RestTemplateUtil;
import com.java2nb.novel.entity.BookContent;
import com.java2nb.novel.mapper.BookContentDynamicSqlSupport;
import com.java2nb.novel.mapper.BookContentMapper;
import com.java2nb.novel.service.BookContentService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.http.client.utils.HttpClientUtils;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import static com.java2nb.novel.mapper.BookContentDynamicSqlSupport.bookContent;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.select.SelectDSL.select;

@Service(value = "b2")
@RequiredArgsConstructor
public class B2FileBookContentServiceImpl implements BookContentService {

    @Value("${content.save.b2path}")
    private String fileSavePath;

    private final B2FileUtil b2FileUtil;
    @Value("${content.save.b2_app_bucket_name}")
    private String bucketName;

    private static final RestTemplate restTemplate = RestTemplateUtil.getInstance("utf-8");
    private final BookContentMapper bookContentMapper;

    @SneakyThrows
    @Override
    public BookContent queryBookContent(Long bookId, Long bookIndexId) {
        long time = System.currentTimeMillis();
        String fileSrc = bookId + "/" + bookIndexId + ".txt";
//        File file=new File(fileSavePath +fileSrc);
//        StringBuffer sb = new StringBuffer();
//        if(file.exists()){
//            BufferedReader in = new BufferedReader(new FileReader(fileSavePath +fileSrc));
//            String str;
//            while ((str = in.readLine()) != null) {
//                sb.append(str);
//            }
//            in.close();
//        }
//        SelectStatementProvider selectStatement = select(BookContentDynamicSqlSupport.id, BookContentDynamicSqlSupport.content)
//                .from(bookContent)
//                .where(BookContentDynamicSqlSupport.indexId, isEqualTo(bookIndexId))
//                .limit(1)
//                .build()
//                .render(RenderingStrategies.MYBATIS3);
//        List<BookContent>  bookContents=bookContentMapper.selectMany(selectStatement);
//        if(bookContents.size()>0){
//            System.out.println("耗时"+(System.currentTimeMillis()-time));
//            return bookContents.get(0);
//        }
        BookContent content=null;
        try {
            String  body = HttpUtil.getByHttpClientWithChrome("https://txt.xs6.org/file/" + bucketName + "/" + fileSrc);
            if(body!=null&&body.length()>0) {
                content = new BookContent() {{
                    setIndexId(bookIndexId);
                    setContent(body);
                }};
                System.out.println("耗时" + (System.currentTimeMillis() - time));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (content == null) {
            SelectStatementProvider selectStatement = select(BookContentDynamicSqlSupport.id, BookContentDynamicSqlSupport.content)
                    .from(bookContent)
                    .where(BookContentDynamicSqlSupport.indexId, isEqualTo(bookIndexId))
                    .limit(1)
                    .build()
                    .render(RenderingStrategies.MYBATIS3);
            List<BookContent> bookContents = bookContentMapper.selectMany(selectStatement);
            if (bookContents.size() > 0) {
                System.out.println("耗时" + (System.currentTimeMillis() - time));
                content= bookContents.get(0);
            }
        }
        if (content == null) {
            content = new BookContent() {{
                setIndexId(bookIndexId);
                setContent("正在手打中...");
            }};
        }
        return content;
    }
}
