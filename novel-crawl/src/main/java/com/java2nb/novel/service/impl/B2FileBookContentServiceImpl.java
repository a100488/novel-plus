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



    private final BookContentMapper bookContentMapper;

    private final B2BookContentMq b2BookContentMq;
    @Override
    public void saveBookContent(List<BookContent> bookContentList,Long bookId) {

        bookContentMapper.insertMultiple(bookContentList);
       // bookContentList.forEach(bookContent -> b2BookContentMq.producerBookContent(bookContent,bookId));

    }

    @Override
    public void saveBookContent(BookContent bookContent,Long bookId) {
        bookContentMapper.insertSelective(bookContent);
       // b2BookContentMq.producerBookContent(bookContent,bookId);

    }

    @Override
    public void updateBookContent(BookContent bookContent,Long bookId) {
        bookContentMapper.update(update(BookContentDynamicSqlSupport.bookContent)
                .set(BookContentDynamicSqlSupport.content)
                .equalTo(bookContent.getContent())
                .where(BookContentDynamicSqlSupport.indexId,isEqualTo(bookContent.getIndexId()))
                .build()
                .render(RenderingStrategies.MYBATIS3));
      //  b2BookContentMq.producerBookContent(bookContent,bookId);
    }
}
