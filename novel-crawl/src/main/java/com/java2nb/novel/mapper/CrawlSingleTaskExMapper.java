package com.java2nb.novel.mapper;

import com.java2nb.novel.entity.CrawlSingleTask;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface CrawlSingleTaskExMapper extends CrawlSingleTaskMapper{
    List<CrawlSingleTask> getCrawlSingleTask(@Param("size") int size);
}
