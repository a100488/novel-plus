package com.java2nb.novel.entity;

import javax.annotation.Generated;

public class BookContent {
    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    private Long id;

    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    private Long indexId;

    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    private String content;

    private Long bookId;


    private Integer errorIndex;

    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    public Long getId() {
        return id;
    }

    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    public void setId(Long id) {
        this.id = id;
    }

    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    public Long getIndexId() {
        return indexId;
    }



    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    public void setIndexId(Long indexId) {
        this.indexId = indexId;
    }

    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    public String getContent() {
        return content;
    }

    @Generated("org.mybatis.generator.api.MyBatisGenerator")
    public void setContent(String content) {
        this.content = content == null ? null : content.trim();
    }
    public Long getBookId() {
        return bookId;
    }

    public void setBookId(Long bookId) {
        this.bookId = bookId;
    }


    public Integer getErrorIndex() {
        return errorIndex;
    }

    public void setErrorIndex(Integer errorIndex) {
        this.errorIndex = errorIndex;
    }
}
