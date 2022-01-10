package com.java2nb.novel.core.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.java2nb.novel.core.cache.CacheService;
import com.java2nb.novel.core.crawl.ChapterBean;
import com.java2nb.novel.core.crawl.CrawlParser;
import com.java2nb.novel.core.crawl.RuleBean;
import com.java2nb.novel.entity.*;
import com.java2nb.novel.service.BookService;
import com.java2nb.novel.service.CrawlService;
import com.java2nb.novel.utils.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Value;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Administrator
 */
@WebListener
@Slf4j
@RequiredArgsConstructor
public class StarterListener implements ServletContextListener {

    private final BookService bookService;

    private final CrawlService crawlService;

    @Value("${crawl.update.thread}")
    private int updateThreadCount;
    private final CacheService cacheService;

    private static ThreadPoolExecutor executor;
    static {
        executor= new ThreadPoolExecutor(10, 10,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
    }
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        cacheService.del("runPachong");
        for (int i = 0; i < updateThreadCount ; i++) {
            new Thread(() -> {
                log.info("程序启动,开始执行自动更新线程。。。");
                while (true) {
                    try {
                        //1.查询最新目录更新时间在一个月之内的前100条需要更新的数据
                        Date currentDate = new Date();
                        Date startDate = DateUtils.addDays(currentDate, -30);
                        List<Book> bookList;
                        String[] booksStrs=getPachongLock();
                        synchronized (StarterListener.class) {
                            bookList = bookService.queryNeedUpdateBook(booksStrs,startDate, 100);
                        }
                        for (Book needUpdateBook : bookList) {

                            if(!isRun(needUpdateBook.getCrawlSourceId()+"_"+needUpdateBook.getCrawlBookId())) {
                                setPachongLock(needUpdateBook.getCrawlSourceId()+"_"+needUpdateBook.getCrawlBookId());
                            }else{
                                continue;
                            }
                            try {
                                //查询爬虫源规则
                                CrawlSource source = crawlService.queryCrawlSource(needUpdateBook.getCrawlSourceId());
                                RuleBean ruleBean = new ObjectMapper().readValue(source.getCrawlRule(), RuleBean.class);

                                //解析小说基本信息
                                CrawlParser.parseBook(ruleBean, needUpdateBook.getCrawlBookId(),book -> {
                                    //这里只做老书更新
                                    book.setId(needUpdateBook.getId());
                                    book.setWordCount(needUpdateBook.getWordCount());
                                    if (needUpdateBook.getPicUrl() != null && needUpdateBook.getPicUrl().contains(Constants.LOCAL_PIC_PREFIX)) {
                                        //本地图片则不更新
                                        book.setPicUrl(null);
                                    }
                                    //查询已存在的章节
                                    Map<Integer, BookIndex> existBookIndexMap = bookService.queryExistBookIndexMap(needUpdateBook.getId());
                                    //解析章节目录
                                    CrawlParser.parseBookIndexAndContent(needUpdateBook.getCrawlBookId(), book, ruleBean, existBookIndexMap,chapter -> {
                                        bookService.updateBookAndIndexAndContent(book, chapter.getBookIndexList(), chapter.getBookContentList(), existBookIndexMap);
                                    });
                                });
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                            }finally {
                                delPachongLock(needUpdateBook.getCrawlSourceId()+"_"+needUpdateBook.getCrawlBookId());
                            }

                        }
                        //  休眠10分钟
                        TimeUnit.MINUTES.sleep(10);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }

                }
            }).start();


        }
        Set<String> taskAlls=new CopyOnWriteArraySet<>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    try {
                      List<CrawlSingleTask> taskList=  crawlService.getCrawlSingleTask(10);

                      if(taskList!=null&&taskList.size()>0){
                          for(int i=0;i<taskList.size();i++){
                              CrawlSingleTask task=taskList.get(i);
                              if(taskAlls.contains(task.getSourceId()+"_"+task.getSourceBookId())){
                                  continue;
                              }
                              executor.submit(new Runnable() {
                                  @Override
                                  public void run() {
                                      taskAlls.add(task.getSourceId()+"_"+task.getSourceBookId());
                                      paquTask(task);
                                  }
                              });
                          }
                      }

                        TimeUnit.SECONDS.sleep(60);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();


    }
    public void paquTask(CrawlSingleTask task){
        byte crawlStatus = 0;
        try {


            //获取采集任务
            if (task != null) {

                if(!isRun(task.getSourceId()+"_"+task.getSourceBookId())) {
                    setPachongLock(task.getSourceId()+"_"+task.getSourceBookId());
                }else{
                    String sourceBookIdStr= cacheService.get("runPachong");
                    log.info(task.getBookName()+sourceBookIdStr);
                    //TimeUnit.SECONDS.sleep(60);
                    return;
                }
                //查询爬虫规则
                CrawlSource source = crawlService.queryCrawlSource(task.getSourceId());
                RuleBean ruleBean = new ObjectMapper().readValue(source.getCrawlRule(), RuleBean.class);

                try {
                    log.info("爬取"+task.getBookName());
                    if (crawlService.parseBookAndSave(task.getCatId(), ruleBean, task.getSourceId(), task.getSourceBookId())) {
                        //采集成功
                        log.error("caiji sucess"+task.getBookName());
                        crawlStatus = 1;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    delPachongLock(task.getSourceId()+"_"+task.getSourceBookId());
                }

            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        if (task != null) {
            crawlService.updateCrawlSingleTask(task, crawlStatus);
        }
    }

    public synchronized  String[] getPachongLock(){
        String sourceBookIdStr= cacheService.get("runPachong");
        if(sourceBookIdStr==null){
            return new String[]{};
        }
        String[] booksStrs=sourceBookIdStr.split(",");
        return booksStrs;
    }
    public synchronized boolean isRun(String sourceBookId){
        boolean isRun=false;
        String[] booksStrs=getPachongLock();
        for(String bookId:booksStrs){
            if(bookId.equals(sourceBookId)){
                isRun=true;
            }
        }
        return isRun;
    }
    public synchronized void setPachongLock(String sourceBookId){

        StringBuilder values= new StringBuilder();
        String[] booksStrs=getPachongLock();
        for(String bookId:booksStrs){
            if(!bookId.equals(sourceBookId)){
                values.append(bookId).append(",");
            }
        }
        values.append(sourceBookId).append(",");
        cacheService.set("runPachong", values.toString());
    }
    public synchronized void delPachongLock(String sourceBookId){
        StringBuilder values= new StringBuilder();
        String[] booksStrs=getPachongLock();
        for(String bookId:booksStrs){
            if(!bookId.equals(sourceBookId)){
                values.append(bookId).append(",");
            }
        }
        cacheService.set("runPachong", values.toString());
    }
}
