package com.java2nb.novel.core.schedule;

import com.java2nb.novel.core.cache.CacheService;
import com.java2nb.novel.core.utils.B2FileUtil;
import com.java2nb.novel.core.utils.Constants;
import com.java2nb.novel.entity.Book;
import com.java2nb.novel.service.BookService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.java2nb.novel.core.cache.CacheKey.BOOK_B2_TXT_CACHE;

/**
 * 删除缓存10天未访问的文件
 *
 * @author Administrator
 */
@ConditionalOnProperty(prefix = "content.save", name = "storage", havingValue = "b2")
@Service
@RequiredArgsConstructor
@Slf4j
public class CrawlTxtTransSchedule {



    @Value("${content.save.b2path}")
    private String fileSavePath;


    private final CacheService cacheService;

    /**
     * 10分钟转一次
     */
    @Scheduled(fixedRate = 1000 * 60 * 10)
    @SneakyThrows
    public void deleteSchedule() {
        //缓存1W章节小说，其余的删除
        int cacheCount=10000;
        log.info("CrawlTxtTransSchedule。。。。。。。。。。。。");
        File file=new File(fileSavePath);
       if(!file.exists()){
           return;
       }
        List<File> files=new ArrayList<>();
        digui(file,files);
        Map<Long,File> timeFileMap=new TreeMap<>();

       for(File file1:files){
           String fileName=file1.getAbsolutePath().replaceAll(fileSavePath,"");
           log.info("deleteSchedule  "+fileName);
           String time= cacheService.get(BOOK_B2_TXT_CACHE+fileName);
          if(time==null||time.length()<1){
              file1.delete();
          }else {
              timeFileMap.put(Long.parseLong(time), file1);
          }
       }
        if(files.size()>cacheCount){
            int deleteCount=files.size()-cacheCount;
            for(Long key:timeFileMap.keySet()){
                if(deleteCount<=0){
                    break;
                }
                File file1=timeFileMap.get(key);
                String fileName=file1.getAbsolutePath().replaceAll(fileSavePath,"");
                file1.delete();
                cacheService.del(BOOK_B2_TXT_CACHE+fileName);
                deleteCount--;
            }
        }

    }
    public static void digui(File dir,List<File> fileList){
        File[] files=dir.listFiles();

        for(File file1:files){
            if(file1.isDirectory()) {
                digui(file1,fileList);
            }else if(file1.getName().endsWith(".txt")){
                fileList.add(file1);
            }

        }
    }
}
