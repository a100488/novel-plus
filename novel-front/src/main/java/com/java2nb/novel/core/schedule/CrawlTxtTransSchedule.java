package com.java2nb.novel.core.schedule;


import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * 删除缓存文件
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
    @Value("${content.save.b2_cacheSize}")
    private Integer cacheSize;


    /**
     * 10分钟转一次
     */
    @Scheduled(fixedRate = 1000 * 60 * 10)
    @SneakyThrows
    public void deleteSchedule() {
        //缓存1W章节小说，其余的删除
        log.info("CrawlTxtTransSchedule。。。。。。。。。。。。");
        File file=new File(fileSavePath);
       if(!file.exists()){
           return;
       }
        List<File> files=new ArrayList<>();
        digui(file,files);
        Map<Long,File> timeFileMap=new TreeMap<>();

       for(File file1:files){

           Long time=  getFileCreateTime(file1);
           log.info(file1.getAbsolutePath()+"---"+time);
           timeFileMap.put(time, file1);

       }
        if(files.size()>cacheSize){
            int deleteCount=files.size()-cacheSize;
            for(Long key:timeFileMap.keySet()){
                if(deleteCount<=0){
                    break;
                }
                File file1=timeFileMap.get(key);
                file1.delete();
                deleteCount--;
            }
        }

    }
    private Long getFileCreateTime(File file){
        try {
            Path path= Paths.get(file.toURI());
            BasicFileAttributeView basicview= Files.getFileAttributeView(path, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS );
            BasicFileAttributes attr = basicview.readAttributes();
            return attr.creationTime().toMillis();
        } catch (Exception e) {
            e.printStackTrace();
            return file.lastModified();
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
