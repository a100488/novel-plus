package com.java2nb.novel.core.utils;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.contentHandlers.B2ContentFileWriter;
import com.backblaze.b2.client.contentSources.B2ContentSource;
import com.backblaze.b2.client.contentSources.B2ContentTypes;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.*;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.File;

/**
 * @author songanwei
 * @description B2文件上传
 * @date 2021/12/27
 */

@RequiredArgsConstructor
@Service
public class B2FileUtil {
    private static Logger logger = LoggerFactory.getLogger(B2FileUtil.class);


    @Value("${content.save.b2_app_key_id}")
    private String APP_KEY_ID;
    @Value("${content.save.b2_app_key}")
    private String APP_KEY;
    @Value("${content.save.b2_app_bucket_name}")
    private String bucketName;

    private static final String USER_AGENT = "B2FileUtil";

    /**
     * 桶
     */
    private static B2Bucket b2Bucket;

    private static B2StorageClient client=null;

    private  B2Bucket getB2Bucket(){
        try {
            if (b2Bucket == null) {
                synchronized (B2StorageClient.class) {
                    if (b2Bucket == null) {
                        b2Bucket=getClient().getBucketOrNullByName(bucketName);
                        if(b2Bucket==null){
                            logger.error("B2FileUtil getBucketOrNullByName  getBucket ERROR");
                            throw new RuntimeException("获取不到桶");
                        }

                    }
                }
            }
        } catch (B2Exception e) {
            e.printStackTrace();
        }
        return b2Bucket;

    }
    private  B2StorageClient getClient(){
        if (client == null) {
            synchronized (B2StorageClient.class) {
                if (client == null) {
                    client = B2StorageClientFactory
                            .createDefaultFactory()
                            .create(APP_KEY_ID, APP_KEY, USER_AGENT);
                    this.bucketName=bucketName;

                }
            }
        }
        return client;
    }





    /**
     * 上传文件
     * @param fileUrl 文件Url
     * @return 返回文件id  空上传失败
     */
    public  String uploadSmallFile(String  fileUrl,String fileName){
        final File fileOnDisk = new File(fileUrl);
        return uploadSmallFile(fileOnDisk,fileName);
    }

    /**
     * 上传文件
     * @param fileOnDisk 文件
     * @return 返回文件id 空上传失败
     */
    public  String uploadSmallFile(File fileOnDisk,String fileName){

        final B2UploadListener uploadListener = (progress) -> {
            final double percent = (100. * (progress.getBytesSoFar() / (double) progress.getLength()));
            logger.debug(String.format("  progress(%3.2f, %s)", percent, progress.toString()));
        };


        // upload a file from the disk.
        final B2FileVersion file;

        try {

            final B2ContentSource source = B2FileContentSource.build(fileOnDisk);
            B2UploadFileRequest request = B2UploadFileRequest
                    .builder(getB2Bucket().getBucketId(), fileName, B2ContentTypes.B2_AUTO, source)
                    .setCustomField("color", "blue")
                    .setListener(uploadListener)
                    .build();
            file = getClient().uploadSmallFile(request);

            if(file!=null){
                return file.getFileId();
            }

        } catch (B2Exception e) {
            logger.error("uploadSmallFile error", e);
        }
        return null;


    }

    /**
     * 下载文件
     * @param fileName 下载文件名
     * @param fileUrl 下载文件url
     * @return
     */
    public  File downloadByFileName(String fileName,String fileUrl){
        File downFile=new File(fileUrl);
        return downloadByFileName(fileName,downFile);
    }
    /**
     * 下载文件
     * @param fileName 下载文件名
     * @param downFile 下载到哪个文件
     * @return
     */
    public  File downloadByFileName(String fileName,File downFile){
        final B2DownloadByNameRequest request = B2DownloadByNameRequest
                .builder(bucketName, fileName)
                .build();
        final B2ContentFileWriter handler = B2ContentFileWriter
                .builder(downFile)
                .setVerifySha1ByRereadingFromDestination(true)
                .build();

        try {
            getClient().downloadByName(request, handler);
            logger.info("{} downloadByFileName sucess",fileName);
            return downFile;
        } catch (B2Exception e) {
            logger.error("downloadById error", e);

        }
        return null;
    }
}
