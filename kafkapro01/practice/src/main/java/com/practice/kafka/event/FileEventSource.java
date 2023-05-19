package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable{

    public static  final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);
    public boolean keepRunning = true;
    private long filePointer = 0L;

    private int updateInterval;
    private File file;
    private EventHandler eventHandler;

    public FileEventSource(File file, EventHandler eventHandler, int updateInterval) {
        this.file = file;
        this.eventHandler = eventHandler;
        this.updateInterval = updateInterval;
    }

    @Override
    public void run() {
        try{
            while(this.keepRunning){
                Thread.sleep(this.updateInterval);
                //파일 크기 계산
                long len = this.file.length();
                logger.info("len:{}", len);
                if(len < this.filePointer){
                    logger.info("file was reset as filePointer is longer than file length");
                    this.filePointer = len;
                }else if(len > this.filePointer){
                    readAppendAndSend();
                }else{
                    continue;
                }
            }
        }catch (InterruptedException ie){
            logger.error(ie.getMessage());
        }catch (ExecutionException ie){
            logger.error(ie.getMessage());
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r");
        raf.seek(this.filePointer);
        String line = null;
        while((line = raf.readLine()) != null){
            sendMessage(line);
        }
        //file이 변경되었으므로 file의 FilePointer를 현재 file의 마지막으로 재설정.
        this.filePointer  = raf.getFilePointer();

    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {

        final String delimiter = ",";

        StringTokenizer st = new StringTokenizer(line, delimiter);
        String key = st.nextToken();
        StringBuffer value = new StringBuffer();
        while (true) {
            value.append(st.nextToken());
            if (!st.hasMoreTokens()) {
                break;
            }
            value.append(delimiter);
        }
        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        this.eventHandler.onMessage(messageEvent);
    }
}
