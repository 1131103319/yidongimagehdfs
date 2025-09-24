package org.apache.yidong.yidongimagehdfs.thead;

import lombok.extern.slf4j.Slf4j;
import org.apache.yidong.yidongimagehdfs.model.ApkStatistics;
import org.apache.yidong.yidongimagehdfs.model.FileStatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ApkFileThead implements Callable<String> {
    //处理的文件
    public File file;
    public ConcurrentHashMap<String,ConcurrentHashMap<String, ApkStatistics>> apkStatisticsHashMap;
    public int hashsize;

    public ApkFileThead(File file, ConcurrentHashMap<String,ConcurrentHashMap<String, ApkStatistics>> apkStatisticsHashMap) {
        this.file = file;
        this.apkStatisticsHashMap = apkStatisticsHashMap;
    }

    @Override
    public String call() throws Exception {
        try {
            processFile();
            return file.getName();
        } catch (Exception e) {
            return "fail";
        }
    }

    public void processFile() {
        if (file != null && file.exists()) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    try {
                        processline(line, apkStatisticsHashMap);
                    } catch (Exception e) {
                        log.error("error process line{},file {}", line,file, e);
                    }
                }
            } catch (IOException e) {
                log.error("Error while reading file " + file.getAbsolutePath(), e);
                throw new RuntimeException("Error while reading file " + file.getAbsolutePath(), e);
            }
        }
    }

    public void processline(String line, ConcurrentHashMap<String,ConcurrentHashMap<String, ApkStatistics>> apkStatisticsHashMap) {
        String[] split = line.split("\\|\\+\\+\\|");
        ApkStatistics apkStatistics = new ApkStatistics();
        apkStatistics.setAppdownloadurl(split[0]);
        apkStatistics.setSrcip(split[1]);
        apkStatistics.setSrcport(split[2]);
        apkStatistics.setDstip(split[3]);
        apkStatistics.setDstport(split[4]);
        apkStatistics.setDstip4list(new HashSet<>(Arrays.asList(split[5].split("\\|"))));
        apkStatistics.setDstip6list(new HashSet<>(Arrays.asList(split[6].split("\\|"))));
        apkStatistics.setFirsttime(LocalDateTime.parse(split[7], FileStatistics.dateTimeFormatter));
        apkStatistics.setLasttime(LocalDateTime.parse(split[8], FileStatistics.dateTimeFormatter));
        apkStatistics.setCount(Long.parseLong(split[11]));
        apkStatistics.setIplist(new HashSet<>(Arrays.asList(split[12].split("\\|"))));
        apkStatistics.setPartition(LocalDate.parse(split[13], FileStatistics.dateTimeFormatter1));

        String key = apkStatistics.getPartition().toString();
        String key1 = apkStatistics.getPartition() + "_" + apkStatistics.getAppdownloadurl();
        apkStatisticsHashMap.compute(key,(k,v)->{
            if(v==null){
                ConcurrentHashMap<String, ApkStatistics> objectObjectConcurrentHashMap = new ConcurrentHashMap<>();
                objectObjectConcurrentHashMap.put(key1, apkStatistics);
                return objectObjectConcurrentHashMap;
            }else{
                v.compute(key1,(k1,v1)->{
                    if(v1==null){
                        return apkStatistics;
                    }else{
                        //数量累加
                        v1.setCount(apkStatistics.getCount() + v1.getCount());
                        HashSet<String> iplist = new HashSet<>();
                        iplist.addAll(apkStatistics.getIplist());
                        iplist.addAll(v1.getIplist());
                        v1.setIplist(iplist);
                        //todo 获取时间，判断前后
                        LocalDateTime firsttime0 = v1.getFirsttime();
                        LocalDateTime firsttime1 = apkStatistics.getFirsttime();
                        LocalDateTime lasttime0 = v1.getLasttime();
                        LocalDateTime lasttime1 = apkStatistics.getLasttime();
                        for (String ipv4 : apkStatistics.getDstip4list()) {
                            v1.getDstip4list().add(ipv4);
                        }
                        for (String ipv6 : apkStatistics.getDstip6list()) {
                            v1.getDstip6list().add(ipv6);
                        }
                        //todo 更新在前面的时间ip
                        if (firsttime1.isBefore(firsttime0)) {
                            v1.setFirsttime(apkStatistics.getFirsttime());
                            v1.setSrcip(apkStatistics.getSrcip());
                            v1.setDstip(apkStatistics.getDstip());
                        }
                        //todo 更新在后边的时间ip
                        if (lasttime1.isAfter(lasttime0)) {
                            v1.setLasttime(apkStatistics.getLasttime());
                        }
                        return v1;
                    }
                });
                return v;
            }
        });

    }
}