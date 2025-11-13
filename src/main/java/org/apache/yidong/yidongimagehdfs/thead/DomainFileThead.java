package org.apache.yidong.yidongimagehdfs.thead;

import lombok.extern.slf4j.Slf4j;
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
public class DomainFileThead implements Callable<String> {
    //处理的文件
    public File file;
    public ConcurrentHashMap<String, ConcurrentHashMap<String, FileStatistics>> fileStatisticsHashMap;
    public int hashsize;

    public DomainFileThead(File file, ConcurrentHashMap<String, ConcurrentHashMap<String, FileStatistics>> fileStatisticsHashMap, int hashsize) {
        this.file = file;
        this.fileStatisticsHashMap = fileStatisticsHashMap;
        this.hashsize = hashsize;
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
                        processline(line, fileStatisticsHashMap);
                    } catch (Exception e) {
                        log.error("error process line{},filename:{}", line, file, e);
                    }
                }
            } catch (IOException e) {
                log.error("Error while reading file " + file.getAbsolutePath(), e);
                throw new RuntimeException("Error while reading file " + file.getAbsolutePath(), e);
            }
        }
    }

    public void processline(String line, ConcurrentHashMap<String, ConcurrentHashMap<String, FileStatistics>> fileStatisticsHashMap) {
        String[] split = line.split("\\|\\+\\+\\|");
        FileStatistics fileStatistics = new FileStatistics();
        fileStatistics.setDomain(split[0]);
        fileStatistics.setDnsipversion(split[1]);
        fileStatistics.setDnsip(split[2]);
        fileStatistics.setSrcip(split[3]);
        fileStatistics.setDstip(split[4]);
        fileStatistics.setDstip4list(new HashSet<String>(Arrays.asList(split[5].split("\\|"))));
        fileStatistics.setDstip6list(new HashSet<String>(Arrays.asList(split[6].split("\\|"))));
        fileStatistics.setProtocoltype(split[7]);
        fileStatistics.setFirsttime(LocalDateTime.parse(split[8], FileStatistics.dateTimeFormatter));
        fileStatistics.setLasttime(LocalDateTime.parse(split[9], FileStatistics.dateTimeFormatter));
        fileStatistics.setVisitcount(Long.parseLong(split[10]));
        fileStatistics.setPartition(LocalDate.parse(split[11], FileStatistics.dateTimeFormatter1));

        String key = Math.abs(fileStatistics.getDomain().hashCode()) % hashsize + "_" + fileStatistics.getPartition();
        String key1 = fileStatistics.getPartition() + "_" + fileStatistics.getDomain() + "_" + fileStatistics.getProtocoltype();

        fileStatisticsHashMap.compute(key, (k, v) -> {
            if (v == null) {
                ConcurrentHashMap<String, FileStatistics> tmp = new ConcurrentHashMap<String, FileStatistics>();
                tmp.put(key1, fileStatistics);
                return tmp;
            } else {
                v.compute(key1, (k1, v1) -> {
                    if (v1 == null) {
                        return fileStatistics;
                    } else {
                        v1.setVisitcount(fileStatistics.getVisitcount() + v1.getVisitcount());
                        //todo 获取时间，判断前后
                        LocalDateTime firsttime0 = v1.getFirsttime();
                        LocalDateTime firsttime1 = fileStatistics.getFirsttime();
                        LocalDateTime lasttime0 = v1.getLasttime();
                        LocalDateTime lasttime1 = fileStatistics.getLasttime();
                        for (String ipv4 : fileStatistics.getDstip4list()) {
                            v1.getDstip4list().add(ipv4);
                        }
                        for (String ipv6 : fileStatistics.getDstip6list()) {
                            v1.getDstip6list().add(ipv6);
                        }
                        //todo 更新在前面的时间ip
                        if (firsttime1.isBefore(firsttime0)) {
                            v1.setFirsttime(fileStatistics.getFirsttime());
                            v1.setSrcip(fileStatistics.getSrcip());
                            v1.setDstip(fileStatistics.getDstip());
                        }
                        //todo 更新在后边的时间ip
                        if (lasttime1.isAfter(lasttime0)) {
                            v1.setLasttime(fileStatistics.getLasttime());
                        }
                        return v1;
                    }
                });
                return v;
            }
        });
    }
}