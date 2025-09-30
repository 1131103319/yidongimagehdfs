package org.apache.yidong.yidongimagehdfs.model;

import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.stream.Collectors;

@Data
public class ApkStatistics {
    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter dateTimeFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter dateTimeFormatter2 = DateTimeFormatter.ofPattern("yyyyMMdd");

    private String appdownloadurl;   //apk链接
    private String srcip;       //首次下载源IP
    private String srcport;      //首次下载源端口
    private String dstip;       //首次下载目的IP
    private String dstport;      //首次下载目的端口
    private HashSet<String> dstip4list=new HashSet<>();       //当天所有下载该apk的所有ipv4的目的IP
    private HashSet<String> dstip6list=new HashSet<>();       //当天所有下载该apk的所有ipv6的目的IP
    private LocalDateTime firsttime;       //首次下载开始时间
    private LocalDateTime lasttime;         //首次下载结束时间
    private String host = "";             //空
    private String referer = "";          //空
    private Long count;            //该apk当天下载量
    private HashSet<String> iplist=new HashSet<>();         //当天下载该apk的源IP
    private LocalDate partition;


    @Override
    public String toString() {
        return
                appdownloadurl + "|++|" +
                        srcip + "|++|" +
                        srcport + "|++|" +
                        dstip + "|++|" +
                        dstport + "|++|" +
                        dstip4list.stream().limit(300)
                                .collect(Collectors.joining("|")) + "|++|" +
                        dstip6list.stream().limit(150)
                                .collect(Collectors.joining("|")) + "|++|" +
                        firsttime.format(dateTimeFormatter) + "|++|" +
                        lasttime.format(dateTimeFormatter) + "|++|" +
                        host + "|++|" +
                        referer + "|++|" +
                        count + "|++|" +
                        iplist.stream().collect(Collectors.joining("|")) + "|++|" +
                        partition.format(dateTimeFormatter1);
    }
}