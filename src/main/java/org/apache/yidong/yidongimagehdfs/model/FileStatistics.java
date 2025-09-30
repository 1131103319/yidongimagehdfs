package org.apache.yidong.yidongimagehdfs.model;

import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.stream.Collectors;

@Data
public class FileStatistics {
    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter dateTimeFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter dateTimeFormatter2 = DateTimeFormatter.ofPattern("yyyyMMdd");

    private String domain; //域名
    private String dnsipversion="";
    private String dnsip="";
    private String srcip; //首次访问该域名的源IP
    private String dstip; //首次访问该域名的目的IP
    private HashSet<String> dstip4list=new HashSet<>(); //格式为IPV4的目的IP拼接串
    private HashSet<String> dstip6list=new HashSet<>(); //格式为IPV6的目的IP拼接串
    private String protocoltype; //访问类型
    private LocalDateTime firsttime; //首次访问该域名的开始时间
    private LocalDateTime lasttime; //首次访问该域名的结束时间
    private Long visitcount; //访问该域名的次数
    private LocalDate partition;


    @Override
    public String toString() {
        return
                 domain + "|++|" +
                 dnsipversion + "|++|" +
                 dnsip + "|++|" +
                 srcip + "|++|" +
                 dstip + "|++|" +
                dstip4list.stream().limit(300)
                        .collect(Collectors.joining("|"))+"|++|" +
                dstip6list .stream().limit(150)
                        .collect(Collectors.joining("|"))+"|++|" +
                 protocoltype + "|++|" +
                 firsttime.format(dateTimeFormatter) + "|++|" +
                 lasttime.format(dateTimeFormatter) + "|++|" +
                 visitcount + "|++|" +
                 partition.format(dateTimeFormatter1);

    }
}