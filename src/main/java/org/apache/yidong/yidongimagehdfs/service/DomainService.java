package org.apache.yidong.yidongimagehdfs.service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yidong.yidongimagehdfs.model.ApkStatistics;
import org.apache.yidong.yidongimagehdfs.model.FileStatistics;
import org.apache.yidong.yidongimagehdfs.thead.ApkFileThead;
import org.apache.yidong.yidongimagehdfs.thead.DomainFileThead;
import org.apache.yidong.yidongimagehdfs.utils.HdfsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.*;
import java.net.InetAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class DomainService {
    private static final Logger log = LoggerFactory.getLogger(DomainService.class);
    @Value("${domainsourcedir}")
    public String domainsourcedir;
    @Value("${dnsdomainsourcedir}")
    public String dnsdomainsourcedir;
    @Value("${domainoutdir}")
    public String domainoutdir;
    @Value("${domainhdfsdir}")
    public String[] domainhdfsdir;

    @Value("${dnsdomainhdfsdir}")
    public String[] dnsdomainhdfsdir;

    @Value("${apksourcedir}")
    public String apksourcedir;
    @Value("${apkoutdir}")
    public String apkoutdir;
    @Value("${apkhdfsdir}")
    public String[] apkhdfsdir;

    @Value("${hashsize}")
    public int hashsize;
    //    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static String ip;
    public static ExecutorService executor = Executors.newFixedThreadPool(10);
    public static ExecutorService executor1 = Executors.newFixedThreadPool(5);
    public static ExecutorService apkexecutor2 = Executors.newFixedThreadPool(5);

    private volatile boolean superflag = false;
    private volatile boolean superflag1 = true;
    private volatile boolean apkflag = true;
    private int count = 0;
    private int apkcount = 0;

    @PostConstruct
    public void init() {
        try {
            ip = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            ip = "localhost";
        }
        log.info("domainsourcedir: " + domainsourcedir);
        log.info("dnsdomainsourcedir: " + dnsdomainsourcedir);
        log.info("domainoutdir: " + domainoutdir);
        log.info("domainhdfsdir: " + Arrays.asList(domainhdfsdir).get(0));
        log.info("dnsdomainhdfsdir: " + Arrays.asList(dnsdomainhdfsdir).get(0));
        log.info("hashsize: " + hashsize);
        log.info("ip: " + ip);
        log.info("apksourcedir: " + apksourcedir);
        log.info("apkoutdir: " + apkoutdir);
        log.info("apkhdfsdir: " + Arrays.asList(apkhdfsdir).get(0));

        new File(domainoutdir).mkdirs();
        new File(apkoutdir).mkdirs();
        File outdir = new File(domainoutdir);
        if (outdir.exists()) {
            File[] outings = outdir.listFiles(f -> f.getName().endsWith(".ing")||f.getName().endsWith(".process"));
            if (outings != null) {
                for (File tmpFile : outings) {
                    tmpFile.delete();
                    log.info("启动清理ing或process文件本批次文件 delete file {}", tmpFile.getName());
                }
            }
            //将处理了的文件重新处理，为了更换新程序临时用途
            File[] outFiles = outdir.listFiles(f-> !f.getName().startsWith("new"));
            if (outFiles != null) {
                for (File tmpFile : outFiles) {
                    tmpFile.renameTo(new File(domainsourcedir +File.separator + tmpFile.getName()));
                    log.info("移动file{} to {}",tmpFile.getName(),domainsourcedir);
                }
            }
        }


        File apkoutfile = new File(apkoutdir);
        File apksourcefile = new File(apksourcedir);
        if(apkoutfile.exists()&&apksourcefile.exists()) {
            File[] files = apkoutfile.listFiles(f -> !f.getName().contains("hdfs") && !f.getName().endsWith(".ing"));
            if (files != null) {
                for (File tmpFile : files) {
                    tmpFile.renameTo(new File(apksourcedir+File.separator+tmpFile.getName()));
                    log.info("移动file{} to {}",tmpFile.getName(),apksourcedir);
                }
            }
        }
    }

    @Scheduled(cron = "${cron}")
    public void run() {
        superflag = true;
        log.info("edits退出超级循环,重置状态");
        while (superflag) {
            //获取文件
            ArrayList<File> files = new ArrayList<>();
            File file = new File(domainsourcedir);
            if (file.exists() && file.isDirectory()) {
                File[] tmpfiles = file.listFiles(f -> !f.getName().endsWith(".process") && !f.getName().contains(".ing"));
                //todo 设置是否多次执行，如果不超过2000就不执行，如果超过就直接多次执行，不再走定时

                if (tmpfiles != null) {
                    count = tmpfiles.length;
                    if (tmpfiles.length < 2000) {
                        superflag = false;
                        log.info("读取到{}个文件，退出超级循环", tmpfiles.length);
                    } else {
                        log.info("读取到{}个文件，开始超级循环", tmpfiles.length);
                    }

                    ArrayList<String> timearray = new ArrayList<>();
                    for (int i = 0; i < 3; i++) {
                        timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter1));
                        timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter2));
                    }
                    for (File tmpfile : tmpfiles) {
                        boolean flag = false;
                        for (String time : timearray) {
                            if (tmpfile.getName().contains(time)) {
                                flag = true;
                            }
                        }
                        if (!flag) {
                            tmpfile.delete();
                            log.info("超时数据删除{}", tmpfile.getName());
                            continue;
                        }
                        if (files.size() < 2000) {
                            //添加文件到队列
                            files.add(tmpfile);
//                        log.info("add file to list{}", tmpfile.getName());
                        }
                    }
                }
            }
            if (files.size() <= 0) {
                log.info("no file found");
                return;
            }
            //处理文件
            //收集数据
            ArrayList<Future<String>> futures = new ArrayList<>();
            //收集正常文件名
            ArrayList<String> formalfilenames = new ArrayList<>();
            ConcurrentHashMap<String, ConcurrentHashMap<String, FileStatistics>> fileStatisticsHashMap = new ConcurrentHashMap<>();
            for (File f : files) {
                Future<String> submit = executor.submit(new DomainFileThead(f, fileStatisticsHashMap, hashsize));
                futures.add(submit);
            }
            for (Future<String> future : futures) {
                try {
                    String result = future.get();
                    if (!result.equals("fail")) {
                        formalfilenames.add(result);
                    }
                } catch (Exception e) {
                    log.error("获取结果异常", e);
                }
            }
            //如果没有成功的，可能有其他问题，不再写入，直接返回
            if (formalfilenames.isEmpty()) {
                log.error("本次执行未知异常,退出");
                return;
            }
            //写出
            long nanotime = System.nanoTime();
            for (String key : fileStatisticsHashMap.keySet()) {
                String filename = "new_" + key + "_" + nanotime + ip + ".ing";
                ConcurrentHashMap<String, FileStatistics> stringFileStatisticsConcurrentHashMap = fileStatisticsHashMap.get(key);
                if (!stringFileStatisticsConcurrentHashMap.isEmpty()) {
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(domainoutdir + File.separator + filename))) {
                        for (FileStatistics fileStatistics : stringFileStatisticsConcurrentHashMap.values()) {
                            writer.write(fileStatistics.toString());
                            writer.newLine();
                        }
                    } catch (Exception e) {
                        log.error("写出异常,全部回退", e);
                        File outdir = new File(domainoutdir);
                        if (outdir.exists()) {
                            File[] outings = outdir.listFiles(f -> f.getName().endsWith(".ing"));
                            if (outings != null) {
                                for (File tmpFile : outings) {
                                    tmpFile.delete();
//                                log.info("清理本批次文件 delete file {}", tmpFile.getName());
                                }
                            }
                        }
                        log.info(LocalDateTime.now().toString() + "本批次异常退出");
                        return;
                    }
                }
            }

            File outdir = new File(domainoutdir);
            if (outdir.exists()) {
                File[] outings = outdir.listFiles(f -> f.getName().endsWith(".ing"));
                if (outings != null) {
                    for (File tmpFile : outings) {
                        tmpFile.renameTo(new File(domainoutdir + File.separator + tmpFile.getName().replace(".ing", "")));
//                    log.info("修正文件 file status {}", tmpFile.getName());
                    }
                }
            }
            //删除source路径文件
            for (String f : formalfilenames) {
                new File(domainsourcedir + File.separator + f).delete();
//                log.info("delete file {}", f);
            }
            log.info(LocalDateTime.now().toString() + "本批次sourcedir处理完成");

            //如果为空，可以正常退出
            if (count <= 0) {
                log.info("目录为空");
                superflag = false;
                break;
            }
        }
    }

    @Scheduled(cron = "${cron1}")
    public void mergeToHdfs() {
        superflag1 = true;
        while (superflag1) {
            if (count < 2000) {
                superflag1 = false;
                log.info("源端退出超级循环,image退出超级循环{}", superflag1);
            } else {
                log.info("images开始超级循环");
            }
            File file = new File(domainoutdir);
            //添加文件
            HashMap<String, ArrayList<File>> stringFileHashMap = new HashMap<>();
            if (file.exists() && file.isDirectory()) {
                File[] tmpfiles = file.listFiles(f -> !f.getName().endsWith(".process") && !f.getName().contains(".ing") && f.getName().startsWith("new"));
                if (tmpfiles != null) {
                    //符合要求的时间数组
                    ArrayList<String> timearray = new ArrayList<>();
                    for (int i = 0; i < 3; i++) {
                        timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter1));
                    }
                    for (File tmpfile : tmpfiles) {
                        boolean flag = false;
                        //处理超时的数据
                        for (String time : timearray) {
                            if (tmpfile.getName().contains(time)) {
                                flag = true;
                            }
                        }
                        if (!flag) {
                            tmpfile.delete();
                            log.info("超时数据处理：delete file {}", tmpfile.getName());
                            continue;
                        }
                        if (stringFileHashMap.size() < 300) {
                            String[] split = tmpfile.getName().split("_");
                            if (split.length < 3) {
                                log.error("domainoutdir异常文件{}", tmpfile.getName());
                            } else {
                                String key = split[0] + "_" + split[1] + "_" + split[2];
                                if (!stringFileHashMap.containsKey(key)) {
                                    ArrayList<File> files = new ArrayList<>();
                                    files.add(tmpfile);
                                    stringFileHashMap.put(key, files);
                                } else {
                                    ArrayList<File> files = stringFileHashMap.get(key);
                                    if (files.size() < 50) {
                                        stringFileHashMap.get(key).add(tmpfile);
                                    }
                                }
                            }
                        }
                    }
                }
            }


            //处理文件
            if (!stringFileHashMap.isEmpty()) {
                log.info("开始处理image文件{}", stringFileHashMap);
                for (ArrayList<File> files : stringFileHashMap.values()) {
                    ConcurrentHashMap<String, ConcurrentHashMap<String, FileStatistics>> fileStatisticsHashMap = new ConcurrentHashMap<>();
                    //收集数据
                    ArrayList<Future<String>> futures = new ArrayList<>();
                    //收集正常文件名
                    ArrayList<String> formalfilenames = new ArrayList<>();
                    //如果这个批次只有一个文件就直接跳过
                    if (files.size() <= 1) continue;
                    for (File f : files) {
                        Future<String> submit = executor1.submit(new DomainFileThead(f, fileStatisticsHashMap, hashsize));
                        futures.add(submit);
                    }

                    for (Future<String> future : futures) {
                        try {
                            String result = future.get();
                            if (!result.equals("fail")) {
                                formalfilenames.add(result);
                            }
                        } catch (Exception e) {
                            log.error("获取结果异常", e);
                        }
                    }
                    //如果没有成功的，可能有其他问题，不再写入，直接返回
                    if (formalfilenames.isEmpty()) {
                        log.error("本次执行未知异常,退出");
                        return;
                    }
                    //写出
                    long nanotime = System.nanoTime();
                    for (String key : fileStatisticsHashMap.keySet()) {
                        String filename = "new_" + key + "_" + nanotime + "_" + ip + ".process";
                        ConcurrentHashMap<String, FileStatistics> stringFileStatisticsConcurrentHashMap = fileStatisticsHashMap.get(key);
                        if (!stringFileStatisticsConcurrentHashMap.isEmpty()) {
                            try (BufferedWriter writer = new BufferedWriter(new FileWriter(domainoutdir + File.separator + filename))) {
                                for (FileStatistics fileStatistics : stringFileStatisticsConcurrentHashMap.values()) {
                                    writer.write(fileStatistics.toString());
                                    writer.newLine();
                                }
                            } catch (Exception e) {
                                log.error("写出异常,全部回退", e);
                                File outdir = new File(domainoutdir);
                                if (outdir.exists()) {
                                    File[] outings = outdir.listFiles(f -> f.getName().contains(".process"));
                                    if (outings != null) {
                                        for (File tmpFile : outings) {
                                            tmpFile.delete();
//                                        log.info("清理本批次文件 delete file {}", tmpFile.getName());
                                        }
                                    }
                                }
                                log.info(LocalDateTime.now().toString() + "本批次异常退出");
                                return;
                            }
                        }
                    }
                    //处理文件状态
                    File outdir = new File(domainoutdir);
                    if (outdir.exists()) {
                        File[] outings = outdir.listFiles(f -> f.getName().endsWith(".process"));
                        if (outings != null) {
                            for (File tmpFile : outings) {
                                tmpFile.renameTo(new File(domainoutdir + File.separator + tmpFile.getName().replace(".process", "")));
//                            log.info("修正文件 file status {}", tmpFile.getName());
                            }
                        }
                    }
                    //删除source路径文件
                    for (String f : formalfilenames) {
                        new File(domainoutdir + File.separator + f).delete();
//                        log.info("delete file {}", f);
                    }
                }
                log.info(LocalDateTime.now().toString() + "本批次outdir处理完成");
            }
        }

        //上传文件
        LocalDateTime now = LocalDateTime.now();
        if (now.getHour() == 2) {
            LocalDate localDate = now.minusDays(1).toLocalDate();
            String format = localDate.format(FileStatistics.dateTimeFormatter1);
            File outdir = new File(domainoutdir);
            if (outdir.exists() && outdir.isDirectory()) {
                File[] files = outdir.listFiles(f -> !f.getName().endsWith(".process") && !f.getName().contains(".ing") && f.getName().contains(format) && f.getName().startsWith("new"));
                if (files != null) {
                    for (File tmpFile : files) {
                        try {
                            String s = tmpFile.getName().split("_")[1];
                            int i = Integer.parseInt(s);
                            HdfsClient.getFileSystem().copyFromLocalFile(true, true, new Path(tmpFile.getAbsolutePath()), new Path(domainhdfsdir[i]));
                            log.info("上传文件{} to {}", tmpFile.getName(), domainhdfsdir[i]);
                        } catch (Exception e) {
                            log.error("上传异常{}", tmpFile.getName(), e);
                        }
                    }
                }
            }
            log.info("本批次上传完成" + LocalDateTime.now().toString());
        }
        log.info("image退出超级循环，重置状态");
    }


    //todo 处理apk文件
    @Scheduled(cron = "${cron2}")
    public void apkMerge() {
        apkflag = true;
        while (apkflag) {
            log.info("apk文件开始处理");
            ArrayList<File> files = new ArrayList<>();
            File file = new File(apksourcedir);
            if (file.exists() && file.isDirectory()) {
                File[] tmpfiles = file.listFiles(f -> !file.getName().contains(".ing") && !file.getName().contains(".process"));
                if (tmpfiles != null) {
                    ArrayList<String> timearray = new ArrayList<>();
                    for(int i=0;i<3;i++){
                        timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter1));
                        timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter2));
                    }
                    apkcount = tmpfiles.length;
                    if (tmpfiles.length < 2000) {
                        apkflag = false;
                        log.info("文件数量{},无需超级循环", tmpfiles.length);
                    }
                    for (File tmpFile : tmpfiles) {
                        boolean flag = false;
                        for (String time : timearray) {
                            if (tmpFile.getName().contains(time)) {
                                flag = true;
                            }
                        }
                        if (!flag) {
                            tmpFile.delete();
                            log.info("超时数据删除{}", tmpFile.getName());
                            continue;
                        }
                        if (files.size() < 2000) {
                            files.add(tmpFile);
//                            log.info("add file {} to arrays", tmpFile.getName());
                        }
                    }
                }
            }
            if (files.size() <= 0) {
                log.info("apk file not found,退出");
                return;
            }

            ArrayList<Future<String>> futures = new ArrayList<>();
            ArrayList<String> formalArrays = new ArrayList<>();
            ConcurrentHashMap<String, ConcurrentHashMap<String, ApkStatistics>> apkStatisticsHashMap = new ConcurrentHashMap<>();
            for (File tmpFile : files) {
                ApkFileThead apkFileThead = new ApkFileThead(tmpFile, apkStatisticsHashMap);
                Future<String> submit = apkexecutor2.submit(apkFileThead);
                futures.add(submit);
            }
            for (Future<String> future : futures) {
                try {
                    String result = future.get();
                    if (!result.equals("fail")) {
                        formalArrays.add(result);
                    }
                } catch (Exception e) {
                    log.error("获取结果异常", e);
                }
            }
            if (formalArrays.size() <= 0) {
                log.error("本次执行未知异常,退出");
                return;
            }
            //开始写出
            for (String key : apkStatisticsHashMap.keySet()) {
                String riqi = key;
                String filename = apksourcedir + File.separator + riqi + "_" + ip + new Random().nextInt() + System.nanoTime() + ".process";
                ConcurrentHashMap<String, ApkStatistics> stringApkStatisticsConcurrentHashMap = apkStatisticsHashMap.get(key);
                if (!stringApkStatisticsConcurrentHashMap.isEmpty()) {
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename));) {
                        for (ApkStatistics apkStatistics : stringApkStatisticsConcurrentHashMap.values()) {
                            writer.write(apkStatistics.toString());
                            writer.newLine();
                        }
                    } catch (IOException e) {
                        log.error("写出异常，全部回退", e);
                        File tmpdir = new File(apksourcedir);
                        if (tmpdir.exists() && tmpdir.isDirectory()) {
                            File[] tmpFiles = tmpdir.listFiles(f -> f.getName().endsWith(".process"));
                            for (File tmpFile : tmpFiles) {
                                tmpFile.delete();
                                log.info("中间文件删除,{}", tmpFile.getName());
                            }
                        }
                        log.info("本次执行异常，退出");
                        return;
                    }
                }
            }
            //处理文件状态
            File tmpdir = new File(apksourcedir);
            if (tmpdir.exists() && tmpdir.isDirectory()) {
                File[] tmpFiles = tmpdir.listFiles(f -> f.getName().endsWith(".process"));
                for (File tmpFile : tmpFiles) {
                    tmpFile.renameTo(new File(apksourcedir + File.separator + tmpFile.getName().replace(".process", "")));
//                    log.info("更改文件状态,{}", tmpFile.getName());
                }
            }
            //删除文件
            for (String formalfile : formalArrays) {
                new File(apksourcedir + File.separator + formalfile).delete();
//                log.info("文件清理{}", formalfile);
            }
            //如果为空，可以正常退出
            if (apkcount <= 0) {
                log.info("目录为空");
                apkflag = false;
                break;
            }
            log.info("apk文件处理完成");
        }
        //上传文件
        LocalDateTime now = LocalDateTime.now();
        if (now.getHour() == 2) {
            runapk();
            log.info("本批次上传完成" + LocalDateTime.now().toString());
        }
    }

    public void runapk() {
        LocalDate localDate = LocalDate.now().minusDays(1);
        String format = localDate.format(ApkStatistics.dateTimeFormatter1);
        ArrayList<File> fileArrayList = getFile(apksourcedir, format);
        if (fileArrayList.isEmpty()) {
            return;
        }
        //写
        String newFile = apkoutdir + File.separator + ip + "hdfs" + format + System.nanoTime() + new Random().nextInt();
        try (
                BufferedWriter newFilewriter = new BufferedWriter(new FileWriter(new File(newFile), true));)
        //读
        {
            for (File file : fileArrayList) {
                try (FileReader fis = new FileReader(file);
                     BufferedReader bufferedReader = new BufferedReader(fis);
                ) {
                    String line = null;
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] split = line.split("\\|\\+\\+\\|");
                        String tmp = "";
                        //获取指定列进行替换
                        if (split.length >= 12) {
                            tmp = String.valueOf(Arrays.stream(split[12].split("\\|")).filter(k -> !k.trim().isEmpty()).distinct().count());
                        } else {
                            log.warn(line + "异常行");
                            continue;
                        }
                        split[12] = tmp;
                        String newLine = String.join("|++|", split);
                        //写入
//                        System.out.println("准备写入"+newLine);
                        newFilewriter.write(newLine);
                        newFilewriter.newLine();
                    }

                } catch (Exception e) {
                    log.error("读取文件异常{}", file.getName(), e);
                }
                //一个文件处理完进行删除
                boolean delete = file.delete();
                if (!delete) {
                    log.warn("删除文件异常{}", file.getName());
                } else log.info("删除文件{}", file.getName());
            }
            //一个批次结束进行上传
            log.info("批次结束，执行上传");
        } catch (Exception e) {
            log.error("本次执行异常，异常原因为", e);
        }
        try {
            FileSystem fileSystem = HdfsClient.getFileSystem();
            if (!fileSystem.exists(new Path(apkhdfsdir[0]))) {
                fileSystem.mkdirs(new Path(apkhdfsdir[0]));
            }
            File file = new File(apkoutdir);
            if (file.exists()&& file.isDirectory()) {
                File[] files = file.listFiles(f -> f.getName().contains("hdfs"));
                if(files!=null) {
                    for (File tmpFile : files) {
                        fileSystem.copyFromLocalFile(true, new Path(tmpFile.getAbsolutePath()), new Path(apkhdfsdir[0]));
                        log.info("文件{}上传到{}", newFile, apkhdfsdir[0]);
                    }
                }
            }
        } catch (Exception e) {
            log.error("上传异常", e);
        }
    }


    public ArrayList<File> getFile(String path, String localDate) {
        ArrayList<File> fileArrayList = new ArrayList<>();
        File file = new File(path);
        if (file.exists() && file.isDirectory()) {
            File[] files = file.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    if (pathname.getName().contains(localDate)&&!pathname.getName().endsWith(".process") && !pathname.getName().contains(".ing")) {
                        return true;
                    }
                    return false;
                }
            });
            if (files != null) {
                for (File file1 : files) {
                    if (fileArrayList.size() <= 2000) {
                        log.info("获取到文件{}", file1.getName());
                        fileArrayList.add(file1);
                    }
                }
            }
        }
        return fileArrayList;
    }
}
