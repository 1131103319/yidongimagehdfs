package org.apache.yidong.yidongimagehdfs;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashSet;
import java.util.stream.Collectors;

@SpringBootTest
class YidongimagehdfsApplicationTests {

    @Test
    void contextLoads() {
//        LocalDate parse = LocalDate.parse("2025-10-11", FileStatistics.dateTimeFormatter1);
//        System.out.println(parse.toString());
//        System.out.println(File.separator);
//        ArrayList<String> timearray = new ArrayList<>();
//        for(int i=0;i<3;i++){
//            timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter1));
//            timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter2));
//        }
//        System.out.println(timearray);
//        File tmpFile = new File("/tmp/hello1");
//        System.out.println(tmpFile.getAbsolutePath());
        HashSet<String> strings = new HashSet<>();
        strings.add("2409:8C54:2000:604:3:0:0:1A");
        strings.add("2409:8C54:2000:604:3:0:0:1b");
        strings.add("2409:8C54:2000:604:3:0:0:1c");
        strings.add("2409:8C54:2000:604:3:0:0:1d");
        System.out.println(strings.stream().limit(2).collect(Collectors.joining("|")));
    }
}
