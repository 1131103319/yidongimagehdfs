package org.apache.yidong.yidongimagehdfs;

import org.apache.yidong.yidongimagehdfs.model.FileStatistics;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDate;
import java.util.ArrayList;

@SpringBootTest
class YidongimagehdfsApplicationTests {

    @Test
    void contextLoads() {
//        LocalDate parse = LocalDate.parse("2025-10-11", FileStatistics.dateTimeFormatter1);
//        System.out.println(parse.toString());
//        System.out.println(File.separator);
        ArrayList<String> timearray = new ArrayList<>();
        for(int i=3;i<7;i++){
            timearray.add(LocalDate.now().minusDays(i).format(FileStatistics.dateTimeFormatter1));
        }
        System.out.println(timearray);
    }

}
