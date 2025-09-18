package org.apache.yidong.yidongimagehdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class YidongimagehdfsApplication {

    public static void main(String[] args) {
        SpringApplication.run(YidongimagehdfsApplication.class, args);
    }

}
