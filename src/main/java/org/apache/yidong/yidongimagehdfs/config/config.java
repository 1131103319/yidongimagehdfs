package org.apache.yidong.yidongimagehdfs.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
public class config  implements SchedulingConfigurer {
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        // 设置用于执行定时任务的线程池
        taskRegistrar.setTaskScheduler(taskScheduler());
    }

    @Bean(destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        // 设置线程池大小
        scheduler.setPoolSize(2);
        // 设置线程名前缀，方便日志排查
        scheduler.setThreadNamePrefix("my-scheduler-");
        // 设置线程池关闭前的等待时间，确保所有任务都能完成
        scheduler.setAwaitTerminationSeconds(60);
        // 设置当任务被关闭时的处理策略（等待所有任务执行完）
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        // 初始化
        scheduler.initialize();
        return scheduler;
    }
}
