package com.peng;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * SpringBoot的启动类
 * 我们直接像运行普通 Java 程序一样运行它，由于 Spring Boot 本身就嵌入servlet容器的缘故，我们的 web 项目就运行成功了， 非常方便
 * SpringBootApplication看作是 @Configuration、@EnableAutoConfiguration、@ComponentScan 注解的集合
 * <p>
 * EnableAutoConfiguration：启用 SpringBoot 的自动配置机制
 * ComponentScan： 扫描被@Component (@Service,@Controller)注解的bean，注解默认会扫描该类所在的包下所有的类。
 * Configuration：允许在上下文中注册额外的bean或导入其他配置类
 *
 * @author Administrator
 */
@SpringBootApplication
public class RunApplication {
    public static void main(String[] args) {
        SpringApplication.run(RunApplication.class, args);
    }
}
