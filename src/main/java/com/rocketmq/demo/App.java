package com.rocketmq.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {


    public static void main(String[] args) {
        System.out.println("进入main--------------------");
        SpringApplication.run(App.class, args);
    }



}
