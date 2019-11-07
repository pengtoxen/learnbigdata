package com.peng.map.hash_map;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class Base {

    @Test
    public void main(String[] args) {
        Student s = new Student("Xiao Ming", 99);
        Map<String, Student> map = new HashMap<>();
        // 将"Xiao Ming"和Student实例映射并关联
        map.put("Xiao Ming", s);
        // 通过key查找并返回映射的Student实例
        Student target = map.get("Xiao Ming");
        // true，同一个实例
        System.out.println(target == s);
        // 99
        System.out.println(target.score);
        // 通过另一个key查找
        Student another = map.get("Bob");
        // 未找到返回null
        System.out.println(another);
    }

    @Test
    public void foreach() {
        Map<String, Integer> map = new HashMap<>();
        map.put("apple", 123);
        map.put("pear", 456);
        map.put("banana", 789);
        for (String key : map.keySet()) {
            Integer value = map.get(key);
            System.out.println(key + " = " + value);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            System.out.println(key + " = " + value);
        }
    }
}

class Student {
    String name;
    int score;

    Student(String name, int score) {
        this.name = name;
        this.score = score;
    }
}