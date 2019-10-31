package com.peng.base;

/**
 * java传递对象给函数或者方法的时候,是在栈中新生成了
 * 一个变量,这个变量指向对象.换言之,函数外部和内部分别
 * 有一个变量指向同一个对象
 */
public class Test {
    public static void main(String[] args) {
        Person p = new Person("张三");
        System.out.println(p);
        change(p);
    }

    public static void change(Person p) {
        //p指向对象p
        System.out.println(p);
        //新建对象p1
        Person p1 = new Person("李四");
        //赋值给p,p由原来的指向对象p -> 指向对象p1
        p = p1;
        System.out.println(p);
    }
}

class Person {
    String name;

    public Person(String name) {
        this.name = name;
    }
}