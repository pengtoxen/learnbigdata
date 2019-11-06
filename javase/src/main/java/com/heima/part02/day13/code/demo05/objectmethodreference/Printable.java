package com.heima.part02.day13.code.demo05.objectmethodreference;
/*
    定义一个打印的函数式接口
 */
@FunctionalInterface
public interface Printable {
    //定义字符串的抽象方法
    void print(String s);
}
