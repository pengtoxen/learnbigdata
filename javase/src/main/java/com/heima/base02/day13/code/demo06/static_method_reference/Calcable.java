package com.heima.base02.day13.code.demo06.static_method_reference;

@FunctionalInterface
public interface Calcable {
    //定义一个抽象方法,传递一个整数,对整数进行绝对值计算并返回
    int calsAbs(int number);
}
