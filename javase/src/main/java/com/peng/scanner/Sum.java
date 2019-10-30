package com.peng.scanner;

import java.util.Scanner;

class Sum {
    public static void main(String[] args) {
        // 创建对象
        Scanner sc = new Scanner(System.in);
        // 接收数据
        System.out.println("请输入第一个数据：");
        int a = sc.nextInt();
        System.out.println("请输入第二个数据：");
        int b = sc.nextInt();
        // 对数据进行求和
        int sum = a + b;

        System.out.println("sum:" + sum);
    }
}