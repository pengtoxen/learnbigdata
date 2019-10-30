package com.peng.string;

import org.junit.Test;

import java.util.Scanner;

public class Demo {
    public static void main(String[] args) {
    }

    /**
     * 拼接字符串
     */
    @Test
    public void joinString() {
        int[] arr = {1, 2, 3};
        // 创建字符串s
        String s = "[";
        // 遍历数组，并拼接字符串
        for (int x = 0; x < arr.length; x++) {
            if (x == arr.length - 1) {
                s = s.concat(arr[x] + "]");
            } else {
                s = s.concat(arr[x] + "#");
            }
        }
        //输出结果
        System.out.println("s:" + s);
    }

    /**
     * 统计字符个数
     */
    @Test
    public void countString() {
        //键盘录入一个字符串数据
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入一个字符串数据：");
        String s = sc.nextLine();
        //定义三个统计变量，初始化值都是0
        int bigCount = 0;
        int smallCount = 0;
        int numberCount = 0;
        //遍历字符串，得到每一个字符
        for (int x = 0; x < s.length(); x++) {
            char ch = s.charAt(x);
            //拿字符进行判断
            if (ch >= 'A' && ch <= 'Z') {
                bigCount++;
            } else if (ch >= 'a' && ch <= 'z') {
                smallCount++;
            } else if (ch >= '0' && ch <= '9') {
                numberCount++;
            } else {
                System.out.println("该字符" + ch + "非法");
            }
        }
        //输出结果
        System.out.println("大写字符：" + bigCount + "个");
        System.out.println("小写字符：" + smallCount + "个");
        System.out.println("数字字符：" + numberCount + "个");

    }
}