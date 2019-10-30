package com.peng.array;

import org.junit.Test;

import java.util.Arrays;

public class Base {
    public static void main(String[] args) {
    }

    /**
     * public static String toString(int[] a) ：返回指定数组内容的字符串表示形式。
     */
    @Test
    public void arrayDefine() {
        int[] ns = {1, 1, 2, 3, 5, 8};
        // 类似 [I@7852e922
        System.out.println(ns);
        System.out.println(Arrays.toString(ns));
    }

    /**
     * public static void sort(int[] a) ：对指定的 int 型数组按数字升序进行排序。
     */
    @Test
    public void arraySort() {
        //自己手动排序
        int[] ns = {28, 12, 89, 73, 65, 18, 96, 50, 8, 36};
        // 排序前:
        System.out.println(Arrays.toString(ns));
        // 排序:
        for (int i = 0; i < ns.length; i++) {
            for (int j = i + 1; j < ns.length; j++) {
                if (ns[i] > ns[j]) {
                    // 交换ns[i]和ns[j]:
                    int tmp = ns[j];
                    ns[j] = ns[i];
                    ns[i] = tmp;
                }
            }
        }
        // 排序后:
        System.out.println(Arrays.toString(ns));

        //使用api排序
        // 定义int 数组
        int[] arr = {24, 7, 5, 48, 4, 46, 35, 11, 6, 2};
        System.out.println("排序前:" + Arrays.toString(arr)); // 排序前:[24, 7, 5, 48, 4, 46, 35, 11, 6, 2]
        // 升序排序
        Arrays.sort(arr);
        System.out.println("排序后:" + Arrays.toString(arr));// 排序后:[2, 4, 5, 6, 7, 11, 24, 35, 46, 48]
    }

    /**
     * 二维数组
     */
    @Test
    public void multiArray() {
        int[][] stds = {
                // 语文, 数学, 英语, 体育
                {68, 79, 95, 81},
                {91, 89, 53, 72},
                {77, 90, 87, 83},
                {92, 98, 89, 85},
                {94, 75, 73, 80}
        };
        System.out.println(stds.length);
        System.out.println(Arrays.toString(stds));
        System.out.println(Arrays.deepToString(stds));
        // TODO: 遍历二维数组，获取每个学生的平均分:
        for (int[] std : stds) {
            int sum = 0;
            for (int sc : std) {
                sum = sum + sc;
            }
            int avg = sum / std.length;
            System.out.println("Average score: " + avg);
        }
        // TODO: 遍历二维数组，获取语文、数学、英语、体育的平均分:
        int[] sums = {0, 0, 0, 0};
        for (int[] std : stds) {
            sums[0] = sums[0] + std[0];
            sums[1] = sums[1] + std[1];
            sums[2] = sums[2] + std[2];
            sums[3] = sums[3] + std[3];
        }
        System.out.println("Average Chinese: " + sums[0] / stds.length);
        System.out.println("Average Math: " + sums[1] / stds.length);
        System.out.println("Average English: " + sums[2] / stds.length);
        System.out.println("Average Physical: " + sums[3] / stds.length);
    }
}