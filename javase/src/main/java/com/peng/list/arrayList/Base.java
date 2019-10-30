package com.peng.list.arrayList;

import java.util.ArrayList;

/**
 * public boolean add(E e) ：将指定的元素添加到此集合的尾部。
 * public E remove(int index) ：移除此集合中指定位置上的元素。返回被删除的元素。
 * public E get(int index) ：返回此集合中指定位置上的元素。返回获取的元素。
 * public int size() ：返回此集合中的元素数。遍历集合时，可以控制索引范围，防止越界。
 */
public class Base {
    public static void main(String[] args) {
        //创建集合对象
        ArrayList<String> list = new ArrayList<String>();
        //添加元素
        list.add("hello");
        list.add("world");
        list.add("java");
        //public E get(int index):返回指定索引处的元素
        System.out.println("get:" + list.get(0));
        System.out.println("get:" + list.get(1));
        System.out.println("get:" + list.get(2));
        //public int size():返回集合中的元素的个数
        System.out.println("size:" + list.size());
        //public E remove(int index):删除指定索引处的元素，返回被删除的元素
        System.out.println("remove:" + list.remove(0));
        //遍历输出
        for (String s : list) {
            System.out.println(s);
        }
    }
}