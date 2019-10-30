package com.peng.string;

import org.junit.Test;

class Base {
    public static void main(String[] args) {
    }

    /**
     * public String() ：初始化新创建的 String对象，以使其表示空字符序列。
     * public String(char[] value) ：通过当前参数中的字符数组来构造新的String。
     * public String(byte[] bytes) ：通过使用平台的默认字符集解码当前参数中的字节数组来构造新的String。
     */
    @Test
    public void initFunc() {
        // 无参构造
        String str = new String();
        // 通过字符数组构造
        char chars[] = {'a', 'b', 'c'};
        String str2 = new String(chars);
        // 通过字节数组构造
        byte bytes[] = {97, 98, 99};
        String str3 = new String(bytes);
    }

    /**
     * public boolean equals (Object anObject) ：将此字符串与指定对象进行比较。
     * public boolean equalsIgnoreCase (String anotherString) ：将此字符串与指定对象进行比较，忽略大小写。
     */
    @Test
    public void equalFunc() {
        // 创建字符串对象
        String s1 = "hello";
        String s2 = "hello";
        String s3 = "HELLO";
        // boolean equals(Object obj):比较字符串的内容是否相同
        System.out.println(s1.equals(s2)); // true
        System.out.println(s1.equals(s3)); // false
        System.out.println("‐‐‐‐‐‐‐‐‐‐‐");
        //boolean equalsIgnoreCase(String str):比较字符串的内容是否相同,忽略大小写
        System.out.println(s1.equalsIgnoreCase(s2)); // true
        System.out.println(s1.equalsIgnoreCase(s3)); // true
        System.out.println("‐‐‐‐‐‐‐‐‐‐‐");
    }

    /**
     * public int length () ：返回此字符串的长度。
     * public String concat (String str) ：将指定的字符串连接到该字符串的末尾。
     * public char charAt (int index) ：返回指定索引处的 char值。
     * public int indexOf (String str) ：返回指定子字符串第一次出现在该字符串内的索引。
     * public String substring (int beginIndex) ：返回一个子字符串，从beginIndex开始截取字符串到字符
     * 串结尾。
     * public String substring (int beginIndex, int endIndex) ：返回一个子字符串，从beginIndex到
     * endIndex截取字符串。含beginIndex，不含endIndex。
     */
    @Test
    public void commonFunc() {
        //创建字符串对象
        String s = "helloworld";
        // int length():获取字符串的长度，其实也就是字符个数
        System.out.println(s.length());
        System.out.println("‐‐‐‐‐‐‐‐");
        // String concat (String str):将将指定的字符串连接到该字符串的末尾.
        String s2 = s.concat("**hello itheima");
        System.out.println(s2);// helloworld**hello itheima
        // char charAt(int index):获取指定索引处的字符
        System.out.println(s.charAt(0));
        System.out.println(s.charAt(1));
        System.out.println("‐‐‐‐‐‐‐‐");
        // int indexOf(String str):获取str在字符串对象中第一次出现的索引,没有返回‐1
        System.out.println(s.indexOf("l"));
        System.out.println(s.indexOf("owo"));
        System.out.println(s.indexOf("ak"));
        System.out.println("‐‐‐‐‐‐‐‐");
        // String substring(int start):从start开始截取字符串到字符串结尾
        System.out.println(s.substring(0));
        System.out.println(s.substring(5));
        System.out.println("‐‐‐‐‐‐‐‐");
        // String substring(int start,int end):从start到end截取字符串。含start，不含end。
        System.out.println(s.substring(0, s.length()));
        System.out.println(s.substring(3, 8));
    }

    /**
     * public char[] toCharArray () ：将此字符串转换为新的字符数组。
     * public byte[] getBytes () ：使用平台的默认字符集将该 String编码转换为新的字节数组。
     * public String replace (CharSequence target, CharSequence replacement) ：将与target匹配的字符串使
     * 用replacement字符串替换。
     */
    @Test
    public void transferFunc() {
        //创建字符串对象
        String s = "abcde";
        // char[] toCharArray():把字符串转换为字符数组
        char[] chs = s.toCharArray();
        for (int x = 0; x < chs.length; x++) {
            System.out.println(chs[x]);
        }
        System.out.println("‐‐‐‐‐‐‐‐‐‐‐");
        // byte[] getBytes ():把字符串转换为字节数组
        byte[] bytes = s.getBytes();
        for (int x = 0; x < bytes.length; x++) {
            System.out.println(bytes[x]);
        }
        System.out.println("‐‐‐‐‐‐‐‐‐‐‐");
        // 替换字母it为大写IT
        String str = "itcast itheima";
        String replace = str.replace("it", "IT");
        System.out.println(replace); // ITcast ITheima
        System.out.println("‐‐‐‐‐‐‐‐‐‐‐");
    }

    /**
     * public String[] split(String regex) ：将此字符串按照给定的regex（规则）拆分为字符串数组。
     */
    @Test
    public void splitFunc() {
        //创建字符串对象
        String s = "aa|bb|cc";
        String[] strArray = s.split("|"); // ["aa","bb","cc"]
        for (int x = 0; x < strArray.length; x++) {
            System.out.println(strArray[x]); // aa bb cc
        }
    }
}