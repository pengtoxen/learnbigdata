package com.peng.namenode_demo;

/**
 * 元数据对象EditLog
 * 代表着一条元数据信息
 *
 * @author Administrator
 */
public class EditLog {

    //每条元数据都有一个事务的ID,保证有序
    long taxId;
    private String log;

    EditLog(long taxId, String log) {
        this.taxId = taxId;
        this.log = log;
    }

    @Override
    public String toString() {
        return "EditLog [taxId=" + taxId + ", log=" + log + "]";
    }
}
