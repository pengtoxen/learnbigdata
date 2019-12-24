package com.peng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * habse的crud操作
 */
public class TestHbase {

    //初始化Configuration对象
    private Configuration conf = null;
    //初始化链接
    private Connection conn = null;

    /**
     * 初始化操作,测试之前执行一次
     *
     * @throws Exception
     */
    @Before
    public void init() throws Exception {
        conf = HBaseConfiguration.create();
        //对于hbase的客户端来说，只需要知道hbase所使用的zookeeper集群地址就可以了
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        //获取链接
        conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 建表
     * hbase shell------> create 'tableName','列族1'，'列族2'
     *
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //构造一个表描述器，并指定表名
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("t_user_info".getBytes()));
        //构造一个列族描述器，并指定列族名
        HColumnDescriptor hcd1 = new HColumnDescriptor("base_info");
        //构造第二个列族描述器，并指定列族名
        HColumnDescriptor hcd2 = new HColumnDescriptor("extra_info");
        //为该列族设定一个版本数量,最多保留3个版本
        hcd2.setVersions(1, 3);
        //将列族描述器添加到表描述器中
        tableDescriptor.addFamily(hcd1).addFamily(hcd2);
        //利用表的管理器创建表
        admin.createTable(tableDescriptor);
        //关闭
        admin.close();
        conn.close();
    }

    /**
     * 修改表的属性
     * hbase shell ---> alter 't_user_info','base_info',alter 't1', NAME => 'f1', VERSIONS => 5
     *
     * @throws Exception
     */
    @Test
    public void modifyTable() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //获取表的描述器
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("t_user_info"));
        //修改已有的ColumnFamily---extra_info最小版本数和最大版本数
        HColumnDescriptor hcd1 = tableDescriptor.getFamily("extra_info".getBytes());
        hcd1.setVersions(2, 5);
        //添加新的ColumnFamily
        tableDescriptor.addFamily(new HColumnDescriptor("other_info"));
        //表的管理器admin 修改表
        admin.modifyTable(TableName.valueOf("t_user_info"), tableDescriptor);
        //关闭
        admin.close();
        conn.close();
    }

    /**
     * put添加数据
     * hbase shell ---> put 't_user_info','rk00001','base_info:name','lisi'
     *
     * @throws Exception
     */
    @Test
    public void testPut() throws Exception {
        //构建一个 table对象，通过table对象来添加数据
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        //创建一个集合，用于存放Put对象
        ArrayList<Put> puts = new ArrayList<Put>();

        //构建一个put对象（kv），指定其行键  例如hbase shell:  put '表名','rowkey','列族:列名称','值'
        Put put01 = new Put(Bytes.toBytes("user001"));  //"user001".getBytes()
        //列族base_info下添加字段username,zhangsan
        put01.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));

        Put put02 = new Put("user001".getBytes());
        put02.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));

        Put put03 = new Put("user002".getBytes());
        put03.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("lisi"));
        put03.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put04 = new Put("zhang_sh_01".getBytes());
        put04.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang01"));
        put04.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put05 = new Put("zhang_sh_02".getBytes());
        put05.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang02"));
        put05.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put06 = new Put("liu_sh_01".getBytes());
        put06.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("liu01"));
        put06.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put07 = new Put("zhang_bj_01".getBytes());
        put07.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang03"));
        put07.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put08 = new Put("zhang_bj_01".getBytes());
        put08.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang04"));
        put08.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        //把所有的put对象添加到一个集合中
        puts.add(put01);
        puts.add(put02);
        puts.add(put03);
        puts.add(put04);
        puts.add(put05);
        puts.add(put06);
        puts.add(put07);
        puts.add(put08);

        //一起提交所有的记录
        table.put(puts);

        table.close();
        conn.close();
    }

    /**
     * 读取数据  get：一次读一行
     * hbase shell ----> get 't_user_info',"rowkey"
     *
     * @throws Exception
     */
    @Test
    public void testGet() throws Exception {
        //获取一个table对象
        Table table = conn.getTable(TableName.valueOf("t_user_info"));

        //构造一个get查询参数对象，指定要get的是哪一行
        Get get = new Get("user001".getBytes());
        //返回查询结果数据
        Result result = table.get(get);
        //获取结果中的所有cell
        List<Cell> cells = result.listCells();
        //遍历所有的cell
        for (Cell c : cells) {

            //获取行键
            byte[] rowBytes = CellUtil.cloneRow(c);
            //获取列族
            byte[] familyBytes = CellUtil.cloneFamily(c);
            //获取列族下的列名称
            byte[] qualifierBytes = CellUtil.cloneQualifier(c);
            //列字段的值
            byte[] valueBytes = CellUtil.cloneValue(c);

            System.out.print(new String(rowBytes) + " ");
            System.out.print(new String(familyBytes) + ":");
            System.out.print(new String(qualifierBytes) + " ");
            System.out.println(new String(valueBytes));
        }

        //关闭
        table.close();
        conn.close();
    }

    /**
     * scan 批量查询数据
     * hbase shell ----> scan 't_user_info'
     *
     * @throws Exception
     */
    @Test
    public void testScan() throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        //获取scan对象
        Scan scan = new Scan();
        //获取查询的数据
        ResultScanner scanner = table.getScanner(scan);
        //获取ResultScanner所有数据，返回迭代器
        Iterator<Result> iter = scanner.iterator();
        //遍历迭代器
        while (iter.hasNext()) {
            //获取当前每一行结果数据
            Result result = iter.next();
            //获取当前每一行中所有的cell对象
            List<Cell> cells = result.listCells();
            //迭代所有的cell
            for (Cell c : cells) {
                //获取行键
                byte[] rowBytes = CellUtil.cloneRow(c);
                //获取列族
                byte[] familyBytes = CellUtil.cloneFamily(c);
                //获取列族下的列名称
                byte[] qualifierBytes = CellUtil.cloneQualifier(c);
                //列字段的值
                byte[] valueBytes = CellUtil.cloneValue(c);

                System.out.print(new String(rowBytes) + " ");
                System.out.print(new String(familyBytes) + ":");
                System.out.print(new String(qualifierBytes) + " ");
                System.out.println(new String(valueBytes));
            }
            System.out.println("-----------------------");
        }

        //关闭
        table.close();
        conn.close();
    }


    /**
     * 删除表中的列数据
     * hbase shell ----> delete 't_user_info','user001','base_info:password'
     *
     * @throws Exception
     */
    @Test
    public void testDel() throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        //获取delete对象,需要一个rowkey
        Delete delete = new Delete("user001".getBytes());
        //在delete对象中指定要删除的列族-列名称
        delete.addColumn("base_info".getBytes(), "password".getBytes());
        //执行删除操作
        table.delete(delete);

        //关闭
        table.close();
        conn.close();
    }

    /**
     * 删除表
     * hbase shell ----> disable 't_user_info'     drop 't_user_info'
     *
     * @throws Exception
     */
    @Test
    public void testDrop() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //删除表时先需要disable，将表置为不可用，然后在delete
        admin.disableTable(TableName.valueOf("t_user_info"));
        admin.deleteTable(TableName.valueOf("t_user_info"));
        admin.close();
        conn.close();
    }

    /**
     * 过滤器使用
     * 直接使用效率低,可以用二级索引来加快查询效率
     *
     * @throws Exception
     */
    @Test
    public void testFilter() throws Exception {

        //1.针对行键的前缀过滤器
        //找到前缀是rowkey是liu前缀的数据
        Filter pf = new PrefixFilter(Bytes.toBytes("liu"));
        testScan(pf);

        //2.行过滤器,需要一个比较运算符和比较器
        //找到找到rowkey<user002的数据
        //按照字典序比较,排在user002上面的数据
        RowFilter rf1 = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("user002")));
        testScan(rf1);
        //找到rowkey包含01子串的数据
        RowFilter rf2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("01"));
        testScan(rf2);

        //3.针对指定一个列的value的比较器来过滤
        //找到以zhang开头的列值数据
        ByteArrayComparable comparator1 = new RegexStringComparator("^zhang");
        //找到包含"si"子串的列值数据
        ByteArrayComparable comparator2 = new SubstringComparator("si");
        SingleColumnValueFilter scvf = new SingleColumnValueFilter("base_info".getBytes(), "username".getBytes(), CompareFilter.CompareOp.EQUAL, comparator2);
        testScan(scvf);

        //4.针对列族名的过滤器,返回结果中只会包含满足条件的列族中的数据
        //找到列族 = base_info的数据
        FamilyFilter ff1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("base_info")));
        //找到列族前缀 = base的数据
        FamilyFilter ff2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("base")));
        testScan(ff2);

        //5.针对列名的过滤器,返回结果中只会包含满足条件的列的数据
        //列名=password的数据
        QualifierFilter qf1 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("password")));
        //列名前缀=user的数据
        QualifierFilter qf2 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("user")));
        testScan(qf2);

        //6.多个过滤器同时使用
        //类似于这样的sql select * from t1 where id >10 and age <30
        //第一个过滤器,以base作为前缀的列族
        FamilyFilter cfff1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("base")));
        //第二个过滤器,列名前缀=password
        ColumnPrefixFilter cfff2 = new ColumnPrefixFilter("password".getBytes());
        //指定多个过滤器是否同时满足条件
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(cfff1);
        filterList.addFilter(cfff2);
        testScan(filterList);
    }

    //定义一个方法，接受一个过滤器，返回结果数据
    public void testScan(Filter filter) throws Exception {
        Table table = conn.getTable(TableName.valueOf("t_user_info"));

        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iter = scanner.iterator();
        //遍历所有的Result对象，获取结果
        while (iter.hasNext()) {
            Result result = iter.next();
            List<Cell> cells = result.listCells();
            for (Cell c : cells) {

                //获取行键
                byte[] rowBytes = CellUtil.cloneRow(c);
                //获取列族
                byte[] familyBytes = CellUtil.cloneFamily(c);
                //获取列族下的列名称
                byte[] qualifierBytes = CellUtil.cloneQualifier(c);
                //列字段的值
                byte[] valueBytes = CellUtil.cloneValue(c);

                System.out.print(new String(rowBytes) + " ");
                System.out.print(new String(familyBytes) + ":");
                System.out.print(new String(qualifierBytes) + " ");
                System.out.println(new String(valueBytes));
            }
            System.out.println("-----------------------");
        }
    }
}
