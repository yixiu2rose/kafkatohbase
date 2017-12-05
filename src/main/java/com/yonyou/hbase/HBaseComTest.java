package com.yonyou.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseComTest {
	private static final Logger log = LoggerFactory.getLogger(HBaseComTest.class.getName());

	final static String HBASETABLE = "raw_trad_dtl";
	final static String HBASECOLUMN = "trad";
	
//	final static String HBASETABLE = "raw_auth_dtl";
//	final static String HBASECOLUMN = "auth";

	/**
	 * 插入数据
	 */
	public static String insertDataMe(Connection connection) {
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = null;
		String rowKey = UUID.randomUUID().toString();
		try {
			table = connection.getTable(tableName);
			String family = "cus";

			Put put = new Put("sadsadsd".getBytes());
			put.addColumn(family.getBytes(), "offset".getBytes(), "dsfdfsd".getBytes());
			put.addColumn(family.getBytes(), "value".getBytes(), "sdfdfdf".getBytes());
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}
		return rowKey;
	}
	
	
	public static void main(String[] args) throws IOException {
		log.warn("============hbase test============");
		Connection conn = HBaseComTest.getConnection();
		// insertDataMe(conn);
		scanTableByPrefixKey(conn,"1000005300");
		/*HBaseComTest.createTable(conn);
		String rowKey = HBaseComTest.insertData(conn);
		HBaseComTest.getData(conn, rowKey);*/
		// String rowKey = "a3de7183-561a-40fa-9418-1426def8f78a";
		// HBaseComTest.deleteData(conn, rowKey);
		// HBaseComTest.scanTable(conn);
		// String prefixKey = "1000005817-1584498823-20171017-0469";
		// HBaseComTest.scanTableByPrefixKey(conn, prefixKey);
		// HBaseComTest.scanTableByFuzzy(conn);
		// Map<String, String> colValue = new HashMap<String, String>();
		// colValue.put("source", "网易汽车");
		// colValue.put("add_time", "2016-10-20 12:52:22");
		// HBaseComTest.scanTableByCondition(conn, colValue);
		// HBaseComTest.scanHBaseRegion(conn);
		HBaseComTest.closeConnection(conn);
	}

	/**
	 * 创建链接
	 */
	public static Connection getConnection() throws IOException {
		System.getProperties().setProperty("HADOOP_USER_NAME", "hbase");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "host149,host150,host151");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		Connection conn = ConnectionFactory.createConnection(conf);
		return conn;
	}

	/**
	 * 创建表
	 */
	public static void createTable(Connection connection) throws IOException {
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf(HBASETABLE);
		System.out.println("to create table named " + tableName);
		boolean exists = admin.tableExists(tableName);
		if (exists) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println(tableName + " is exist,detele....");
		}
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor columnDesc = new HColumnDescriptor(HBASECOLUMN);
		tableDesc.addFamily(columnDesc);
		admin.createTable(tableDesc);
		System.out.println("end create table.");
	}

	/**
	 * 删除表
	 */
	public static void deleteTable(Connection connection, String tableName) throws IOException {
		Admin admin = connection.getAdmin();
		TableName hTableName = TableName.valueOf(tableName);
		System.out.println("delete table named " + tableName);
		boolean exists = admin.tableExists(hTableName);
		if (exists) {
			admin.disableTable(hTableName);
			admin.deleteTable(hTableName);
			System.out.println(tableName + " is deleted.");
		}
	}

	/**
	 * 插入数据
	 */
	public static String insertData(Connection connection) {
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = null;
		String rowKey = UUID.randomUUID().toString();
		try {
			table = connection.getTable(tableName);
			String title = "互联网造车改变汽车生态";
			String content_abstract = "互联网生态与汽车产业的创新融合";
			String source = "网易汽车";
			String keywords = "互联网,汽车";
			String add_time = "2016-10-20 12:52:22";

			Put put = new Put(rowKey.getBytes());
			put.addColumn(HBASECOLUMN.getBytes(), "title".getBytes(), title.getBytes("UTF-8"));
			put.addColumn(HBASECOLUMN.getBytes(), "abstract".getBytes(), content_abstract.getBytes("UTF-8"));
			put.addColumn(HBASECOLUMN.getBytes(), "source".getBytes(), source.getBytes("UTF-8"));
			put.addColumn(HBASECOLUMN.getBytes(), "keywords".getBytes(), keywords.getBytes("UTF-8"));
			put.addColumn(HBASECOLUMN.getBytes(), "add_time".getBytes(), add_time.getBytes("UTF-8"));
			table.put(put);
			System.out.println("insert data: " + rowKey);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}
		return rowKey;
	}

	/**
	 * 按照条件检索数据
	 */
	public static void getData(Connection connection, String rowKey) throws IOException {
		System.out.println("Get data from table " + HBASETABLE + " by rowkey.");
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = connection.getTable(tableName);

		long startTime1 = System.currentTimeMillis();
		Scan scan = new Scan();
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(rowKey.getBytes()));
		scan.setFilter(filter1);
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("Scanning table... ");
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Result result = iterator.next();
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.print(qualifier + "=" + value + "\t");
			}
		}
		long endTime1 = System.currentTimeMillis();
		System.out.println("\nscan time: " + (endTime1 - startTime1) / 1000.0);
		scanner.close();

		long startTime2 = System.currentTimeMillis();
		byte[] family = Bytes.toBytes(HBASECOLUMN);
		byte[] row = Bytes.toBytes(rowKey);
		Get get = new Get(row);
		get.addFamily(family);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		if (cells != null) {
			for (Cell cell : cells) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.print(qualifier + "=" + value + "\t");
			}
		}
		long endTime2 = System.currentTimeMillis();
		System.out.println("\nget time: " + (endTime2 - startTime2) / 1000.0);
		closeTable(table);
	}

	/**
	 * 删除
	 */
	public static void deleteData(Connection connection, String rowKey) throws IOException {
		System.out.println("delete data from table " + HBASETABLE + " by rowkey.");
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = connection.getTable(tableName);
		byte[] family = Bytes.toBytes(HBASECOLUMN);
		byte[] row = Bytes.toBytes(rowKey);
		Get get = new Get(row);
		get.addFamily(family);
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		for (Cell cell : cells) {
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			String value = new String(CellUtil.cloneValue(cell), "UTF-8");
			System.out.print(qualifier + "=" + value + "\t");
		}
		System.out.println("\ndelete sucess.");
		Delete delete = new Delete(result.getRow());
		table.delete(delete);
		closeTable(table);
	}

	/**
	 * 检索数据-表扫描
	 */
	public static void scanTable(Connection connection) throws IOException {
		System.out.println("Scan table " + HBASETABLE + " to browse all data.");
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = connection.getTable(tableName);
		Scan scan = new Scan();
		// scan.addFamily( Bytes.toBytes(HBASECOLUMN));
		ResultScanner resultScanner = table.getScanner(scan);
		int count = 0;
		for (Iterator<Result> it = resultScanner.iterator(); it.hasNext();) {
			Result result = it.next();
			List<Cell> cells = result.listCells();
			++count;
			String rowKey = new String(result.getRow());
			System.out.println(rowKey);
			for (Cell cell : cells) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.print(qualifier + "=" + value + "\t");
			}
			System.out.println();
		}
		closeTable(table);
		System.out.println("total row count: " + count);
	}

	/**
	 * 根据rowkey的前缀匹配扫描表
	 */
	public static void scanTableByPrefixKey(Connection connection, String prefixKey) throws IOException {
		System.out.println("Scan table by prefix key " + HBASETABLE + " to browse all data.");
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = connection.getTable(tableName);

		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.setStartRow(Bytes.toBytes(prefixKey.toString()));

		PrefixFilter preFilter = new PrefixFilter(Bytes.toBytes(prefixKey));
		// FirstKeyOnlyFilter过滤条件仅会返回匹配key的第一条kv
		// 对hbase中的表进行count，sum操作等集合操作的时候，使用FirstKeyOnlyFilter会带来性能上的提升
		// FirstKeyOnlyFilter keyFilter = new FirstKeyOnlyFilter();
		// FilterList filters = new FilterList(Operator.MUST_PASS_ALL, preFilter,
		// keyFilter);
		scan.setFilter(preFilter);

		ResultScanner resultScanner = table.getScanner(scan);
		int count = 0;
		for (Iterator<Result> it = resultScanner.iterator(); it.hasNext();) {
			Result result = it.next();
			List<Cell> cells = result.listCells();
			++count;
			String rowKey = new String(result.getRow());
			System.out.println(rowKey);
			for (Cell cell : cells) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.print(qualifier + "=" + value + "\t");
			}
			System.out.println();
		}
		resultScanner.close();
		closeTable(table);
		System.out.println("total row count: " + count);
	}

	/**
	 * rowkey的模糊查询
	 */
	public static void scanTableByFuzzy(Connection connection) throws IOException {
		Date now = Calendar.getInstance().getTime();
		System.out.println(now.toString());
		System.out.println("Scan table by fuzzy key " + HBASETABLE + " to browse all data.");
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = connection.getTable(tableName);

		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		// scan.addColumn(Bytes.toBytes(HBASECOLUMN), Bytes.toBytes("keywords"));

		// FuzzyRowFilter需要把模糊字段值组织成Pair对：
		// pair中第一个参数为模糊查询的string;
		// 第二个参数为byte[]其中装与string位数相同的数值0或1,0表示该位必须与string中值相同，1表示可以不同
		// 定长的RowKey串，其中模糊匹配部分用?或\\x00占位;
		String key = "?????????9189";
		byte[] mask = new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0 };
		Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(Bytes.toBytes(key), mask);
		FuzzyRowFilter fuzzyFilter = new FuzzyRowFilter(Arrays.asList(pair));

		scan.setFilter(fuzzyFilter);
		ResultScanner resultScanner = table.getScanner(scan);
		int count = 0;
		for (Iterator<Result> it = resultScanner.iterator(); it.hasNext();) {
			Result result = it.next();
			List<Cell> cells = result.listCells();
			++count;
			String rowKey = new String(result.getRow());
			System.out.println(rowKey);
			for (Cell cell : cells) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.print(qualifier + "=" + value + "\t");
			}
			System.out.println();
		}
		resultScanner.close();
		closeTable(table);
		System.out.println("total row count: " + count);
	}

	/**
	 * 条件检索数据-表扫描
	 */
	public static void scanTableByCondition(Connection connection, Map<String, String> colValue) throws IOException {
		System.out.println("Scan table by condition " + HBASETABLE + " to browse all data.");
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = connection.getTable(tableName);

		Scan scan = new Scan();
		// scan.addFamily(Bytes.toBytes(HBASECOLUMN));
		FilterList filterList = new FilterList();
		Iterator<Entry<String, String>> entry = colValue.entrySet().iterator();
		// 各个条件之间是"与"关系
		while (entry.hasNext()) {
			Entry<String, String> filter = entry.next();
			String column = filter.getKey();
			String value = filter.getValue();
			filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(HBASECOLUMN), Bytes.toBytes(column),
					CompareOp.EQUAL, Bytes.toBytes(value)));
		}
		// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
		// scan.addColumn(Bytes.toBytes(HBASECOLUMN), Bytes.toBytes(column));
		// filterList != null
		// 要是colValue为空filterList没有add任何filter，scan的结果为空；
		// filterList仅add一个HBase Table中不存在的column时，scan的结果为全表扫描；
		scan.setFilter(filterList);

		ResultScanner resultScanner = table.getScanner(scan);
		int count = 0;
		for (Iterator<Result> it = resultScanner.iterator(); it.hasNext();) {
			Result result = it.next();
			List<Cell> cells = result.listCells();
			++count;
			String rowKey = new String(result.getRow());
			System.out.println(rowKey);
			for (Cell cell : cells) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.print(qualifier + "=" + value + "\t");
			}
			System.out.println();
		}
		resultScanner.close();
		closeTable(table);
		System.out.println("total row count: " + count);
	}

	/**
	 * 按region扫描数据
	 */
	public static void scanHBaseRegion(Connection connection) {
		TableName tableName = TableName.valueOf(HBASETABLE);
		Table table = null;
		try {
			table = connection.getTable(tableName);
			table.getTableDescriptor();
			connection.getRegionLocator(tableName);
			RegionLocator regionLocator = connection.getRegionLocator(tableName);
			List<HRegionLocation> regions = regionLocator.getAllRegionLocations();
			System.out.println("total regions: " + regions.size());
			long startTime = System.nanoTime();
			for (HRegionLocation region : regions) {
				HRegionInfo regionInfo = region.getRegionInfo();
				String start = Bytes.toString(regionInfo.getStartKey());
				String end = Bytes.toString(regionInfo.getEndKey());
				System.out.println("region ==> start key:" + start + " end key:" + end);

				int count = 0;
				Scan scan = new Scan();
				if (regionInfo.getStartKey() != null && regionInfo.getStartKey().length > 0) {
					scan.setStartRow(regionInfo.getStartKey());
				}
				if (regionInfo.getEndKey() != null && regionInfo.getEndKey().length > 0) {
					scan.setStopRow(regionInfo.getEndKey());
				}
				ResultScanner rs = table.getScanner(scan);
				for (Result r : rs) {
					count++;
					String hkey = Bytes.toString(r.getRow());
					String col1 = Bytes.toString(r.getValue(Bytes.toBytes(HBASECOLUMN), Bytes.toBytes("title")));
					String col2 = Bytes.toString(r.getValue(Bytes.toBytes(HBASECOLUMN), Bytes.toBytes("add_time")));
					System.out.println(hkey + "\ttitle=" + col1 + "\tadd_time=" + col2);
				}
				long estimatedTime = System.nanoTime() - startTime;
				System.out.println("Region Row Count=" + count + "; Time=" + estimatedTime / 1000000 + "ms");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeTable(table);
		}
	}

	/**
	 * 关闭HBase Table
	 */
	private static void closeTable(Table hTable) {
		try {
			if (hTable != null) {
				hTable.close();
			}
		} catch (IOException e) {
			log.error("关闭HBase Table异常. {}", e);
		}
	}

	/**
	 * 关闭链接
	 */
	private static void closeConnection(Connection conn) {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (IOException e) {
			log.error("关闭HBase链接异常. {}", e);
		}
	}
}
