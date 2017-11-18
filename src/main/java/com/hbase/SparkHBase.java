package com.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkHBase {

	private static final Logger LOG = LoggerFactory.getLogger(SparkHBase.class);

	public static void createTable(Configuration conf, String tablename, String[] columnFamily)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		// HBaseAdmin admin=new HBaseAdmin(conf);

		HBaseAdmin admin = (HBaseAdmin) ConnectionFactory.createConnection(conf).getAdmin();
		if (admin.tableExists(tablename)) {
			LOG.info(tablename + " Table exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
			for (int i = 0; i < columnFamily.length; i++) {
				HColumnDescriptor columnDesc = new HColumnDescriptor(columnFamily[i]);
				tableDesc.addFamily(columnDesc);
			}
			admin.createTable(tableDesc);
			LOG.info(tablename + " create table success!");
		}
		admin.close();
	}

	@SuppressWarnings({ "resource", "deprecation" })
	public void dropTable(Configuration conf, String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void addRow(Table table, String rowKey, String columnFamily, String key, String value)
			throws IOException {
		Put rowPut = new Put(Bytes.toBytes(rowKey));
		rowPut.addColumn(columnFamily.getBytes(), key.getBytes(), value.getBytes());
		table.put(rowPut);
		// System.out.println("put '"+rowKey+"', '"+columnFamily+":"+key+"',
		// '"+value+"'");
	}

	@SuppressWarnings("deprecation")
	public static void putRow(HTable table, String rowKey, String columnFamily, String key, String value)
			throws IOException {
		Put rowPut = new Put(Bytes.toBytes(rowKey));
		rowPut.add(columnFamily.getBytes(), key.getBytes(), value.getBytes());
		table.put(rowPut);
		// System.out.println("put '"+rowKey+"', '"+columnFamily+":"+key+"',
		// '"+value+"'");
	}

	/**
	 * 閹靛綊鍣哄ǎ璇插閺佺増锟�?
	 * 
	 * @param list
	 * @throws IOException
	 */

	public void addDataBatch(HTable table, List<Put> list) {
		try {
			table.put(list);
		} catch (RetriesExhaustedWithDetailsException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * 閺屻儴顕楅崗銊╁劥
	 */
	@SuppressWarnings("deprecation")
	public void queryAll(HTable table) {
		Scan scan = new Scan();
		try {
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				int i = 0;
				for (KeyValue rowKV : result.list()) {
					if (i++ == 0) {
						System.out.print("rowkey:" + new String(rowKV.getRow()) + " ");
					}
				}

				System.out.println();
			}
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * 閹稿鐓囷拷?锟芥顔岄弻銉嚄 column = value 閻ㄥ嫭鏆熼幑锟�?
	 * 
	 * @param queryColumn
	 *            鐟曚焦鐓＄拠銏㈡畱閸掓锟�?
	 * @param value
	 *            鏉╁洦鎶ら弶鈥叉閸婏拷
	 * @param columns
	 *            鏉╂柨娲栭惃鍕灙閸氬秹娉﹂崥锟�?
	 */
	public ResultScanner queryBySingleColumn(HTable table, String queryColumn, String value, String[] columns) {
		if (columns == null || queryColumn == null || value == null) {
			return null;
		}

		try {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(queryColumn),
					Bytes.toBytes(queryColumn), CompareOp.EQUAL, new SubstringComparator(value));
			Scan scan = new Scan();

			for (String columnName : columns) {
				scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(columnName));
			}

			scan.setFilter(filter);
			return table.getScanner(scan);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

		return null;
	}

	public static Result getRow(Table table, String rowKey) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		// System.out.println("Get: "+result);
		return result;
	}

	/**
	 * 閸︺劍瀵氾拷?锟芥氨娈戦弶鈥叉娑撳绱濋幐澶嬬厙娑擄拷鐡у▓浣冧粵閸氾拷
	 * 
	 * @param paramMap
	 *            閸欏倹鏆熼弶鈥叉
	 * @param dimensionColumns
	 *            缂佹潙锟�?
	 * @param aggregateColumn
	 *            閼辨艾鎮庯拷?锟芥锟�?
	 * @return 鏉╂柨娲杕ap閿涘ey 娑撶imensionColumns 缂佹潙瀹抽惄绋款嚠鎼存梻娈戦弫鐗堝祦閿涘瘉alue
	 *         娑撶ggregateColumn 鐎涙顔岋拷?锟界懓绨查惃鍕拷
	 */

	@SuppressWarnings("null")
	public Map<String, Long> aggregateBySingleColumn(Map<String, String> paramMap, String[] dimensionColumns,
			String aggregateColumn) {
		if (dimensionColumns == null || dimensionColumns.length == 0 || paramMap == null || aggregateColumn == null
				|| aggregateColumn.equals("")) {
			return null;
		}

		HTable table = null;
		Map<String, Long> map = null;
		try {
			FilterList filterList = new FilterList();
			Scan scan = new Scan();
			// 婵烇綀顕ф慨鐐存交閸ャ劍濮㈤柡澶嗭拷濞嗭拷
			for (String paramKey : paramMap.keySet()) {
				SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(paramKey),
						Bytes.toBytes(paramKey), CompareOp.EQUAL, new SubstringComparator(paramMap.get(paramKey)));
				filterList.addFilter(filter);
			}
			scan.setFilter(filterList);

			// 閻熸洑绀侀惈宥夋偝閹殿喗鐣遍柛鎺炴嫹
			for (String column : dimensionColumns) {
				scan.addColumn(Bytes.toBytes(column), Bytes.toBytes(column));
			}
			scan.addColumn(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn));

			ResultScanner results = table.getScanner(scan);

			// 閻忓繐妫欓悡锛勬嫚閵忋垻娉㈤柡瀣矋閺備線宕楅¨鐮 濞戞搫锟�?
			map = new ConcurrentHashMap<String, Long>();
			for (Result result : results) {
				// String dimensionKey = "";
				StringBuilder dimensionKey = new StringBuilder();
				// 闁告瑦鐗曢敓锟�?
				String value = new String(
						result.getValue(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn)));
				Long aggregateValue = value == null ? 0 : Long.parseLong(value);

				// 闁瑰嘲鍚嬬敮纰杄y
				for (String column : dimensionColumns) {
					dimensionKey
							.append("\t" + new String(result.getValue(Bytes.toBytes(column), Bytes.toBytes(column))));
				}
				dimensionKey = dimensionKey.deleteCharAt(0);

				if (map.containsKey(dimensionKey)) {
					map.put(dimensionKey.toString(), map.get(dimensionKey.toString()) + aggregateValue);
				} else {
					map.put(dimensionKey.toString(), aggregateValue);
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return map;
	}

	// public static void main(String[] args) throws Exception {
	// Configuration conf= Conn.getHbaseConf();
	// HTable table =new HTable(conf, "test");
	//
	// try {
	//
	// String[] familyColumn= new String[]{"test1","test2"};
	//
	// createTable(conf,"test",familyColumn);
	// UUID uuid = UUID.randomUUID();
	// String s_uuid = uuid.toString();
	// SparkHBase.putRow(table, s_uuid, "uuid", "col1", s_uuid);
	// SparkHBase.getRow(table, s_uuid);
	//
	//
	//// util.queryAll();
	// // Map<String, String> paramMap = new HashMap<String, String>();
	// // paramMap.put("stat_date", "2016-02-03");
	// // Map<String, Long> map = SparkHBase.aggregateBySingleColumn(paramMap,
	// new String[]{"date", "name"}, "pv");
	//
	//
	// // for (String key : map.keySet()) {
	// // System.out.println(key + "\t" + map.get(key));
	// // }
	//
	//
	//
	// } catch (Exception e) {
	// if(e.getClass().equals(MasterNotRunningException.class)){
	// System.out.println("MasterNotRunningException");
	//
	// }
	// if(e.getClass().equals(ZooKeeperConnectionException.class)){
	// System.out.println("ZooKeeperConnectionException");
	//
	// }
	// if(e.getClass().equals(IOException.class)){
	// System.out.println("IOException");
	// }
	// e.printStackTrace();
	// }finally{
	// if(null!=table){
	// table.close();
	// }
	// }

	/**
	 * System.out.println("00--------------"); // if
	 * (admin.tableExists("table1")) {
	 * 
	 * // System.out.println("001--------------");
	 * 
	 * // admin.disableTable("table1"); // admin.deleteTable("table1"); // }
	 * System.out.println("1--------------");
	 * 
	 * HTableDescriptor tableDescripter = new HTableDescriptor("table");
	 * 
	 * System.out.println("2--------------"); tableDescripter.addFamily(new
	 * HColumnDescriptor("one")); tableDescripter.addFamily(new
	 * HColumnDescriptor("two")); tableDescripter.addFamily(new
	 * HColumnDescriptor("three")); System.out.println("3--------------"); //
	 * admin.createTable(tableDescripter);
	 * 
	 * /**
	 * 
	 * 
	 * //Put闁瑰灝绉崇紞锟�? HTable table = new HTable(conf, "user"); Put put = new
	 * Put(Bytes.toBytes("row6")); put.add(Bytes.toBytes("basic"),
	 * Bytes.toBytes("name"), Bytes.toBytes("value6")); table.put(put);
	 * table.flushCommits();
	 * 
	 * //Delete闁瑰灝绉崇紞锟�? Delete delete = new Delete(Bytes.toBytes("row1"));
	 * table.delete(delete);
	 * 
	 * table.close();
	 * 
	 * //Scan闁瑰灝绉崇紞锟�? Scan scan = new Scan();
	 * scan.setStartRow(Bytes.toBytes("0120140722"));
	 * scan.setStopRow(Bytes.toBytes("1620140728"));
	 * scan.addFamily(Bytes.toBytes("basic"));
	 * scan.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("name"));
	 * 
	 * String tableName = "user"; conf.set(TableInputFormat.INPUT_TABLE,
	 * tableName);
	 * 
	 * ByteArrayOutputStream out = new ByteArrayOutputStream(); DataOutputStream
	 * dos = new DataOutputStream(out); // scan.write(dos); String scanStr =
	 * Base64.encodeBytes(out.toByteArray()); IOUtils.closeQuietly(dos);
	 * IOUtils.closeQuietly(out); //濡ゅ倹顭囨晶妤呭嫉椤掞拷璁插ù鐘劤閺併倖淇婇崒娆戠憮闁哄倻鎳撶槐锟犳晬閿燂拷
	 * //ClientProtos.Scan proto = ProtobufUtil.toScan(scan); //String scanStr =
	 * Base64.encodeBytes(proto.toByteArray()); conf.set(TableInputFormat.SCAN,
	 * scanStr);
	 * 
	 * JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = context
	 * .newAPIHadoopRDD(conf, TableInputFormat.class,
	 * ImmutableBytesWritable.class, Result.class);
	 * 
	 * Long count = hBaseRDD.count(); System.out.println("count: " + count);
	 * 
	 * List<Tuple2<ImmutableBytesWritable, Result>> tuples = hBaseRDD
	 * .take(count.intValue()); for (int i = 0, len = count.intValue(); i < len;
	 * i++) { Result result = tuples.get(i)._2(); KeyValue[] kvs = result.raw();
	 * for (KeyValue kv : kvs) { System.out.println("rowkey:" + new
	 * String(kv.getRow()) + " cf:" + new String(kv.getFamily()) + " column:" +
	 * new String(kv.getQualifier()) + " value:" + new String(kv.getValue())); }
	 * }
	 */
	// }
}