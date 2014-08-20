package cloud;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Scanner;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

public class Cloud {
  static int SCALE =800;
  public static void main(String[] args) throws IOException {
    while(true){
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    HTable table = new HTable(conf, "TAGCLOUD");
    System.out.println("태그클라우드! 단어를 입력하세요.(q:취소)");
    Scanner sc = new Scanner(System.in);
    String keyword = sc.next();	
    if(keyword.equalsIgnoreCase("q")) break;
    System.out.println("최근 몇일간의 자료를 원하십니까?");
    int x = sc.nextInt();
    SimpleDateFormat fmt = new SimpleDateFormat("yyMMddHH");
    Date date = new Date();
    long dt = calDate(-1);
//    long dt = Long.parseLong(fmt.format(date));
    long dt1 = calDate(x);
    Scan scan = new Scan();
    Scan scan2 = new Scan();
    scan.addColumn(Bytes.toBytes("cnt"), Bytes.toBytes(""));


    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(keyword+"-"));
    Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("-"+keyword));
    scan.setFilter(filter);
    scan2.setFilter(filter2);
    scan.setTimeRange(dt1,dt);
    scan2.setTimeRange(dt1,dt);
    ResultScanner scanner = table.getScanner(scan);
    ResultScanner scanner2 = table.getScanner(scan2);
    Map<String, Integer> hm = new HashMap<String, Integer>();
	int a=0;
    for (Result res : scanner) {
	String row = Bytes.toString(res.getRow());
	String rel = row.substring(row.indexOf("-")+1);
	int cnt = Integer.parseInt(Bytes.toString(res.getValue(Bytes.toBytes("cnt"),Bytes.toBytes(""))));
	int relcnt = getrelcnt(rel, keyword);
//	if(relcnt==0){
//	}else{
		int rate = cnt + relcnt;
		if(rate>SCALE){
			if(hm.containsKey(rel)){
				hm.put(rel, (hm.get(rel)+rate)/2);
			}
			else hm.put(rel, rate);	
//			System.out.println(rel+"\t"+rate);
		}
  //      }
    }
    for(Result res : scanner2) {
	String row = Bytes.toString(res.getRow());
	String rel = row.substring(0,row.indexOf("-"));
	int cnt = Integer.parseInt(Bytes.toString(res.getValue(Bytes.toBytes("cnt"),Bytes.toBytes(""))));
	if(!hm.containsKey(rel)&&cnt>SCALE){
		hm.put(rel,cnt);
	}
    }
    List<String> list = Srt.sort(hm);
//    int K=1;
    for(String s : list){
	Get get = new Get(Bytes.toBytes(keyword+"-"+s));
	get.addColumn(Bytes.toBytes("url"), Bytes.toBytes(""));
	Result res = table.get(get);
	byte[] val = res.getValue(Bytes.toBytes("url"),Bytes.toBytes(""));
	String url = Bytes.toString(val);
	if(url==null){
		url=geturl(s,keyword);
	}	
	System.out.println(s+"\t"+url);
//	K++;
//	if(K==40){
//		break;
//	}
    }
    System.out.println();
    scanner.close();
  }
  }


  public static int getrelcnt(String rel, String keyword) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    HTable table = new HTable(conf, "TAGCLOUD"); 
    Get get = new Get(Bytes.toBytes(rel+"-"+keyword));
    get.addColumn(Bytes.toBytes("cnt"), Bytes.toBytes(""));
    Result res = table.get(get);
    byte[] val = res.getValue(Bytes.toBytes("cnt"),Bytes.toBytes(""));
    String value = Bytes.toString(val);
    if(value!=null){
	    int cnt = Integer.parseInt(value);
	    return cnt;
    } else
	return 0;
  }
  public static String geturl(String rel, String keyword) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    HTable table = new HTable(conf, "TAGCLOUD");
    Get get = new Get(Bytes.toBytes(rel+"-"+keyword));
    get.addColumn(Bytes.toBytes("url"), Bytes.toBytes(""));
    Result res = table.get(get);
    byte[] val = res.getValue(Bytes.toBytes("url"),Bytes.toBytes(""));
    String value = Bytes.toString(val);
    if(value!=null){
            return value;
    } else
        return "";
    
  }
  public static long calDate(int x){
    SimpleDateFormat fmt = new SimpleDateFormat("yyMMddHH");
    GregorianCalendar cal = new GregorianCalendar();
    cal.add(cal.DATE,-x);
    Date date1 = cal.getTime();
    String date = fmt.format(date1);
    return Long.parseLong(date);
  }
}
