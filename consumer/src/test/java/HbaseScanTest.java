import com.atguigu.utils.ConnectInstance;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

/**
 * 查询17515331243,在2019-01与2019-04之间的所有通话记录
 * Created by MiYang on 2018/6/4.
 */
public class HbaseScanTest {
    @Test
    public void scanData() throws ParseException, IOException {
        String phone = "17515331243";
        String startPoint = "2019-01";
        String endPoint = "2019-04";

        HbaseScanUtil scanUtil = new HbaseScanUtil();
        scanUtil.init(phone,startPoint,endPoint);
        Connection connection = ConnectInstance.getInstance();
        Table table = connection.getTable(TableName.valueOf("ct:calllog"));

        while (scanUtil.hasNext()){
            String[] rowkeys = scanUtil.next();
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(rowkeys[0]));
            scan.setStopRow(Bytes.toBytes(rowkeys[1]));

            System.out.println("时间范围"+rowkeys[0].split("_")[2]+"-----"+rowkeys[1].split("_")[2]);
            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
            }
        }
    }
}
