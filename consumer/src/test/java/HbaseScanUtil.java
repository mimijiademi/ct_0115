import com.atguigu.utils.HbaseUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 17515331243,在2019-01与2019-04
 * Created by MiYang on 2018/6/4.
 */
public class HbaseScanUtil {
    private List<String[]> list;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
    private int i = 0;

    /**
     *  phone:17515331243,start:2019-01,stop:2019-04
     *  按照以下范围寻找
     *  2019-01~2019-02
     *  2019-02~2019-03
     *  2019-03~2019-04
     *  2019-04~2019-05
     *
     */
    public void init(String phone, String start, String stop) throws ParseException {
        list = new ArrayList<String[]>();
        Date startDate = sdf.parse(start);
        Date stopDate = sdf.parse(stop);
        //当前范围段，例如2019-01~2019-02，开始的开始时间点
        Calendar startPoint = Calendar.getInstance();
        startPoint.setTime(startDate);
        //当前范围段，例如2019-01~2019-02，结束的时间点
        Calendar stopPoint = Calendar.getInstance();
        stopPoint.setTime(startDate);
        stopPoint.add(Calendar.MONTH, 1);

        while (stopPoint.getTimeInMillis() <= stopDate.getTime()) {

            String startTime = sdf.format(startPoint.getTime());
            String stopTime = sdf.format(stopPoint.getTime());

            String regionHash = HbaseUtil.getRegionHash(phone, startTime, 6);

            String startRow = regionHash + "_" + phone + "_" + startTime;
            String stopRow = regionHash + "_" + phone + "_" + stopTime;

            String[] rowkeys = {startRow, stopRow};
            list.add(rowkeys);

            startPoint.add(Calendar.MONTH,1);
            stopPoint.add(Calendar.MONTH,1);


        }
    }


    public boolean hasNext(){
        return i<list.size();
    }

    public String[] next(){
        return list.get(i++);
    }
}
