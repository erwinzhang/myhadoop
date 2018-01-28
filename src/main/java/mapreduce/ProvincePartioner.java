package mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by erzhang on 1/28/18.
 */
public class ProvincePartioner extends Partitioner {

    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }
}
