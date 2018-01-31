package mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by erzhang on 1/30/18.
 */
public class RateBean implements Writable {

    private String movie;
    private String rate;

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(getMovie());
        dataOutput.writeUTF(getRate());
    }

    public void readFields(DataInput dataInput) throws IOException {
        setMovie(dataInput.readUTF());
        setRate(dataInput.readUTF());
    }


}
