package Utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class New_key implements WritableComparable<New_key> {
    private String w1;
    private String w2;
    private Double probability;

    public New_key(String w1, String w2, Double probability) {
        super();
        this.w1 = w1;
        this.w2 = w2;
        this.probability = probability;
    }

    public String getW1() {
        return this.w1;
    }

    public String getW2() {
        return this.w2;
    }

    public Double getProbability() {
        return this.probability;
    }

    @Override
    public int compareTo(New_key o) {
        int result1;
        int result2;
        if ((result1 = this.w1.compareTo(o.getW1())) == 0) {
            if ((result2 = this.w2.compareTo(o.getW2())) == 0) {
                return this.probability.compareTo(o.getProbability()) * (-1);
            }
            else
                return result2;
        }
        else
            return result1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.w1);
        dataOutput.writeUTF(this.w2);
        dataOutput.writeDouble(this.probability);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.w1 = dataInput.readUTF();
        this.w2 = dataInput.readUTF();
        this.probability = dataInput.readDouble();
    }
}
