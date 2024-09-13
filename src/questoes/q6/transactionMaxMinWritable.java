package questoes.q6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class transactionMaxMinWritable implements Writable {
    private Long max;
    private Long min;


    public transactionMaxMinWritable(Long max, Long min) {
        this.max = max;
        this.min = min;
    }

    public Long getMin() {
        return min;
    }

    public void setMin(Long min) {
        this.min = min;
    }

    public Long getMax() {
        return max;
    }

    public void setMax(Long max) {
        this.max = max;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(max));
        dataOutput.writeUTF(String.valueOf(min));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        max = Long.parseLong(dataInput.readUTF());
        min = Long.parseLong(dataInput.readUTF());
    }
}
