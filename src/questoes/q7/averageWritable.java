package questoes.q7;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class averageWritable implements Writable {
    private Long total;
    private Long quantity;

    public averageWritable () {

    }

    public averageWritable (Long total, Long quantity) {
        this.total = total;
        this.quantity = quantity;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Long getQuantity() { return quantity; }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(total));
        dataOutput.writeUTF(String.valueOf(quantity));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total = Long.parseLong(dataInput.readUTF());
        quantity = Long.parseLong(dataInput.readUTF());
    }
}
