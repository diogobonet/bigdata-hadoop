package q5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class transactionAvgYearWritable implements Writable {
    private Long valor;
    private Long qtd;

    public transactionAvgYearWritable() {
    }

    public transactionAvgYearWritable(Long base, Long qtd) {
        this.valor = base;
        this.qtd = qtd;
    }

    public Long getValor() {
        return valor;
    }

    public void setValor(Long valor) {
        this.valor = valor;
    }

    public Long getQtd() {
        return qtd;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(valor));
        dataOutput.writeUTF(String.valueOf(qtd));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        valor = Long.parseLong(dataInput.readUTF());
        qtd = Long.parseLong(dataInput.readUTF());
    }
}