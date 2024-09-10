package q1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class keyWritable implements WritableComparable<keyWritable> {

    String mes;
    String weekday;

    public keyWritable(String mes, String weekday) {
        this.mes = mes;
        this.weekday = weekday;
    }

    public String getMes() {
        return mes;
    }

    public void setMes(String mes) {
        this.mes = mes;
    }

    public String getWeekday() {
        return weekday;
    }

    public void setWeekday(String weekday) {
        this.weekday = weekday;
    }


    @Override
    public String toString() {
        return "q1.keyWritable{" +
                "mes='" + mes + '\'' +
                ", weekday='" + weekday + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        keyWritable that = (keyWritable) o;
        return Objects.equals(mes, that.mes) && Objects.equals(weekday, that.weekday);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mes, weekday);
    }

    @Override
    public int compareTo(keyWritable o) {
        if(this.hashCode() < o.hashCode()) {
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return 1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(mes));
        out.writeUTF(String.valueOf(weekday));
    }

    //le os campos, ou seja, lê os atributos
    @Override
    public void readFields(DataInput in) throws IOException {
        mes = in.readUTF();
        weekday = in.readUTF();

    }
}
