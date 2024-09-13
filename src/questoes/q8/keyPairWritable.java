package questoes.q8;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class keyPairWritable implements WritableComparable<keyPairWritable> {
    private String year;
    private String country;

    public keyPairWritable() {

    }

    public keyPairWritable(String year, String country) {
        this.year = year;
        this.country = country;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return "Ano='" + year + '\'' +
                ", Pais='" + country;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        keyPairWritable that = (keyPairWritable) o;
        return Objects.equals(year, that.year) && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, country);
    }

    @Override
    public int compareTo(keyPairWritable o) {
        if (this.hashCode() < o.hashCode()) {
            return -1;
        } else if (this.hashCode() > o.hashCode()){
            return 1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(country);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readUTF();
        country = dataInput.readUTF();
    }
}
