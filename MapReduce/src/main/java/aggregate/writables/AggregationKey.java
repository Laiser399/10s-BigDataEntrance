package aggregate.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregationKey implements WritableComparable<AggregationKey> {
    private final Text countryText = new Text();
    private int year;
    private int quarter;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        countryText.write(dataOutput);
        dataOutput.writeInt(year);
        dataOutput.writeInt(quarter);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        countryText.readFields(dataInput);
        year = dataInput.readInt();
        quarter = dataInput.readInt();
    }

    @Override
    public int compareTo(AggregationKey o) {
        int result = countryText.toString().compareTo(o.countryText.toString());
        if (result != 0) {
            return result;
        }

        result = Integer.compare(year, o.year);
        if (result != 0) {
            return result;
        }

        return Integer.compare(quarter, o.quarter);
    }

    public String getCountry() {
        return countryText.toString();
    }

    public int getYear() {
        return year;
    }

    public int getQuarter() {
        return quarter;
    }

    public void setCountry(String country) {
        countryText.set(country);
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setQuarter(int quarter) {
        this.quarter = quarter;
    }
}
