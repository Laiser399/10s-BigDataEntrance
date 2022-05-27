package joincountry.writables;

import joincountry.enums.RowType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TypedRow implements WritableComparable<TypedRow> {
    private RowType rowType = RowType.USER;
    private final Text rowText = new Text();

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(rowType.ordinal());
        rowText.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowType = RowType.values()[dataInput.readInt()];
        rowText.readFields(dataInput);
    }

    @Override
    public int compareTo(TypedRow o) {
        int result = rowType.compareTo(o.rowType);
        if (result != 0) {
            return result;
        }

        return rowText.compareTo(o.rowText);
    }

    public RowType getRowType() {
        return rowType;
    }

    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    public String getRow() {
        return rowText.toString();
    }

    public void setRow(String rowText) {
        this.rowText.set(rowText);
    }
}
