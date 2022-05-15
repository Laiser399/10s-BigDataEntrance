package joincountry.writables;

import joincountry.enums.TextType;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TypedText extends Text {
    private TextType textType;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(textType.ordinal());
        super.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        textType = TextType.values()[dataInput.readInt()];
        super.readFields(dataInput);
    }

    public TextType getTextType() {
        return textType;
    }

    public void setTextType(TextType textType) {
        this.textType = textType;
    }
}
