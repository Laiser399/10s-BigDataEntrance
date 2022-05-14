package mapusers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class UsersMapper extends Mapper<Object, Text, NullWritable, Text> {
    private final Logger logger = Logger.getLogger(UsersMapper.class.getName());

    private final Text outputText = new Text();
    private Map<String, String> locationMappings;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        Path locationMappingsPath = new Path(configuration.get(Main.LOCATION_MAPPINGS_PATH));
        FileSystem fs = FileSystem.get(configuration);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(locationMappingsPath)))) {
            locationMappings = reader
                    .lines()
                    .map(this::splitLocationMappingsLine)
                    .collect(Collectors.toMap(
                            LocationMappingEntry::getWeirdLocation,
                            LocationMappingEntry::getCountry
                    ));
        }
    }

    private LocationMappingEntry splitLocationMappingsLine(String line) {
        int index = line.lastIndexOf('\t');
        if (index == -1) {
            String errorMessage = String.format("Not found delimiter at line \"%s\".", line);
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        String weirdLocation = unescapeBackSlashes(unescapeQuotes(line.substring(0, index)));
        String country = line.substring(index + 1);

        return new LocationMappingEntry(weirdLocation, country);
    }

    private String unescapeQuotes(String value) {
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private String unescapeBackSlashes(String value) {
        return value.replaceAll("\\\\\\\\", "\\\\");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\u0001", -1);

        if (parts.length < 14) {
            String elements = Arrays.stream(parts)
                    .map(x -> "\t" + x + "\n")
                    .collect(Collectors.joining());

            String charCodes = value.toString().chars()
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining(","));

            String errorMessage = String.format(
                    "Wrong number of elements in row. Expected 14, got %d.\n" +
                            "Line: \"%s\".\n" +
                            "Char codes: %s.\n" +
                            "Elements:\n" +
                            "%s",
                    parts.length, value, charCodes, elements);
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        long userId;
        try {
            userId = Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            logger.warn(String.format("Could not parse user id. Value: \"%s\".", parts[0]), e);
            return;
        }
        String weirdLocation = parts[6];

        if (locationMappings.containsKey(weirdLocation)) {
            outputText.set(String.format("%d\u0001%s", userId, locationMappings.get(weirdLocation)));
            context.write(NullWritable.get(), outputText);
        } else {
            logger.warn(String.format("Mapping for location \"%s\" was not found.", weirdLocation));
        }
    }
}
