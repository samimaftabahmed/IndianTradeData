import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ImportMapper extends Mapper<LongWritable, Text, CountryYearCompositeKey, Text> {

    @SuppressWarnings("Duplicates")
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text valueText = new Text();

        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

        while (tokenizer.hasMoreElements()) {

            String token = tokenizer.nextToken();

            if (!token.equals("HSCode,Commodity,value,country,year")) {

                int foundPos = 0;
                boolean nullStat = true;
                String[] datum = token.split(",");

                for (int i = 2; i < datum.length; i++) {

                    if (datum[i].matches("\\d.\\d")) {
                        nullStat = false;
                        break;
                    } else {
                        foundPos++;
                    }
                }

                if (!nullStat) {

                    String year = datum[4 + foundPos].trim();
                    String commodityValue = datum[2 + foundPos].trim();
                    String keyValue = datum[3 + foundPos].trim();

                    String val = "import " + commodityValue;
                    valueText.set(val);

                    CountryYearCompositeKey compositeKey = new CountryYearCompositeKey(
                            new Text(keyValue), new Text(year));

                    context.write(compositeKey, valueText);
                }
            }
        }
    }
}
