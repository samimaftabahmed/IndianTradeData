import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ExportMapper extends Mapper<LongWritable, Text, CountryYearCompositeKey, Text> {

    @SuppressWarnings("Duplicates")
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text keyText = new Text();
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

                String year;
                String commodityValue;
                String keyValue;

                if (nullStat) {
                    year = datum[4].trim();
                    commodityValue = "0.0";
                    keyValue = datum[3].trim();

                } else {
                    year = datum[4 + foundPos].trim();
                    commodityValue = datum[2 + foundPos].trim();
                    keyValue = datum[3 + foundPos].trim();
                }

                String val = "export " + commodityValue;
                valueText.set(val);
                keyText.set(keyValue);

                CountryYearCompositeKey compositeKey = new CountryYearCompositeKey(
                        keyValue, year);

                context.write(compositeKey, valueText);
            }
        }
    }
}

