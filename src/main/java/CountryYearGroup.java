import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CountryYearGroup extends WritableComparator {


    public CountryYearGroup() {
        super(CountryYearCompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CountryYearCompositeKey compositeKey1 = (CountryYearCompositeKey) a;
        CountryYearCompositeKey compositeKey2 = (CountryYearCompositeKey) b;

        return compositeKey1.getCountry().compareTo(compositeKey2.getCountry());
    }
}
