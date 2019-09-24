import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryYearCompositeKey implements WritableComparable<CountryYearCompositeKey> {

    private String country;
    private String year;

    public CountryYearCompositeKey() {
    }

    public CountryYearCompositeKey(String country, String year) {
        this.country = country;
        this.year = year;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public int compareTo(CountryYearCompositeKey countryYearCompositeKey) {
        return ComparisonChain.start()
                .compare(country, countryYearCompositeKey.country)
                .compare(year, countryYearCompositeKey.year)
                .result();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(country);
        out.writeUTF(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country = in.readUTF();
        year = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountryYearCompositeKey that = (CountryYearCompositeKey) o;
        return Objects.equal(country, that.country) &&
                Objects.equal(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(country, year);
    }


}
