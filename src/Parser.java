package mapred;

public class Parser {

    public static class ParsedLine {
        public String stateCode;
        public double dailyMean;

        public ParsedLine(String s, double m) {
            this.stateCode = s;
            this.dailyMean = m;
        }
    }

    public static ParsedLine parse(String line) {
        if (line == null) return null;
        line = line.trim();
        if (line.length() == 0) return null;

        // skip header row (always row 1)
        if (line.startsWith("State Code")) {
            return null;
        }

        // CSV split, keep empty fields
        String[] cols = line.split(",", -1);

        // need at least:
        //  0  -> State Code
        //  16 -> Arithmetic Mean
        if (cols.length <= 16) {
            return null;
        }

        String stateCode = cols[0].trim();
        if (stateCode.length() == 0) {
            return null;
        }

        // Arithmetic Mean column
        String meanStr = cols[16].trim();
        if (meanStr.length() == 0) {
            // no daily mean?skip this day
            return null;
        }

        double mean;
        try {
            mean = Double.parseDouble(meanStr);
        } catch (NumberFormatException e) {
            // unusual entry;skip
            return null;
        }

        return new ParsedLine(stateCode, mean);
    }
}