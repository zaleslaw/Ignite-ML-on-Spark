package Chapter_2_IgniteDF.performance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class LargeGeneratorExample {
    public static final int AMOUNT_OF_ROWS = 10_000_000;
    /**
     * Run example.
     *
     * @param args Args.
     */
    public static void main(String... args) throws IOException {
        String fileName = "D:\\ds_" + AMOUNT_OF_ROWS + ".txt";

        if (!Files.exists(Paths.get(fileName)))
            Files.createFile(Paths.get(fileName));

        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));

        AtomicInteger cnt = new AtomicInteger(0);
        String header = "id;BUSINESS_UNIT;JOURNAL_DATE;LEDGER;ACCOUNT;ALTACCT;DEPTID;OPERATING_UNIT;PRODUCT;ACRUAL_IND_SFI;GEO_AREA_SFI;PROF_CTR_OFC_SFI";

        appendToFile(bw, header);

        String data;
        for (int i = 0; i < AMOUNT_OF_ROWS; i++) {
            int cntVal = cnt.incrementAndGet();

            data = String.valueOf(cntVal) + ";" +
                "BUSINESS_UNIT" + i % 10 + ";" +
                "JOURNAL_DATE;LEDGER;" +
                +Math.max((i + 99) % 1000, 100) + ";" +
                "ALTACCT;" +
                "DEPTID" + (i + 1) % 7 + ";" +
                "OPERATING_UNIT;" +
                "PRODUCT" + (i + 2) % 11 + ";" +
                "ACRUAL_IND_SFI;GEO_AREA_SFI;PROF_CTR_OFC_SFI";

            if (cntVal % 1_000_000 == 0)
                System.out.println(cntVal);

            appendToFile(bw, data);
        }

        try {
            bw.close();
        }
        catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private static void appendToFile(BufferedWriter bw, String data) {
        String contentToAppend = data + "\n";

        try {
            bw.write(contentToAppend);
        }
        catch (IOException e1) {
            e1.printStackTrace();
        }
    }
}
