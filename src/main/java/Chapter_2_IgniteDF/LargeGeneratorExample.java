package Chapter_2_IgniteDF;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class LargeGeneratorExample {
    /**
     * Run example.
     *
     * @param args Args.
     */
    public static void main(String... args) throws IOException {
        String fileName = "D:\\ds_large.txt";

        if (!Files.exists(Paths.get(fileName)))
            Files.createFile(Paths.get(fileName));

        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));

        AtomicInteger cnt = new AtomicInteger(0);
        String header = "id;BUSINESS_UNIT;JOURNAL_DATE;LEDGER;ACCOUNT;ALTACCT;DEPTID;OPERATING_UNIT;PRODUCT;ACRUAL_IND_SFI;GEO_AREA_SFI;PROF_CTR_OFC_SFI";

        appendToFile(bw, header);

        String data;
        for (int i = 0; i < 9_000_000; i++) {
            int cntVal = cnt.incrementAndGet();

            data = String.valueOf(cntVal) + ";BUSINESS_UNIT;JOURNAL_DATE;LEDGER;ACCOUNT;ALTACCT;DEPTID;OPERATING_UNIT;PRODUCT;ACRUAL_IND_SFI;GEO_AREA_SFI;PROF_CTR_OFC_SFI";

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
