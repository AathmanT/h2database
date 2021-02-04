import java.util.*;

public class HashJoin {

    public static void main(String[] args) {
        // Table_A (Age,Name)
        // Table_B (Name,Alias)
        String[][] table_A = {
                {"27", "Jonah"},
                {"18", "Alan"},
                {"28", "Glory"},
                {"18", "Popeye"},
                {"28", "Alan"}
        };

        String[][] table_B = {
                {"Jonah", "Whales"},
                {"Jonah", "Spiders"},
                {"Alan", "Ghosts"},
                {"Alan", "Zombies"},
                {"Glory", "Buffy"},
                {"Bob", "foo"}
        };

        hashJoin(table_A, 1, table_B, 0).stream()
                .forEach(r -> System.out.println(Arrays.deepToString(r)));
    }

    static List<String[][]> hashJoin(String[][] table_A, int column_idx_A,
            String[][] table_B, int column_idx_B) {

        List<String[][]> results = new ArrayList<>();
        Map<String, List<String[]>> map = new HashMap<>();

        // Creating a hashmap and assigning each unique record as the key for other records
        for (String[] record_A : table_A) {

           /* map = {
                    "key1":bucket1,
                    "key2":bucket2,
            }
            */
            // get existing bucket or get a default new bucket
            List<String[]> bucket = map.getOrDefault(key = record_A[column_idx_A], defaultValue = new ArrayList<>());

            bucket.add(record_A);
            map.put(record_A[column_idx_A], bucket);
        }

        // Using hashmap get the bucket of records for each record in table_B
        for (String[] record_B : table_B) {

            List<String[]> bucket = map.get(record_B[column_idx_B]);

            if (bucket != null) {

                bucket.stream().forEach(record_A -> {

                    // Concatanate both records and add to results
                    results.add(new String[][]{record_A, record_B});
                });
            }
        }

        return results;
    }
}