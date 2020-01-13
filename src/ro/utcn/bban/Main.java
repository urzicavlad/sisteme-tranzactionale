package ro.utcn.bban;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;

public class Main {


    private static final List<String> OPERATIONS = new ArrayList<>();
    private static final List<String> T1_VARIABLES = new ArrayList<>();
    private static final List<String> T2_VARIABLES = new ArrayList<>();
    private static final List<String> T3_VARIABLES = new ArrayList<>();
    static {
        OPERATIONS.add("READ");
        OPERATIONS.add("WRITE");

        T1_VARIABLES.add("X");
        T1_VARIABLES.add("Y");

        T2_VARIABLES.add("X");
        T2_VARIABLES.add("Y");
        T2_VARIABLES.add("Z");

        T3_VARIABLES.add("Y");
        T3_VARIABLES.add("Z");

    }
    private static final List<String> COMMITS = List.of(getCommit("T1"), getCommit("T2"), getCommit("T3"));
    private static final List<int[]> COMMIT_INDEXES = generate(10, 3);
    private static final Set<List<String>> T1 = getTransactionTwoOperations(createInput(OPERATIONS, T1_VARIABLES), "T1");
    private static final Set<List<String>> T2 = getTransactionThreeOperations(createInput(OPERATIONS, T2_VARIABLES), "T2");
    private static final Set<List<String>> T3 = getTransactionTwoOperations(createInput(OPERATIONS, T3_VARIABLES), "T3");
    private static final Set<Set<String>> SPLIT_TRANSACTION_WITHOUT_COMMIT = cartesianProduct(T1, T2, T3);


    public static void main(String[] args) throws FileNotFoundException {

        print("N1", T1, new File("resources/T1.log"));
        print("N2", T2, new File("resources/T2.log"));
        print("N3", T3, new File("resources/T3.log"));

        System.setOut(new PrintStream(new File("resources/H.log")));
        final String[][] totalSerialHistory = shuffleProduct(normalize(), 10);
        System.out.println(String.format("==================== [%s] SIZE ---" + totalSerialHistory.length
                + "--- ====================", "H"));
        Stream.of(totalSerialHistory).forEach(array-> System.out.println(Arrays.toString(array)));

        System.setOut(new PrintStream(new File("resources/CSR(H).log")));
        final String[][] CSR = generateCSR(SPLIT_TRANSACTION_WITHOUT_COMMIT);
        System.out.println(String.format("==================== [%s] SIZE ---" + CSR.length
                + "--- ====================", "N_CSR"));
        Stream.of(CSR).forEach(array-> System.out.println(Arrays.toString(array)));

    }

    private static Set<String> normalize(){
        return SPLIT_TRANSACTION_WITHOUT_COMMIT.stream().map(set -> {
            StringBuilder sb = new StringBuilder();
            set.forEach(sb::append);
            return sb.toString().replace("][", ",");
        }).collect(Collectors.toSet());
    }

    private static Set<List<String>> normalize(Set<Set<String>> sets){
        return sets.stream().map(ArrayList::new).collect(Collectors.toSet());
    }

    private static void print(String cardinalName, Set<List<String>> input, File file) {
        try {
            System.setOut(new PrintStream(file));
            System.out.println(String.format("==================== [%s] SIZE ---" + input.size()
                    + "--- ====================", cardinalName));
            input.stream()
                    .peek(strings -> strings.add(getCommit(cardinalName.replace("N", "T"))))
                    .forEach(System.out::println);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static String[][] generateCSR(Set<Set<String>> sets) {
        Set<String> CSRList = new HashSet<>();
        for (Set<String> set : sets) {
            List<String> transactions = new ArrayList<>(set);
            if ((!checkCSR(transactions.get(0), transactions.get(1)))
                    || (!checkCSR(transactions.get(1), transactions.get(2))
                    || (!checkCSR(transactions.get(2), transactions.get(0))))) {
                continue;
            }
            StringBuilder sb = new StringBuilder();
            for (String el : transactions) {
                sb.append(el);
            }
            final String replace = sb.toString().replace("][", ",");
            CSRList.add(replace);
        }

        final String[][] csrPotential = shuffleProduct(CSRList, 10);
        final String[][] csrTrue = new String[22941][10];

        int i = 0;
        label:
        for (String[] elements : csrPotential) {
            boolean T1 = false;
            boolean T2 = false;
            boolean T3 = false;
            for (String element : elements) {
                if (element == null) {
                    break label;
                }
                if (element.equalsIgnoreCase("COMMIT(T1)")) T1 = true;
                if (element.equalsIgnoreCase("COMMIT(T2)")) T2 = true;
                if (element.equalsIgnoreCase("COMMIT(T3)")) T3 = true;
                if (element.contains("-T1") && T1) continue label;
                if (element.contains("-T2") && T2) continue label;
                if (element.contains("-T3") && T3) continue label;
            }
            csrTrue[i++] = elements;
        }
        return csrTrue;
    }

    public static String[][] shuffleProduct(Set<String> H, int length) {
        final String[][] result = new String[(H.size() * COMMIT_INDEXES.size()) + 1][length];
        int i = 0;
        for (String h : H) {
            for (int[] array : COMMIT_INDEXES) {
                int z = 0;
                for (int index : array) {
                    result[i][index] = COMMITS.get(z++);
                }
                final String[] strings = splitString(h);

                int count = 0;
                final String[] elements = result[i];
                for (int j = 0; j < length; j++) {
                    if (isNull(elements[j])) {
                        elements[j] = strings[count++];
                    }
                }
                i++;
            }
        }
        return result;
    }


    private static String[] splitString(String item) {
        return item.replace("[", "").replace("]", "").split(",");
    }

    private static void helper(List<int[]> combinations, int data[], int start, int end, int index) {
        if (index == data.length) {
            int[] combination = data.clone();
            combinations.add(combination);
        } else if (start <= end) {
            data[index] = start;
            helper(combinations, data, start + 1, end, index + 1);
            helper(combinations, data, start + 1, end, index);
        }
    }


    public static List<int[]> generate(int n, int r) {
        List<int[]> combinations = new ArrayList<>();
        helper(combinations, new int[r], 0, n - 1, 0);
        return combinations;
    }


    public static boolean checkCSR(String transaction1, String transaction2) {
        if (transaction1.contains("WRITE(X") && transaction2.contains("WRITE(X")) {
            return false;
        } else if (transaction1.contains("WRITE(Y") && transaction2.contains("WRITE(Y")) {
            return false;
        } else return !transaction1.contains("WRITE(Z") || !transaction2.contains("WRITE(Z");
    }


    public static Set<Set<String>> cartesianProduct(Set<?>... sets) {
        if (sets.length < 2)
            throw new IllegalArgumentException(
                    "Can't have a product of fewer than two sets (got " +
                            sets.length + ")");

        return _cartesianProduct(0, sets);
    }

    private static Set<Set<String>> _cartesianProduct(int index, Set<?>... sets) {
        Set<Set<String>> ret = new HashSet<>();
        if (index == sets.length) {
            ret.add(new HashSet<>());
        } else {
            for (Object obj : sets[index]) {
                for (Set<String> set : _cartesianProduct(index + 1, sets)) {
                    set.add(obj.toString());
                    ret.add(set);
                }
            }
        }
        return ret;
    }

    private static Set<List<String>> getTransactionThreeOperations(List<String> input, String transactionName) {
        final Set<List<String>> result = new HashSet<>();
        for (final String operation : input) {
            for (int j = 0; j < input.size(); j++) {
                if (operation.equalsIgnoreCase(input.get(j))) continue;
                final List<String> temp = new ArrayList<>();
                temp.add(operation.replace(")", "-".concat(transactionName).concat(")")));
                temp.add(input.get(j).replace(")", "-".concat(transactionName).concat(")")));
                for (int i = 0; i < input.size(); i++) {
                    if (operation.equalsIgnoreCase(input.get(i))) continue;
                    if (input.get(j).equalsIgnoreCase(input.get(i))) continue;
                    List<String> tes = new ArrayList<>(temp);
                    tes.add(input.get(i).replace(")", "-".concat(transactionName).concat(")")));
                    //tes.add(getCommit(transactionName));
                    result.add(tes);
                }
            }
        }
        return result;
    }


    private static Set<List<String>> getTransactionTwoOperations(List<String> input, String transactionName) {
        final Set<List<String>> result = new HashSet<>();
        for (final String operation : input) {
            for (String s : input) {
                if (operation.equalsIgnoreCase(s)) continue;
                final List<String> temp = new ArrayList<>();
                temp.add(operation.replace(")", "-".concat(transactionName).concat(")")));
                temp.add(s.replace(")", "-".concat(transactionName).concat(")")));
                // temp.add(getCommit(transactionName));
                result.add(temp);
            }
        }
        return result;
    }

    private static List<String> createInput(List<String> operations, List<String> variables) {
        final List<String> result = new ArrayList<>();
        for (String operation : operations) {
            for (String variable : variables) {
                result.add(String.format(operation.concat("(%s)"), variable));
            }
        }
        return result;
    }

    private static String getCommit(String transactionName) {
        return String.format("COMMIT(%s)", transactionName);
    }

}