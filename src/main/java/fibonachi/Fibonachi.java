package fibonachi;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Fibonachi{

    public List<Integer> createFList(int n) {

        int first = 1;
        int second = 1;

        List<Integer> fNumbers = new ArrayList<Integer>();

         if (n == 1) {
            fNumbers.add(first);
            return fNumbers;
        } else if (n == 2) {
            fNumbers.add(first);
            fNumbers.add(second);
            return fNumbers;
        } else {

            fNumbers.add(first);
            fNumbers.add(second);

            for (int i = 2; i < n; i++) {
                int current = fNumbers.get(i - 2) + fNumbers.get(i - 1);
                fNumbers.add(current);
            }
        }
        return fNumbers;
    }

    public void sumOfNumbers(List<Integer> numbers, int part)  {

        int end = part;

        while (!numbers.isEmpty()) {

            if (end >= numbers.size()) {
                end = numbers.size();
            }

            List<Integer> templist = new LinkedList<>(numbers.subList(0, end));
            sumOfNumbers(templist);

            numbers.removeAll(templist);

        }
    }

    private void sumOfNumbers(List<Integer> subList) {

        System.out.println();

        StringBuilder sumString = new StringBuilder("");

        int sum = 0;

        for (Integer item : subList) {
            sum += item;
            sumString.append(item).append(" + ");
        }

        sumString.delete(sumString.length() - 3, sumString.length() - 1);

        System.out.println("Sum of " + sumString.toString() + "is :" + sum);
    }

}