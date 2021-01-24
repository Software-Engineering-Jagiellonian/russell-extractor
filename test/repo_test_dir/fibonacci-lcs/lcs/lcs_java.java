import java.util.Scanner;

public class LongestCommonSubsequence {


    public static void main(String[] args) {

        Scanner Longest = new Scanner(System.in);

        System.out.print("Input first String ");
        String A = Longest.next();
        System.out.print("Input second String ");
        String B = Longest.next();

        lcsMethod(A, B);

    }


    public static void lcsMethod(String A, String B) {
        int a = A.length();
        int b = B.length();

        int[][] lcsArray = new int[a + 1][b + 1];

        //returns the length of the lcs for String A nd String B

        for (int i = 0; i <= a; i++) {

            for (int j = 0; j <= b; j++) {

                if (i == 0 || j == 0) {
                    lcsArray[i][j] = 0;

                } else if (A.charAt(i - 1) == B.charAt(j - 1)) {
                    lcsArray[i][j] = lcsArray[i - 1][j - 1] + 1;
                } else {
                    lcsArray[i][j] = Math.max(lcsArray[i - 1][j], lcsArray[i][j - 1]);
                }
            }

        }

        //to print lcsMethod
        int index = lcsArray[a][b];
        int newIndex = index;

        //creating char array and store the lsc String
        char[] lcsChar = new char[index + 1];
        //lcsChar [index] = '';//set the terminating character

        while (a > 0 && b > 0) {
            //if curent char in A[] and B[] is the same, then current character is part of the lcs
            if (A.charAt(a - 1) == B.charAt(b - 1)) {
                //put current char in result
                lcsChar[index - 1] = A.charAt(a - 1);

                a--;
                b--;
                index--;
            }
            //if not the same , then find the larger of the two and move in the direction  of the larger
            else if (lcsArray[a - 1][b] > lcsArray[a][b - 1]) {
                a--;
            } else {
                b--;
            }
        }

        System.out.print("LCS of " + A + " and " + B + " is ");

        for (int i = 0; i < newIndex; i++) {
            System.out.print(lcsChar[i]);
        }


    }

}