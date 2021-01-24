import java.util.Scanner;

public class FibonacciNumber {
    public static void main(String args[]) {
        Scanner in = new Scanner(System.in);
        int n = in.nextInt();
        System.out.printf("%.0f\n", calc_fib_fast(n));
    }

    private static double calc_fib_fast(int n) {
        if (n <= 1) {
            return n;
        } else {
            double[] fibs = new double[n+1];
            fibs[0] = 0;
            fibs[1] = 1;

            for (int i = 2; i < n+1; i++) {
                fibs[i] = fibs[i - 1] + fibs[i - 2];
            }

            return fibs[n];
        }
    }
}
