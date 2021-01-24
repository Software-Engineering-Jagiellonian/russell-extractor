using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LongestCommonSubsequence
{
    class LCS
    {
        private readonly int[,] table;
        private string x;
        private string y;
        public LCS(string x, string y)
        {
            table = new int[x.Length+1,y.Length+1];
            this.x = x;
            this.y = y;
            LCSFillUpTable();
        }
        public List<String> GetAllLongestSubsequences()
        {
            return GetAllSubsequences(x.Length, y.Length).Where(x=> x.Length > 1).Distinct().ToList();
        }
        private List<String> GetAllSubsequences(int m, int n)
        {
            if (m == 0 || n == 0)
            {
                return new List<string> { "" };
            }

            if (this.x[m - 1] == this.y[n - 1])
            {
                List<String> lcs = GetAllSubsequences(m - 1, n - 1);

                for (int i = 0; i < lcs.Count; i++)
                {
                    lcs[i] += this.x[m - 1];
                }

                return lcs;
            }

            
            if (table[m - 1, n] > table[m, n - 1])
                return GetAllSubsequences(m - 1, n);

            if (table[m, n - 1] > table[m - 1, n])
                return GetAllSubsequences(m, n - 1);

            //Tu następuje rozdzielenie gdy wartości są takie same
            List<String> top = GetAllSubsequences(m - 1, n);
            List<String> left = GetAllSubsequences(m, n - 1);

            //Połącz listy ze sobą
            top.AddRange(left);

            return top;
        }

        private void LCSFillUpTable()
        {
            for (int i = 1; i <= this.x.Length; i++)
            {
                for (int j = 1; j <= this.y.Length; j++)
                {
                    if (this.x[i - 1] == this.y[j - 1])
                        table[i,j] = table[i - 1,j - 1] + 1;
                    else
                        table[i,j] = Math.Max(table[i - 1, j], table[i, j - 1]);
                }
            }
        }

        public override string ToString()
        {
            return GetAllLongestSubsequences().First();
        }
    }
}