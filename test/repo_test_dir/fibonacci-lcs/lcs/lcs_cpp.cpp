#include <iostream>

using namespace std;

int main(){
 
  string string1,string2;
  cout << "Enter the strings for comparison" << endl;
  cout << "String 1 : " ;
  cin >> string1;
  cout << "String 2 : " ;
  cin >> string2;
 
  int strlen1,strlen2;
  strlen1 = string1.length();
  strlen2 = string2.length();
 
  int table[strlen1+1][strlen2+1];
  int i,j;
  for(i=0;i<=strlen1;i++)
  {
    for(j=0;j<=strlen2;j++)
    {
      if(i==0)
      {
        table[i][j] = 0;
      }
      else if(j==0)
      {
        table[i][j] = 0;
      }
    
      else if(string1[i-1] == string2[j-1])
      {
        table[i][j] = table[i-1][j-1] + 1;
      }
    
      else
      {
        table[i][j] = (table[i-1][j]>=table[i][j-1]) ? table[i-1][j] : table[i][j-1];
      }
    
      cout << table[i][j] << " ";
    }
    cout << endl;
  }
 
 
  int lcs[30];
  i = strlen1;
  j = strlen2;
  int pointer = 0;
 
  while(i!=0 && j!=0)
  {
    if(table[i][j]!=table[i-1][j] && table[i][j]!=table[i][j-1])
    {
      i=i-1;
      j=j-1;
      lcs[pointer] = i;
      pointer = pointer+1;
    
    
    }
    else if(table[i][j]==table[i-1][j])
    {
      i=i-1;
    }
    else
    {
      j=j-1;
    }
  }
  for(i=pointer-1;i>=0;i--)
  {
    cout <<string1[lcs[i]];
  }

  cout << endl;
 
 
}