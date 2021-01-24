const n = parseFloat(prompt('Enter the sequence number of the Fibonacci number'));

function fibonacci(n) {
  let a = 1,
      b = 1;
  for (let i = 3; i <= n; i++) {
  let c = a + b;
     a = b;
     b = c;
  }
 return b;
}

alert( fibonacci(n) );

//function fib(n){
//if (n==1||n==2) return 1;
//else return fib(n-2)+fib(n-1);		
//}
//var userNum=+prompt('Введите порядковый номер числа Фибоначчи');
//alert(fib(userNum));
