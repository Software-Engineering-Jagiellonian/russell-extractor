cache = {"1": 0, "2": 1}
def fib(n):
    if str(n) in cache:
        return cache[str(n)]
    if type(n) != int:
        raise TypeError("must be an int")
    if n < 1:
        raise ValueError("must be greater than or equal to 1")
    res = fib(n - 1) + fib(n - 2)
    cache[str(n)] = res
    return res
if __name__ == "__main__":
    n = int(input("Number: "))
    print(fib(n))
