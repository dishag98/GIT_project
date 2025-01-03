num = 20

a,b = 0,1

for i in range(num):
    print(a)

    fib = a+b
    a = b
    b = fib
    a = b
    b = fib