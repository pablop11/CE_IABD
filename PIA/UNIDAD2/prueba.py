a, b = 0,1
while a < 10:
    print(a)
    a, b = b, a+b
print("EL PRIMER ELEMENTO MAYOR QUE 10 ES ", a)