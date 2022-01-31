import threading
import time


def printSquare(n):

    sum = 0
    for i in range(1, n):
        sum = sum + (i*i)

    print(f"Sum of squares of first {n} natural numbers is {sum}")


def sleepFun():

    time.sleep(1)


if __name__ == "__main__":

    n = input("Enter n:")
    n = int(n)
    startTime = time.time()
    thread1 = threading.Thread(target=printSquare(n))
    thread2 = threading.Thread(target=sleepFun())
    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()
    print(f"Multi-thread Time of Execution {time.time()-startTime} seconds")

    startTime = time.time()
    printSquare(n)
    sleepFun()
    print(f"Sequential Time of Execution {time.time()-startTime} seconds")
