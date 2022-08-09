from time import sleep
from random import random
from threading import Thread
from queue import Queue

def producer(queue):
    print('Producer: Running')
    # Parse x pages at a time and 
    for i in range(100):
        value = random()
        sleep(value)
        item = (i, 3*value)
        queue.put(item)

    queue.put(None)
    print('Producer: Done')

def consumer(queue, identifier):
    print(f'Consumer {identifier}: Running')
    while True:
        size = queue.qsize()
        print(f'>queue size: {size}')
        item = queue.get()
        if item == None:
            queue.put(item)
            break
        sleep(item[1])
        print(f'>consumer {identifier} got {item}')
    print(f'Consumer {identifier}: Done')

def main():
    queue = Queue()
    consumers = [Thread(target=consumer, args=(queue,i)) for i in range(3)]
    for consumer_ in consumers:
        consumer_.start()

    producer_ = Thread(target=producer, args=(queue,))
    producer_.start()
    producer_.join()

    for consumer_ in consumers:
        consumer_.join()

if __name__ == '__main__':
    main()
