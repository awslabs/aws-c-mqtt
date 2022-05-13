
import random
from time import sleep

# Fibonacci script to simulate load on the CPU
class FibonacciRunner():
    def __init__(self, times_to_run, sleep_between_runs, chance_of_error) -> None:
        self.times_to_run = times_to_run
        self.sleep_between_runs = sleep_between_runs
        self.chance_of_error = chance_of_error

    def start(self):
        fib_number = 0
        while (fib_number < self.times_to_run):
            fib_number += 1
            #print ("On canary number: ", fib_number)

            self.fibonacci(fib_number)
            if (self.chance_of_error > 0):
                if (random.uniform(0, 1) <= self.chance_of_error):
                    print ("Random canary crash!!!")
                    exit(1) # Random "crash"
            if (self.sleep_between_runs > 0):
                sleep(self.sleep_between_runs)

    def fibonacci(self, number):
        # process stuff - just return a number for testing normally. Other stuff below was an attempt to stress the CPU
        return 0

        if (number <= 0):
            return 0 # cannot calculate zero
        # handle 1 and 2
        elif number == 1 or number == 2:
            return 1
        else:
            return self.fibonacci(number - 1) + self.fibonacci(number - 2)


# Simulate a CPU load
print ("Starting \"Canary\"...")
runner = FibonacciRunner(60*1, 1, 0.0)
runner.start()
print ("\"Canary\" Finished!")

exit(0) # successful run
#exit(1) # unsuccessful run
