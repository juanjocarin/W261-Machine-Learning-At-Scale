from mrjob.job import MRJob
from mrjob.job import MRStep
from math import copysign

# This MrJob calculates the gradient of the entire training set 
#     Mapper: calculate partial gradient for each example  
#     
class MrJobBatchGDUpdate_WLS(MRJob):
    # run before the mapper processes any input
    def read_coefficientsfile(self):
        # Read coefficients file
        with open('/HD/tmp/coefficients.txt', 'r') as f:
            self.coefficients = [float(v) for v in f.readline().split(',')]
        # Initialze gradient for this iteration
        self.partial_Gradient = [0]*len(self.coefficients)
    
    # Calculate partial gradient for each example 
    def partial_gradient(self, _, line):
        D = (map(float,line.split(',')))
        # y_hat is the predicted value given current weights
        y_hat = self.coefficients[0]+self.coefficients[1]*D[1]
        # Update parial gradient vector with gradient form current example
        self.partial_Gradient = [self.partial_Gradient[0]+(y_hat-D[0])/
                                     abs(D[1]), 
                                 self.partial_Gradient[1]+(y_hat-D[0])*
                                     copysign(1,D[1])]
    
    # Finally emit in-memory partial gradient and partial count
    def partial_gradient_emit(self):
        yield None, self.partial_Gradient
        
    # Accumulate partial gradient from mapper and emit total gradient 
    # Output: key = None, Value = gradient vector
    def gradient_accumulator(self, _, partial_Gradient_Record): 
        total_gradient = [0]*2
        for partial_Gradient in partial_Gradient_Record:
            total_gradient[0] = total_gradient[0] + partial_Gradient[0]
            total_gradient[1] = total_gradient[1] + partial_Gradient[1]
        yield None, [v for v in total_gradient]

    def steps(self):
        return [MRStep(mapper_init = self.read_coefficientsfile, 
                       mapper = self.partial_gradient, 
                       mapper_final = self.partial_gradient_emit,
                       reducer = self.gradient_accumulator)]
    
if __name__ == '__main__':
    MrJobBatchGDUpdate_WLS.run()