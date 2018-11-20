import numpy as np

def evaluate(l1,l2,n=150):
    weights = []
    for i in range(1,n+1):
        a = np.array(l1.copy()[0:i])
        b = 0.0
        for x in range(0,i):
            if l2.copy().loc[x] in a:
                b+=1.0
        weights.append(b/i)
    return np.mean(weights)

