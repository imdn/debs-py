from scipy.cluster.vq import kmeans,kmeans2

def cluster(data, seeds, iter_limit):
    # Kmeans2 is slower
    result = kmeans(data, seeds, iter=iter_limit)
    #return result[0]
    return result
