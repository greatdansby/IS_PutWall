def print_timer(debug, start, label=''):
    buffer = '-'*max(30-len(label), 0)
    if debug:
        print('{}{} Elapsed Time {:.5} seconds'.format(label, buffer, time.time()-start))
        return time.time()
    return start