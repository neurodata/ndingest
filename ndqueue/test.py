

def y_test(message_list):
    if message_list is None:
        raise StopIteration
    else:
        for message in message_list:
            yield message[0], message[1], message[2]


message_list = [[1,2,3]]


a = 1