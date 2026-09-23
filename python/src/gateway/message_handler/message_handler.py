from common import message_protocol


class MessageHandler:
    # Class variable, each client has an instance  of this class with a different id
    client_id_count = 0

    def __init__(self):
        self.client_id = MessageHandler.client_id_count
        MessageHandler.client_id_count += 1
        
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    # EOF by lenght, if len == 1 it's EOF
    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    # If it's my ID, returns a list with the top
    def deserialize_result_message(self, message):
        [client_id, top] = message_protocol.internal.deserialize(message)
        if client_id == self.client_id:
            return top
        else:
            return None
