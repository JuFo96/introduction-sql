import mysql.connector

class MySqlDBConnection:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password    
        self.connection = None
        self.connect() 

    def connect(self):
        self.connection = mysql.connector.connect(host = self.host, port= self.port, user = self.user, password = self.password, database="db")
        return self.connection
    
    def close(self):
        self.connection.close()

    def is_connected(self):
        return self.connection.is_connected()

    def execute(self, cursor, command):
        cursor.execute(command)

    #def cursor(self):
    #    return self.connection.cursor()

