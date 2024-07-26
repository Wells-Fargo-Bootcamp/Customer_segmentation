import mysql.connector
import logging
import apache_beam as beam

class WriteToMySQL(beam.DoFn):
    def __init__(self, host, database, user, password, batch_size=100):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.insert_buffer = []
        self.update_buffer = []

    def start_bundle(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as e:
            logging.error(f"Error connecting to MySQL: {e}")
            raise

    def process(self, element):
        try:
            self.cursor.execute("SELECT * FROM customer WHERE CustomerID = %s", (element['CustomerID'],))
            existing_record = self.cursor.fetchone()

            if existing_record:
                logging.info(f"CustomerID {element['CustomerID']} already exists. Updating record...")
                self.update_buffer.append((element['Gender'], element['Age'], element['Annual_Income'], element['Spending_Score'], element['CustomerID']))
            else:
                logging.info(f"Inserting new record for CustomerID {element['CustomerID']}...")
                self.insert_buffer.append((element['CustomerID'], element['Gender'], element['Age'], element['Annual_Income'], element['Spending_Score']))

            if len(self.insert_buffer) >= self.batch_size:
                self.write_batch_inserts()

            if len(self.update_buffer) >= self.batch_size:
                self.write_batch_updates()

        except mysql.connector.Error as e:
            logging.error(f"Error processing element {element}: {e}")

    def finish_bundle(self):
        try:
            if self.insert_buffer:
                self.write_batch_inserts()

            if self.update_buffer:
                self.write_batch_updates()
        except mysql.connector.Error as e:
            logging.error(f"Error finishing bundle: {e}")

        finally:
            self.cursor.close()
            self.conn.close()

    def write_batch_inserts(self):
        try:
            sql = "INSERT INTO customer (CustomerID, Gender, Age, Annual_Income, Spending_Score) VALUES (%s, %s, %s, %s, %s)"
            self.cursor.executemany(sql, self.insert_buffer)
            self.conn.commit()
            self.insert_buffer.clear()
        except mysql.connector.Error as e:
            logging.error(f"Error writing batch: {e}")
            self.conn.rollback()  # Rollback on error

    def write_batch_updates(self):
        try:
            update_sql = """
            UPDATE customer
            SET Gender = %s, Age = %s, Annual_Income = %s, Spending_Score = %s
            WHERE CustomerID = %s
            """
            self.cursor.executemany(update_sql, self.update_buffer)
            self.conn.commit()
            self.update_buffer.clear()
        except mysql.connector.Error as e:
            logging.error(f"Error writing update batch: {e}")
            self.conn.rollback()  # Rollback on error
