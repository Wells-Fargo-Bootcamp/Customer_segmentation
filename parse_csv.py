import apache_beam as beam
import csv
import logging
from io import StringIO

class ParseCSV(beam.DoFn):
    
    def process(self, element):
        #logging.info(f"Processing element: {element}")

        expected_columns = 5

        reader = csv.reader(StringIO(element))
        next(reader)  # Skip header row
        for row in reader:
            #logging.info(f"Row read: {row}")
            row_with_none = [None] * expected_columns

            for i in range(min(len(row), expected_columns)):
                if row[i]:  # Only update if value is not empty
                    row_with_none[i] = row[i]

            
            try:
                # Validate and process each column
                customer_id = self.validate_customer_id(row_with_none[0])
                if not customer_id:
                    logging.warning(f"Missing or invalid CustomerID in row {row}")
                    yield beam.pvalue.TaggedOutput('malformed', row)
                    continue
                
                gender = self.validate_gender(row_with_none[1])
                age = self.validate_age(row_with_none[2])
                annual_income = self.validate_annual_income(row_with_none[3])
                spending_score = self.validate_spending_score(row_with_none[4])

                yield {
                    'CustomerID': customer_id,
                    'Gender': gender,
                    'Age': age,
                    'Annual_Income': annual_income,
                    'Spending_Score': spending_score,
                }
            except ValueError as err:
                logging.warning(f"Value error in row {row}: {err}")
                yield beam.pvalue.TaggedOutput('malformed', element)
            except Exception as e:
                logging.warning(f"Unexpected error in row {row}: {e}")
                yield beam.pvalue.TaggedOutput('malformed', element)

    def validate_customer_id(self, customer_id):
        if customer_id:
            try:
                customer_id = int(customer_id)
                return customer_id
            except ValueError:
                logging.warning("Invalid CustomerID: Not an integer")
                return None
        else:
            logging.warning("Missing CustomerID")
            return None

    def validate_gender(self, gender):
        if gender and gender in ["Male", "Female"]:
            return gender
        else:
            logging.warning(f"Invalid gender: {gender}")
            return None

    def validate_age(self, age):
        if age:
            try:
                age = int(age)
                if age > 0:
                    return age
                else:
                    logging.warning(f"Invalid age: {age}")
                    return None
            except ValueError:
                logging.warning(f"Invalid age format: {age}")
                return None
        return None

    def validate_annual_income(self, annual_income):
        if annual_income:
            try:
                annual_income = float(annual_income)
                if annual_income > 0:
                    return annual_income
                else:
                    logging.warning(f"Invalid annual income: {annual_income}")
                    return None
            except ValueError:
                logging.warning(f"Invalid annual income format: {annual_income}")
                return None
        return None

    def validate_spending_score(self, spending_score):
        if spending_score:
            try:
                spending_score = int(spending_score)
                if 1 <= spending_score <= 100:
                    return spending_score
                else:
                    logging.warning(f"Invalid spending score: {spending_score}")
                    return None
            except ValueError:
                logging.warning(f"Invalid spending score format: {spending_score}")
                return None
        return None
