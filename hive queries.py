from pyhive import hive

def execute_hive_query(query):
    # Connect to Hive
    conn = hive.Connection(host="localhost", port=10000, username="yasser")

    # Create a cursor
    cursor = conn.cursor()

    try:
        # Execute the Hive query
        cursor.execute(query)

        # Fetch the results
        results = cursor.fetchall()

        return results

    finally:
        # Close the connection
        conn.close()

def detect_fraud():
    # Query 1: Identifying Unusual Transaction Amounts
    query1 = """
    SELECT *
    FROM transactions
    WHERE amount > (SELECT AVG(amount) + 3 * STDDEV(amount) FROM transactions);
    """
    
    # Query 2: Unusual Frequency of Transactions
    query2 = """
    SELECT card_number, COUNT(*) as transaction_count
    FROM transactions
    GROUP BY card_number
    HAVING transaction_count > (SELECT AVG(transaction_count) + 3 * STDDEV(transaction_count) FROM
    (SELECT card_number, COUNT(*) as transaction_count FROM transactions GROUP BY card_number) t);
    """
    
    # Query 3: Detecting Geographic Anomalies
    query3 = """
    SELECT *
    FROM transactions
    WHERE (city, state) NOT IN (SELECT city, state FROM valid_locations);
    """
    
    # Query 4: Time-based Anomalies
    query4 = """
    SELECT *
    FROM transactions
    WHERE HOUR(timestamp) < 6 OR HOUR(timestamp) > 22;
    """
    
    # Query 5: Unusual Transaction Types
    query5 = """
    SELECT *
    FROM transactions
    WHERE transaction_type NOT IN ('purchase', 'withdrawal', 'transfer');
    """

    # Execute the queries
    results1 = execute_hive_query(query1)
    results2 = execute_hive_query(query2)
    results3 = execute_hive_query(query3)
    results4 = execute_hive_query(query4)
    results5 = execute_hive_query(query5)

    # Check for fraud based on query results
    if results1 or results2 or results3 or results4 or results5:
        print("Potential fraud detected! Check the results for details.")
    else:
        print("No potential fraud detected.")

if __name__ == "__main__":
    detect_fraud()
