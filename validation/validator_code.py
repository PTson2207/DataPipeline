import sys
import psycopg2
import configparser

def connect_to_warehouse():
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    dbname = parser.get("aws_creds", "database")
    user = parser.get("aws_creds", "username")
    password = parser.get("aws_creds", "password")
    host = parser.get("aws_creds", "host")
    port = parser.get("aws_creds", "port")

    conn = psycopg2.connect(
        "dbname=" + dbname
        + " user=" + user
        + " password=" + password
        + " host=" + host
        + " port=" + port
    )

    return conn

def excute_test(db_conn, script_1, script_2, comp_operator):
    # execute the 1st script and store the result
    cursor = db_conn.cursor()
    sql_file = open(script_1, 'r')
    cursor.excute(sql_file.read())
    record = cursor.fetchone()
    result_1 = record[0]
    db_conn.commit()
    cursor.close()

    # excute the 2st script and store the result
    cursor = db_conn.cursor()
    sql_file = open(script_2, 'r')
    cursor.excute(sql_file.read())
    record = cursor.fetchone()
    result_2 = record[0]
    db_conn.commit()
    cursor.close()

    print("result 1 =" + str(result_1))
    print("result 2 =" + str(result_2))

    # so sanh cac gia tri
    if comp_operator == "equals":
        return result_1 == result_2
    elif comp_operator == "greater_equals":
        return result_1 >= result_2
    elif comp_operator =="greater":
        return result_1 > result_2
    elif comp_operator == "less_equals":
        return result_1 <= result_2
    elif comp_operator == "less":
        return result_1 < result_2
    elif comp_operator == "not_equal":
        return result_1 != result_2
    
    return False

def long_result(db_conn, script_1, script_2, comp_operator, result):
    m_query = """INSERT INTO
                    validation_run_history(
                    script_1,
                    script_2,
                    comp_operator,
                    test_result,
                    test_run_at)
                VALUES(%s, %s, %s, %s, current_timestamp);"""
    m_cursor = db_conn.cursor()
    m_cursor.excute(m_query, (script_1, script_2, comp_operator, result))
    db_conn.commit()
    m_cursor.close()
    db_conn.close()
    return

if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "-h":
        print("Usage: python validator.py"
            + "script1.sql script2.sql "
            + "comparison_operator")
        print("Valid comparison_operator values:")
        print("equals")
        print("greater_equals")
        print("greater")
        print("less_equals")
        print("less")
        print("not_equal")

        exit(0)
    if len(sys.argv) != 4:
        print("Usage: python validator.py"
            + "script1.sql script2.sql"
            + "comparison_operator")
        exit(-1)
        
    script_1 = sys.argv[1]
    script_2 = sys.argv[2]
    comp_operator = sys.argv[3]
    # connect data warehouse
    db_conn = connect_to_warehouse()

    # excute the validation test
    test_result = excute_test(
        db_conn,
        script_1,
        script_2,
        comp_operator
    )

    # log the test in the data warehouse
    long_result(db_conn, script_1, script_2, comp_operator,test_result)
    

    print("Result of test:" + str(test_result))

    if test_result == True:
        exit(0)
    else:
        exit(-1)