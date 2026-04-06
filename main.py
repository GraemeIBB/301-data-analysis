from data_load_and_clean.load_and_clean import load_and_clean_dataset1, load_and_clean_dataset2, load_and_clean_dataset3, load_and_clean_dataset4
from database.db_connector import connect_db, create_tables, insert_into_table1, insert_into_table2, insert_into_table3, insert_into_table4, drop_tables
from analysis.regression.regression import predict_quarterly_international_tourist_arrivals

from analysis.eda.spend_per_arrival_eda import run_eda
from analysis.spend_per_arrival_analysis import QuestionTwo

def is_empty(table, cursor):
    sql = f"SELECT * FROM {table} LIMIT 10"
    cursor.execute(sql)

    if len(cursor.fetchall()) > 1:
        return False
    else:
        return True


def main():
    # interprovincial_tourist_spending = load_and_clean_dataset1()
    # international_tourist_spending = load_and_clean_dataset2()
    # tourism_supply_demand = load_and_clean_dataset3()
    # provincial_visitor_count = load_and_clean_dataset4()

    cnx = connect_db()
    cursor = cnx.cursor()

    # drop_tables('interprovincial_tourist_spending', 'international_tourist_spending', 
    #             'tourism_supply_demand', 'provincial_visitor_count', cursor)
    
    # create_tables(cursor)

    # if is_empty('interprovincial_tourist_spending', cursor):
    #     insert_into_table1(cursor, interprovincial_tourist_spending)
    
    # if is_empty('international_tourist_spending', cursor):
    #     insert_into_table2(cursor, international_tourist_spending)
    
    # if is_empty('tourism_supply_demand', cursor):
    #     insert_into_table3(cursor, tourism_supply_demand)
    
    # if is_empty('provincial_visitor_count', cursor):
    #     insert_into_table4(cursor, provincial_visitor_count)

    # cnx.commit()

    # Run Q2 analysis
    run_eda()
    q2 = QuestionTwo()
    q2.answer()
    q2.shift()


    cursor.execute("SELECT * FROM provincial_visitor_count LIMIT 10")
    for row in cursor.fetchall():
        print(row)

    # cursor.execute("SELECT * FROM tourism_supply_demand LIMIT 10")
    # for row in cursor.fetchall():
    #     print(row)

    
    # cursor.execute("SELECT * FROM international_tourist_spending LIMIT 10")
    # for row in cursor.fetchall():
    #     print(row)

    # cursor.execute("SELECT * FROM interprovincial_tourist_spending LIMIT 10")
    # for row in cursor.fetchall():
    #     print(row)

    
    predict_quarterly_international_tourist_arrivals(cursor)

    
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()