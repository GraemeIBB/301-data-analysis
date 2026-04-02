from data_load_and_clean.load_and_clean import load_and_clean_dataset1, load_and_clean_dataset2, load_and_clean_dataset3, load_and_clean_dataset4
from database.db_connector import connect_db, create_tables, insert_into_table1, insert_into_table2, insert_into_table3, insert_into_table4

def main():
    interprovincial_tourist_spending = load_and_clean_dataset1()
    international_tourist_spending = load_and_clean_dataset2()
    tourism_supply_demand = load_and_clean_dataset3()
    provincial_visitor_count = load_and_clean_dataset4()

    cnx = connect_db()
    cursor = cnx.cursor()
    create_tables(cursor)

    insert_into_table1(cursor, interprovincial_tourist_spending)
    insert_into_table2(cursor, international_tourist_spending)
    insert_into_table3(cursor, tourism_supply_demand)
    insert_into_table4(cursor, provincial_visitor_count)


if __name__ == "__main__":
    main()