# create tables.sql

class SqlCreateTablesMyproject:
    # CREATE TABLES
    create_schema_fuel = ("""CREATE SCHEMA IF NOT EXISTS fuel;
                            SET search_path TO fuel;""")


    create_city_table = ("""SET search_path TO fuel;
                        CREATE TABLE IF NOT EXISTS fuel.city (
                          "city_code" int,
                          "city_complete_code" int,
                          "city_name" varchar,
                          "city_size" varchar,
                          "city_state_code" varchar,
                          "city_capital" varchar,
                          "city_country_region" varchar,
                          "city" varchar
                        );      
                    """)
                                
                    
    create_state_table = ("""SET search_path TO fuel;
                        CREATE TABLE IF NOT EXISTS fuel.state (
                          "state_code" varchar,
                          "state_name" varchar,
                          "state_capital_code" int,
                          "state_capital_complete_code" int,
                          "state_country_region" varchar,
                          "state_capital_name" varchar,
                          "state_capital" varchar
                        );            
                    """)
 
    create_ipca_table = ("""SET search_path TO fuel;
                        CREATE TABLE IF NOT EXISTS fuel.ipca (
                          "ipca_tax" double precision,
                          "ipca_year_month" int
                    );
                """)

    create_usd_table = ("""SET search_path TO fuel;
                       CREATE TABLE IF NOT EXISTS fuel.usd (
                          "usd_avg_purchase" double precision,
                          "usd_avg_Sale" double precision,
                          "usd_year_month" int
                        );
                    """)
                    
    create_brent_table = ("""SET search_path TO fuel;
                         CREATE TABLE IF NOT EXISTS fuel.brent (
                          "brent_avg_value" double precision,
                          "brent_year_month" int
                        );
                    """)
    
    create_wage_table = ("""SET search_path TO fuel;
                        CREATE TABLE IF NOT EXISTS fuel.wage (
                          "wage_value" int,
                          "wage_year_month" int
                    );
                """)

    create_fuel_table = ("""SET search_path TO fuel;
                    CREATE TABLE IF NOT EXISTS fuel.fuel (
                      "fuel_state_code" varchar,
                      "fuel_city_code" int,
                      "fuel_product_name" varchar, 
                      "fuel_sales_price" double precision,
                      "fuel_year_month" int,
                      "fuel_purchase_price" double precision                      
                        );
                    """)
 
    fuel_create_all = [create_brent_table, create_city_table, create_fuel_table, create_ipca_table,\
                   create_state_table, create_usd_table, create_wage_table, create_schema_fuel]
