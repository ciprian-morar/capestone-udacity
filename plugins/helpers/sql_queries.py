fact_imm_table_drop = "DROP TABLE IF EXISTS imm_fact;"
person_table_drop = "DROP TABLE IF EXISTS person_dim;"
demo_table_drop = "DROP TABLE IF EXISTS demo_dim;"
time_table_drop = "DROP TABLE IF EXISTS time_dim;"

person_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS person_dim (
    person_id bigint,
    gender char(1),
    age int2,
    residence_country varchar,
    birth_country varchar,
    PRIMARY KEY(imm_fact_id));
""")

time_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS time_dim (
    time_id timestamp, 
    year int4,
    month int4,
    day int4,
    weekday varchar,
    hour int4,
    week int4,
    PRIMARY KEY(arrdate))
""")

demo_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS demo_dim (
    state_code char(2), 
    state varchar, 
    male_distribution char(6), 
    female_distribution char(6), 
    median_age int4,
    race varchar(20),
    PRIMARY KEY(state_code))
""")

imm_fact_table_create = ("""
CREATE TABLE IF NOT EXISTS imm_fact (
    imm_fact_id bigint identity(0,1),
    arrdate timestamp,
    person_id int,
    state_code char(2),
    mode_code int4,
    port_code varchar(6),
    visa_code int4,
     FOREIGN KEY(person_id)
          REFERENCES person_dim(person_id),
    FOREIGN KEY(arrdate)
          REFERENCES time_dim(arrdate),
    FOREIGN KEY(state_code)
          REFERENCES demo_dim(state_code),
    PRIMARY KEY(imm_fact_id));
""")

imm_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS imm_stage (
    imm_stage_id bigint identity(0,1),
    arrdate timestamp,
    admnum bigint,
    year int4,
    month int4,
    day int4,
    weekday varchar,
    hour int4,
    week int4,
    i94cit char(3),
    i94res char(3),
    i94bir int4,
    i94port char(3)
    i94mode int4,
    i94addr char(2),
    i94visa int4,
    gender char(1),
    PRIMARY KEY(imm_fact_id));
""")

country_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS country_stage (
    country_code char(3), 
    country varchar,   
    PRIMARY KEY(country_code))
""")

port_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS port_dim (
    port_code char(3), 
    city varchar, 
    state_code char(2),
    state_varchar,  
    PRIMARY KEY(port_code))
""")

visa_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS visa_dim (
    visa_code int4, 
    visa varchar, 
    PRIMARY KEY(visa_code))
""")

mode_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS mode_dim (
    mode_code int4, 
    mode varchar, 
    PRIMARY KEY(mode_code))
""")

time_dim_table_insert = ("""
        SELECT a.time_id,
                a.year,
                a.month,
                a.day,
                a.weekday,
                a.hour,
                a.week,
        FROM imm_stage a
    """)

person_dim_table_insert = ("""
        SELECT a.admnum as person_id,
                a.gender,
                a.i94bir as age,
                c.country as residence_country,
                b.country as birth_country,
        FROM imm_stage a
        LEFT JOIN country_stage as b ON a.i94cit=b.country_code
        LEFT JOIN country_stage as c ON a.i94res=c.country_code
""")

imm_fact_table_insert = ("""
        SELECT a.arrdate,
                a.admnum as person_id,
                a.i94addr as state_code,
                a.i94mode as mode_code,
                a.i94port as port_code,
                a.i94visa as visa_code,
        FROM imm_stage a
""")

