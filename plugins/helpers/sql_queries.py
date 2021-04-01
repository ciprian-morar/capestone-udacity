fact_imm_table_drop = "DROP TABLE IF EXISTS imm_fact;"
person_table_drop = "DROP TABLE IF EXISTS person_dim;"
demo_table_drop = "DROP TABLE IF EXISTS demo_dim;"
time_table_drop = "DROP TABLE IF EXISTS time_dim;"

person_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS person_dim (
    person_id int,
    gender char(1),
    age int2,
    residence_country varchar,
    birth_country varchar,
    PRIMARY KEY(person_id));
""")

time_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS time_dim (
    time_id int, 
    year int4,
    month int4,
    day int4,
    weekday varchar,
    hour int4,
    week int4,
    PRIMARY KEY(time_id))
""")

demo_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS demo_dim (
    state_code text, 
    state text, 
    male_distribution text, 
    female_distribution text, 
    median_age int4,
    race text,
    PRIMARY KEY(state_code))
""")

imm_fact_table_create = ("""
CREATE TABLE IF NOT EXISTS imm_fact (
    imm_fact_id bigint identity(0,1),
    arrdate int,
    person_id int,
    state_code char(2),
    mode_code int4,
    port_code varchar(6),
    visa_code int4,
    PRIMARY KEY(imm_fact_id));
""")

imm_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS imm_stage (
    admnum int,
    arrdate int,
    year int4,
    month int4,
    day int4,
    weekday text,
    hour int4,
    week int4,
    i94cit int4,
    i94res int4,
    i94bir int4,
    i94port text,
    i94mode int4,
    i94addr text,
    i94visa int4,
    gender text,
    PRIMARY KEY(admnum));
""")

country_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS country_stage (
    country_code int4, 
    country text,   
    PRIMARY KEY(country_code))
""")

port_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS port_dim (
    port_code text, 
    city text,
    PRIMARY KEY(port_code))
""")

visa_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS visa_dim (
    visa_code int4, 
    visa text, 
    PRIMARY KEY(visa_code))
""")

mode_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS mode_dim (
    mode_code int4, 
    mode text, 
    PRIMARY KEY(mode_code))
""")

time_dim_table_insert = ("""
        SELECT a.arrdate as time_id,
                a.year,
                a.month,
                a.day,
                a.weekday,
                a.hour,
                a.week
        FROM imm_stage a
    """)

person_dim_table_insert = ("""
        SELECT abs(a.admnum) as person_id,
                a.gender,
                a.i94bir as age,
                c.country as residence_country,
                b.country as birth_country
        FROM imm_stage a
        LEFT JOIN country_stage as b ON a.i94cit=b.country_code
        LEFT JOIN country_stage as c ON a.i94res=c.country_code
""")

imm_fact_table_insert = ("""
        SELECT a.arrdate,
                abs(a.admnum) as person_id,
                a.i94addr as state_code,
                a.i94mode as mode_code,
                a.i94port as port_code,
                a.i94visa as visa_code
        FROM imm_stage a
""")

