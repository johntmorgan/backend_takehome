from sqlalchemy import *
from flask import Flask
import csv
import re
app = Flask(__name__)

@app.route('/trigger_etl')
def trigger_etl():
    etl()
    return {"message": "ETL process started"}, 200

def etl():
    # Load CSV files into arrays for processing
    users = load_users()
    exps = load_experiments()
    compounds = load_compounds()

    # Process files to derive requested features
    calc_user_experiments(users, exps)
    calc_avg_user_exp_time(users, exps)
    calc_common_user_compound(users, exps, compounds)

    # Load files into postgres
    db = setup_db()
    create_tables(db)
    load_to_database(users, exps, compounds, db)
    pass

def load_users():
    users = []
    with open('./data/users.csv', 'r') as f:
        users_csv = csv.reader(f)
        next(users_csv)
        for user_row in users_csv:
            users.append(user_row)
    for i, row in enumerate(users):
        for j, entry in enumerate(row):
            row[j] = re.sub(r"[\n\t\s]*", "", entry)
        if i > 0:
            row[0] = int(row[0])
    return users

def load_compounds():
    compounds = []
    with open('./data/compounds.csv', 'r') as f:
      compounds_csv = csv.reader(f)
      for compound_row in compounds_csv:
          compounds.append(compound_row)
    for i, row in enumerate(compounds):
        for j, entry in enumerate(row):
            row[j] = re.sub(r"[\n\t\s]*", "", entry)
        if i > 0:
            row[0] = int(row[0])
    return compounds

def load_experiments():
    exps = []
    with open('./data/user_experiments.csv', 'r') as f:
        exps_csv = csv.reader(f)
        for exp_row in exps_csv:
            exps.append(exp_row)
    for i, row in enumerate(exps):
        for j, entry in enumerate(row):
            row[j] = re.sub(r"[\n\t\s]*", "", entry)
        if i > 0:
            row[0] = int(row[0])
    return exps

def calc_user_experiments(users, exps):
    users[0].append("experiment_count")
    for user in users[1:]:
        user.append(0)
    for exp in exps[1:]:
        users[int(exp[1])][4] += 1

# Assumed "experiments amount" in instructions refers to experiment_run_time.
def calc_avg_user_exp_time(users, exps):
    users[0].append("total_run_time")
    for user in users[1:]:
        user.append(0)
    for exp in exps[1:]:
        users[int(exp[1])][5] += int(exp[3])
    users[0].append("avg_run_time")
    for user in users[1:]:
        user.append(float(user[5]) / float(user[4]))

# Unsure how to break ties in most experimented compound from instructions.
# Decided to merge all most-used compounds in a semicolon-delimited string,
# as in the input data, and display the chemical formula rather than the
# generic compound letter name.
def calc_common_user_compound(users, exps, compounds):
    users[0].append("favorite_compound")
    for user in users[1:]:
        user.append({})
    for exp in exps[1:]:
        compound_ids = exp[2].split(";")
        freq = users[int(exp[1])][7]
        for cid in compound_ids:
            if cid in freq:
                freq[cid] += 1
            else:
                freq[cid] = 1
    for user in users[1:]:
        top_freq = 0
        most_freq = []
        freqs = user[7]
        for comp_num in freqs:
            if freqs[comp_num] > top_freq:
                top_freq = freqs[comp_num]
                most_freq = [compounds[int(comp_num)][2]]
            elif freqs[comp_num] == top_freq:
                most_freq.append(compounds[int(comp_num)][2])
        user[7] = ";".join(most_freq)

def setup_db():
    db_name = 'db'
    db_user = 'postgres'
    db_pass = 'secret'
    db_host = 'db'
    db_port = '5432'

    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host,
        db_port, db_name)
    db = create_engine(db_string, echo=True)
    return db

# Writing raw SQL, as have not worked in depth with Python ORMs before
def create_tables(db):
    with db.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS users;"))
        conn.execute(text("CREATE TABLE users (" +
            "user_id SERIAL PRIMARY KEY,"+
            "name VARCHAR(20)," +
            "email VARCHAR(50)," +
            "signup_date DATE," +
            "experiment_count INT," +
            "total_run_time INT," +
            "avg_run_time DECIMAL," +
            "favorite_compound VARCHAR(50));"))
        conn.commit()

        conn.execute(text("DROP TABLE IF EXISTS user_experiments;"))
        conn.execute(text("CREATE TABLE user_experiments (" +
            "experiment_id SERIAL PRIMARY KEY," +
            "user_id INT," +
            "experiment_compound_ids VARCHAR(20)," +
            "experiment_run_time INT);"))
        conn.commit()

        conn.execute(text("DROP TABLE IF EXISTS compounds;"))
        conn.execute(text("CREATE TABLE compounds (" +
            "compound_id SERIAL PRIMARY KEY," +
            "compound_name VARCHAR(20)," +
            "compound_structure VARCHAR(50));"))
        conn.commit()

def load_to_database(users, exps, compounds, db):
    with db.connect() as conn:
        for user_row in users[1:]:
            conn.execute(text('INSERT INTO users(user_id, name, email, ' +
                'signup_date, experiment_count, total_run_time, ' +
                'avg_run_time, favorite_compound) VALUES (' +
                 '\'' + str(user_row[0]) + '\'' + ',' +
                 '\'' + str(user_row[1]) + '\'' + ',' +
                 '\'' + str(user_row[2]) + '\'' + ',' +
                 '\'' + str(user_row[3]) + '\'' + ',' +
                 '\'' + str(user_row[4]) + '\'' + ',' +
                 '\'' + str(user_row[5]) + '\'' + ',' +
                 '\'' + str(user_row[6]) + '\'' + ',' +
                 '\'' + str(user_row[7]) + '\'' + ');'))
            conn.commit()

        for exp_row in exps[1:]:
            conn.execute(text('INSERT INTO user_experiments(experiment_id, ' +
                'user_id, experiment_compound_ids, experiment_run_time) ' +
                'VALUES (' +
                 '\'' + str(exp_row[0]) + '\'' + ',' +
                 '\'' + str(exp_row[1]) + '\'' + ',' +
                 '\'' + str(exp_row[2]) + '\'' + ',' +
                 '\'' + str(exp_row[3]) + '\'' + ');'))
            conn.commit()

        for comp_row in compounds[1:]:
            conn.execute(text('INSERT INTO compounds(compound_id, ' +
                'compound_name, compound_structure) VALUES (' +
                 '\'' + str(comp_row[0]) + '\'' + ',' +
                 '\'' + str(comp_row[1]) + '\'' + ',' +
                 '\'' + str(comp_row[2]) + '\'' + ');'))
            conn.commit()

# Only querying users table, as it contains all the derived features
# Output all users table data for inspection, not sure if should limit to
# derived feature columns
@app.route('/query_db')
def query_db():
    db = setup_db()
    with db.connect() as conn:
        results = conn.execute(text('SELECT * FROM users'))
        results = [tuple(row) for row in results]
    return {"Data from Postgres users table with all derived features": results}, 200