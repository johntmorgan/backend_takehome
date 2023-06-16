from flask import Flask
import csv
import re
app = Flask(__name__)

@app.route('/trigger_etl')
def trigger_etl():
    etl()
    return {"message": "ETL process started"}, 200

def etl():
    # Load CSV files
    compounds = load_compounds()
    users = load_users()
    exps = load_experiments()

    # Process files to derive features
    calc_user_experiments(users, exps)
    calc_avg_user_exp_time(users, exps)
    calc_common_user_compound(users, exps, compounds)

    print_tables(users, compounds, exps)
    pass

def load_compounds():
    compounds = []
    with open('data/compounds.csv', 'r') as f:
      compounds_csv = csv.reader(f)
      for compound_row in compounds_csv:
          compounds.append(compound_row)
    for i, row in enumerate(compounds):
        for j, entry in enumerate(row):
            row[j] = re.sub(r"[\n\t\s]*", "", entry)
        if i > 0:
            row[0] = int(row[0])
    return compounds

def load_users():
    users = []
    with open('data/users.csv', 'r') as f:
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

def load_experiments():
    exps = []
    with open('data/user_experiments.csv', 'r') as f:
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

def calc_avg_user_exp_time(users, exps):
    users[0].append("total_run_time")
    for user in users[1:]:
        user.append(0)
    for exp in exps[1:]:
        users[int(exp[1])][5] += int(exp[3])
    users[0].append("avg_run_time")
    for user in users[1:]:
        user.append(float(user[5]) / float(user[4]))

# How to break ties? Save as array? Arbitrarily pick first compound?
def calc_common_user_compound(users, exps, compounds):
    users[0].append("compound_counts")
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

def print_tables(users, compounds, exps):
    for user_row in users:
        print(user_row)
    for compound_row in compounds:
        print(compound_row)
    for exp_row in exps:
        print(exp_row)
