import utils.big_query as bq
import json
import config

from datetime import datetime


def update_hh_data_staging_table(creds, sites):
    with open("sql/tbl_mds_supplies_and_hh_data.sql", "r") as sql:
        query = bq.Sql(sql.read(), "mds_supplies_with_hh_data")

    query.format_query(config.sites)

    settings = {'dataset': 'u_archie',
                'table_name': 'tbl_mds_supplies_and_hh_data',
                'write_method': 'WRITE_TRUNCATE'}

    price_test = input("Are you sure you need to re-populate the staging"
                       "table? It's very expensive... (y/n)")

    if price_test.lower() == "y":
        query.execute_on_big_query(creds.bq_client, config=settings)
    else:
        print("Exiting")
        quit()


def generate_file_name():
    now = datetime.now()
    return now.strftime("mds_hh_data_%Y%m%d_%H%M%S.txt")


def format_file_contents(df):
    re_orientated_df = df.to_json(index=False, orient="table")
    json_data = json.loads(re_orientated_df)

    total_rows = json_data['data'][0]['total_rows']
    consumption_sum = json_data['data'][0]['sum_floored_consumption']

    header = json_data['data'][0]['header'] + "\n"
    footer = "T" + total_rows + consumption_sum

    records = []

    for record in json_data['data']:
        records.append(record['record']
                       + (" " * 18).join(record['output_format'])
                       + " " * 18 + "\n")

    output = [header] + records + [footer]

    return output


def get_mds_hh_data_file(creds, start_date, end_date):
    with open("sql/get_parsed_hh_data.sql", "r") as sql:
        query = bq.Sql(sql.read(), "get_parsed_hh_data")

    settings = {'dataset': 'u_archie',
                'table_name': 'tbl_mds_supplies_and_hh_data_history_table',
                'write_method': 'WRITE_APPEND',
                'partitioned_table': 'TRUE'}

    query.format_query([start_date, end_date])
    df = query.execute_on_big_query(creds.bq_client, config=settings,
                                    return_result=True)

    output = format_file_contents(df)

    with open(f"outputs/{generate_file_name()}", "w", newline="\n") as file:
        for row in output:
            file.write(row)

    return output


def output_test(output):
    for i, line in enumerate(output):
        if line[0] == "D":
            message = f"Record at position {i} != 1520 characters"
            assert len(line) == 1521, message  # 1521 to account for new line
        elif line[0] == "H":
            message = f"Header != 15 characters"
            assert len(line) == 16, message
        elif line[0] == "F":
            message = f"Footer != 15 characters"
            assert len(line) == 22, message
    print("All records are the correct character lengths")


def main(run_staging_table=False):
    creds = bq.Credentials(config.cred_path, "portfolio-analytics-nonprod")

    if run_staging_table:
        update_hh_data_staging_table(creds, config.sites)

    output = get_mds_hh_data_file(creds, config.start_date, config.end_date)
    output_test(output)


if __name__ == "__main__":
    main(run_staging_table=config.run_staging_table)
