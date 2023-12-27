import psycopg2, psycopg2.extras, sys, json, os, time, datetime, shutil

my_dicts = {}


def cleanup_string(string):
    string = string.replace("'", "").replace('"', "")
    string = " ".join(string.split())
    string = string.upper()
    return string


def process_files(conn, cursor):
    valid_file_count = get_files()
    if valid_file_count < 3:
        num_of_files_missing = 3 - valid_file_count
        # print 'Not all Client files found'
        log_obj = {
            "conn": conn,
            "cursor": cursor,
            "task_type": "cf_file_import",
            "task_status": "failed",
        }
        log_obj["task_details"] = {
            "reason": str(num_of_files_missing) + " Client First files not found",
            "file_list": my_dicts["files"],
        }
        log_task_details(log_obj)
        return False
    return True


def get_files():
    my_dicts["files"] = {}
    file_directory = my_dicts["file_directory"]
    filenames = next(os.walk(file_directory))[2]
    le_file_found = False
    mc_file_found = False
    le_group_file_found = False
    valid_file_count = 0

    for file_name in filenames:
        fname = file_name.upper()
        if not fname.endswith(".DAT"):
            continue

        if fname.startswith("MC_LE"):
            my_dicts["files"]["legal_entities"] = file_directory + file_name
            if not le_file_found:
                valid_file_count += 1
                le_file_found = True
        if fname.startswith("MC_COV"):
            my_dicts["files"]["marketing_clients"] = file_directory + file_name
            if not mc_file_found:
                valid_file_count += 1
                mc_file_found = True
        if fname.startswith("GP_COV"):
            my_dicts["files"]["legal_entities_group"] = file_directory + file_name
            if not le_group_file_found:
                valid_file_count += 1
                le_group_file_found = True

    # print my_dicts['files']
    return valid_file_count


def import_files():
    """ First Pass thru Legal Entity File """
    pending_marketing_clients = {}
    with open(my_dicts["files"]["legal_entities"], 'r', encoding='latin-1') as f:
        for line in f:
            x = line.split("&|$")
            if len(x) < 32:
                continue

            country_iso_code = x[7].strip().upper()
            le_rmpm_code = x[8].strip().upper()

            if (
                country_iso_code == ""
                or country_iso_code not in my_dicts["country_lookup"]
                or le_rmpm_code == ""
                or le_rmpm_code in my_dicts["le_lookup"]
            ):
                continue

            pending_marketing_clients[x[1].strip().upper()] = ""
    """ Processing Marketing Client File """
    pending_le_groups = {}
    with open(my_dicts["files"]["marketing_clients"], 'r', encoding='latin-1') as f:
        for line in f:
            x = line.split("&|$")
            if len(x) < 30:
                continue

            marketing_client_id = x[1].strip().upper()
            le_group_rmpm_code = x[15].strip().upper()

            if (
                marketing_client_id not in pending_marketing_clients
                or le_group_rmpm_code == ""
            ):
                continue

            pending_marketing_clients[marketing_client_id] = le_group_rmpm_code
            pending_le_groups[le_group_rmpm_code] = ""
    """ Processing Client Group File """
    with open(my_dicts["files"]["legal_entities_group"], 'r', encoding='latin-1') as f:
        for line in f:
            x = line.split("&|$")
            if len(x) < 29:
                continue

            le_group_rmpm_code = x[2].strip().upper()
            le_group_name = cleanup_string(x[3])

            if le_group_rmpm_code not in pending_le_groups:
                continue

            pending_le_groups[le_group_rmpm_code] = le_group_name
    """ Second/Final Pass thru Legal Entity File """
    with open(my_dicts["files"]["legal_entities"], 'r', encoding='latin-1') as f:
        for line in f:
            x = line.split("&|$")
            if len(x) < 32:
                continue

            country_iso_code = x[7].strip().upper()
            le_rmpm_code = x[8].strip().upper()

            if (
                country_iso_code == ""
                or country_iso_code not in my_dicts["country_lookup"]
                or le_rmpm_code == ""
                or le_rmpm_code in my_dicts["le_lookup"]
            ):
                continue

            le_name = cleanup_string(x[5])
            marketing_client_code = x[1].strip().upper()
            country_code = my_dicts["country_lookup"][country_iso_code]

            my_dicts["legal_entities"]["rmpm_le_code"].append(le_rmpm_code)
            my_dicts["legal_entities"]["le_name"].append(le_name)
            my_dicts["legal_entities"]["country_code"].append(country_code)
            my_dicts["legal_entities"]["branch"].append("")
            my_dicts["legal_entities"]["radix"].append("")
            my_dicts["legal_entities"]["le_code"].append(my_dicts["next_le_code"])
            my_dicts["le_lookup"][le_rmpm_code] = my_dicts["next_le_code"]
            my_dicts["next_le_code"] = int(my_dicts["next_le_code"]) + 1

            if marketing_client_code not in pending_marketing_clients:
                my_dicts["legal_entities"]["le_group_code"].append(
                    my_dicts["independent_group_code"]
                )
            elif pending_marketing_clients[marketing_client_code] == "":
                my_dicts["legal_entities"]["le_group_code"].append(
                    my_dicts["independent_group_code"]
                )
            elif (
                pending_le_groups[pending_marketing_clients[marketing_client_code]]
                == ""
            ):
                my_dicts["legal_entities"]["le_group_code"].append(
                    my_dicts["independent_group_code"]
                )
            else:
                le_group_rmpm_code = pending_marketing_clients[marketing_client_code]
                if le_group_rmpm_code in my_dicts["le_group_lookup"]:
                    my_dicts["legal_entities"]["le_group_code"].append(
                        my_dicts["le_group_lookup"][le_group_rmpm_code]
                    )
                else:
                    my_dicts["legal_entities_group"]["le_group_rmpm_id"].append(
                        le_group_rmpm_code
                    )
                    my_dicts["legal_entities_group"]["le_group_name"].append(
                        pending_le_groups[le_group_rmpm_code]
                    )
                    my_dicts["legal_entities_group"]["le_group_code"].append(
                        my_dicts["next_le_group_code"]
                    )
                    my_dicts["le_group_lookup"][le_group_rmpm_code] = my_dicts[
                        "next_le_group_code"
                    ]
                    my_dicts["legal_entities"]["le_group_code"].append(
                        my_dicts["next_le_group_code"]
                    )
                    my_dicts["next_le_group_code"] = (
                        int(my_dicts["next_le_group_code"]) + 1
                    )


def setup_process(conn, cursor):
    master_list = ["countries", "legal_entities", "legal_entities_group"]
    master_init_objects = {
        "legal_entities": {
            "rmpm_le_code": [],
            "le_code": [],
            "le_name": [],
            "country_code": [],
            "le_group_code": [],
            "branch": [],
            "radix": [],
        },
        "legal_entities_group": {
            "le_group_code": [100001],
            "le_group_name": ["INDEPENDENT ENTITIES (Does not belong to RMPM Group)"],
            "le_group_rmpm_id": ["INDENT"],
        },
    }
    shortlisted_country_list = [
        "AT",
        "BG",
        "CH",
        "CZ",
        "DE",
        "DK",
        "ES",
        "GB",
        "HU",
        "IE",
        "NL",
        "NO",
        "PT",
        "RO",
        "RU",
        "SE",
        "SK",
        "BH",
        "AE",
        "QA",
        "KW",
        "SA",
        "ZA",
    ]
    my_dicts["independent_group_code"] = 100001

    for master in master_list:
        cursor.execute("SELECT * FROM checkpoint where cp_type = '%s'" % master)
        records = cursor.fetchall()
        if len(records) > 0:
            for rec in records:
                my_dicts[master] = json.loads(rec["cp_details"])
        else:
            my_dicts[master] = master_init_objects[master].copy()

    my_dicts["country_lookup"] = {}
    for idx, iso_code in enumerate(my_dicts["countries"]["country_iso_code"]):
        if iso_code in shortlisted_country_list:
            my_dicts["country_lookup"][iso_code] = my_dicts["countries"][
                "country_code"
            ][idx]

    highest_le_code = 300000
    my_dicts["le_lookup"] = {}
    for idx, rmpm_le_code in enumerate(my_dicts["legal_entities"]["rmpm_le_code"]):
        my_dicts["le_lookup"][rmpm_le_code] = my_dicts["legal_entities"]["le_code"][idx]
        highest_le_code = (
            highest_le_code
            if my_dicts["legal_entities"]["le_code"][idx] < highest_le_code
            else my_dicts["legal_entities"]["le_code"][idx]
        )
    my_dicts["next_le_code"] = int(highest_le_code) + 1

    highest_le_group_code = 100001
    my_dicts["le_group_lookup"] = {}
    for idx, le_group_rmpm_id in enumerate(
        my_dicts["legal_entities_group"]["le_group_rmpm_id"]
    ):
        my_dicts["le_group_lookup"][le_group_rmpm_id] = my_dicts[
            "legal_entities_group"
        ]["le_group_code"][idx]
        highest_le_group_code = (
            highest_le_group_code
            if my_dicts["legal_entities_group"]["le_group_code"][idx]
            < highest_le_group_code
            else my_dicts["legal_entities_group"]["le_group_code"][idx]
        )
    my_dicts["next_le_group_code"] = int(highest_le_group_code) + 1

    cursor.execute("SELECT * FROM app_config")
    records = cursor.fetchall()

    for rec in records:
        file_directory = rec["client_files_dir"].strip()
        if not os.path.isdir(file_directory):
            log_obj = {
                "conn": conn,
                "cursor": cursor,
                "task_type": "cf_file_import",
                "task_status": "failed",
            }
            log_obj["task_details"] = {
                "reason": file_directory + " directory not found",
                "file_list": my_dicts["files"],
            }
            log_task_details(log_obj)
            return False
        my_dicts["file_directory"] = file_directory

    if len(records) == 0 or records[0]["client_files_dir"].strip() == "":
        log_obj = {
            "conn": conn,
            "cursor": cursor,
            "task_type": "cf_file_import",
            "task_status": "failed",
        }
        log_obj["task_details"] = {
            "reason": "App Config Not Setup for File Import",
            "file_list": my_dicts["files"],
        }
        log_task_details(log_obj)
        return False

    return True


def update_db(conn, cursor):
    cursor.execute("SELECT * FROM checkpoint where cp_type = 'legal_entities_group'")
    records = cursor.fetchall()
    if len(records) > 0:
        print("Updating LE Group Record")
        cursor.execute(
            "UPDATE checkpoint SET cp_details = '%s' WHERE cp_type = 'legal_entities_group'"
            % json.dumps(my_dicts["legal_entities_group"])
        )
    else:
        print("Creating LE Group Record")
        cursor.execute(
            "INSERT INTO checkpoint (cp_type, serial_no, cp_details, last_modified_on) VALUES (%s, %s, %s, %s)",
            (
                "legal_entities_group",
                1,
                json.dumps(my_dicts["legal_entities_group"]),
                datetime.datetime.now(),
            ),
        )
    conn.commit()

    cursor.execute("SELECT * FROM checkpoint where cp_type = 'legal_entities'")
    records = cursor.fetchall()
    if len(records) > 0:
        print("Updating LE Record")
        cursor.execute(
            "UPDATE checkpoint SET cp_details = '%s' WHERE cp_type = 'legal_entities'"
            % json.dumps(my_dicts["legal_entities"])
        )
    else:
        print("Creating LE Record")
        cursor.execute(
            "INSERT INTO checkpoint (cp_type, serial_no, cp_details, last_modified_on) VALUES (%s, %s, %s, %s, %s)",
            (
                "legal_entities",
                1,
                json.dumps(my_dicts["legal_entities"]),
                datetime.datetime.now(),
            ),
        )
    conn.commit()


def log_task_details(log_obj):
    log_obj["cursor"].execute(
        "INSERT INTO task_execution_details (task_type, task_status, task_dt, task_invoked_by, task_details, task_execution_other_details, last_modified_on) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            log_obj["task_type"],
            log_obj["task_status"],
            datetime.datetime.now(),
            "system",
            json.dumps(log_obj["task_details"]),
            "{}",
            datetime.datetime.now(),
        ),
    )
    log_obj["conn"].commit()
    print(log_obj["task_status"] + " (" + log_obj["task_details"]["reason"] + ")")


def move_files():
    try:
        file_directory = my_dicts["file_directory"]
        archive_directory = file_directory + "archive/"
        if not os.path.isdir(archive_directory):
            os.makedirs(archive_directory)
        filenames = next(os.walk(file_directory))[2]
        for file_name in filenames:
            shutil.copy(file_directory + file_name, archive_directory)
        for file_name in filenames:
            os.remove(file_directory + file_name)
    except:
        return False
    return True


def main():
    try:

        conn_string = "host='localhost' dbname='eps3db' user='eps3admin' password='c3tech@135'"

        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        my_dicts["files"] = {}
        all_ok = setup_process(conn, cursor)
        if not all_ok:
            return

        all_ok = process_files(conn, cursor)
        if not all_ok:
            return

        # start_time = time.time()
        import_files()
        # print("--- %s seconds to import & process data from files ---" % (time.time() - start_time))

        # start_time = time.time()
        update_db(conn, cursor)

        all_ok = move_files()
        if not all_ok:
            log_obj = {
                "conn": conn,
                "cursor": cursor,
                "task_type": "cf_file_import",
                "task_status": "partially complete",
            }
            log_obj["task_details"] = {
                "reason": "Client First Data Imported, but Files could not be moved",
                "file_list": my_dicts["files"],
            }
            log_task_details(log_obj)
            return

        log_obj = {
            "conn": conn,
            "cursor": cursor,
            "task_type": "cf_file_import",
            "task_status": "complete",
        }
        log_obj["task_details"] = {
            "reason": "Client First Data Imported",
            "file_list": my_dicts["files"],
        }
        log_task_details(log_obj)
        # print("--- %s seconds to Update DB ---" % (time.time() - start_time))
    except psycopg2.OperationalError as e:
        print("DB Error ",e)
    except Exception as e:

        print("Error ",e)

if __name__ == "__main__":
    main()
