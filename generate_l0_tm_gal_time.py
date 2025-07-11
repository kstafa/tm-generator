import sqlite3
import json
from datetime import datetime, timezone, timedelta
import os

def gnss_to_tai_timestamp(wn, tow_ps, leap_sec):
    """
    Convertit un Week Number (WN) et Time of Week (TOW) en timestamp TAI ISO 8601.
    """
    gps_epoch = datetime(1980, 1, 6, tzinfo=timezone.utc)
    tow_seconds = tow_ps / 1e12
    gps_time = gps_epoch + timedelta(weeks=wn, seconds=tow_seconds)
    tai_offset = 19 + leap_sec  # GPS→TAI offset + leap
    tai_time = gps_time + timedelta(seconds=tai_offset)
    return tai_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def convert_gnss_to_json(db_path, output_path):
    # Vérification du fichier
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"Base de données introuvable : {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Affiche la liste des tables pour vérifier que GNSS_TIME existe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cursor.fetchall()]
    print("Tables détectées dans la base :", tables)

    # Si la table GNSS_TIME n'est pas présente, on arrête tout de suite
    if "GNSS_TIME" not in tables:
        raise sqlite3.OperationalError("Table GNSS_TIME introuvable dans la base.")

    # On récupère les champs utiles
    cursor.execute("""
        SELECT rx_time_nsec, wn, tow_ps, leap_sec
        FROM GNSS_TIME
        ORDER BY rx_time_nsec
    """)
    rows = cursor.fetchall()
    conn.close()

    # Prépare la structure JSON
    json_data = {
        "mib_ref": "1.0.0",
        "payload_name": "/GENERIC/SWING/PAYLOAD/AQUILA",
        "packet_name": "MAQUI_TM_003025GAL",
        "metadata": {},
        "parameters": [
            {"name": "TM_packet_time_TAI",   "pos": 0,  "metadata": {}},
            {"name": "SYNC_PATTERN_1",       "pos": 1,  "metadata": {}},
            {"name": "SYNC_PATTERN_2",       "pos": 2,  "metadata": {}},
            {"name": "PACKET_ID",            "pos": 3,  "metadata": {}},
            {"name": "PACKET_LENGTH",        "pos": 4,  "metadata": {}},
            {"name": "MAQUI_AM_GARXTIMENS",  "pos": 5,  "metadata": {}},
            {"name": "RESERVED_1",           "pos": 6,  "metadata": {}},
            {"name": "RESERVED_2",           "pos": 7,  "metadata": {}},
            {"name": "MAQUI_AM_GAWKNUMB",    "pos": 8,  "metadata": {}},
            {"name": "MAQUI_AM_GAPICOS",     "pos": 9,  "metadata": {}},
            {"name": "RESERVED_3",           "pos": 10, "metadata": {}},
            {"name": "RESERVED_4",           "pos": 11, "metadata": {}},
            {"name": "RESERVED_5",           "pos": 12, "metadata": {}},
            {"name": "MAQUI_AM_GALEAPSEC",   "pos": 13, "metadata": {}}
        ],
        "data": []
    }

        # Remplissage des données
    for rx_time_nsec, wn, tow_ps, leap_sec in rows:
        # on skippe tout ce qui est avant la semaine 2000
        if wn < 2000:
            continue

        # traitement normal
        tai_ts = gnss_to_tai_timestamp(wn, tow_ps, leap_sec)
        json_data["data"].append([
            tai_ts,       # 0
            "0xAA",       # 1
            "0x21",       # 2
            2,            # 3
            25,           # 4
            rx_time_nsec, # 5
            0,            # 6
            0,            # 7
            wn,           # 8
            tow_ps,       # 9
            0,            # 10
            0,            # 11
            0,            # 12
            leap_sec      # 13
        ])

    # # Écriture du JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        # 1) Pretty-print du préambule (tout sauf "data" et "parameters")
        head = {
            "mib_ref":      json_data["mib_ref"],
            "payload_name": json_data["payload_name"],
            "packet_name":  json_data["packet_name"],
            "metadata":     json_data["metadata"],
        }
        f.write(json.dumps(head, indent=2, ensure_ascii=False)[:-1])  
        # retire la dernière '}' pour ajouter parameters

        # 2) Écriture inline de chaque paramètre sur sa ligne
        f.write(',\n  "parameters": [\n')
        for i, p in enumerate(json_data["parameters"]):
            comma = "," if i < len(json_data["parameters"]) - 1 else ""
            line = f'    {{ "name": "{p["name"]}", "pos": {p["pos"]}, "metadata": {{}} }}{comma}\n'
            f.write(line)
        f.write('  ],\n')

        # 3) Pretty-print de data (compact ligne par ligne)
        f.write('  "data": [\n')
        for i, row in enumerate(json_data["data"]):
            comma = "," if i < len(json_data["data"]) - 1 else ""
            f.write(f'    {json.dumps(row, separators=(",",":"), ensure_ascii=False)}{comma}\n')
        f.write('  ]\n}\n')



if __name__ == "__main__":
    db_path     = "frames_log_08.04.2025-09.12.19.db"
    output_path = "L0_TM_GAL_TIME.json"
    try:
        convert_gnss_to_json(db_path, output_path)
    except Exception as e:
        print("Erreur :", e)
