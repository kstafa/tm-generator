import sqlite3
import json
import os
from datetime import datetime, timezone, timedelta

def gnss_to_tai_timestamp(wn, tow_ps, leap_sec):
    gps_epoch = datetime(1980, 1, 6, tzinfo=timezone.utc)
    tow_seconds = tow_ps / 1e12
    gps_time = gps_epoch + timedelta(weeks=wn, seconds=tow_seconds)
    tai_time = gps_time + timedelta(seconds=19 + leap_sec)
    return tai_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def convert_pv_to_json(db_path, output_path):
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"Base SQLite introuvable : {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Vérifier que les tables existent
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {row[0] for row in cursor.fetchall()}
    required = {"GNSS_TIME", "BEST_POS_ECEF"}
    if not required.issubset(tables):
        raise RuntimeError(f"Tables manquantes : {required - tables}")

    # Requête pour joindre GNSS_TIME et BEST_POS_ECEF
    cursor.execute("""
        SELECT 
          g.wn, g.tow_ps, g.leap_sec, 
          e.x, e.y, e.z
        FROM GNSS_TIME AS g
        JOIN BEST_POS_ECEF AS e
          ON g.rx_time_nsec = e.rx_time_nsec
        ORDER BY g.rx_time_nsec
    """)
    rows = cursor.fetchall()
    conn.close()

    # Structure JSON
    json_data = {
        "mib_ref":      "1.0.0",
        "payload_name": "/GENERIC/SWING/PAYLOAD/AQUILA",
        "packet_name":  "MAQUI_TM_003025PV",
        "metadata":     {},
        "parameters": [
            { "name": "TM_packet_time_TAI",  "pos": 0,  "metadata": {} },
            { "name": "SYNC_PATTERN_1",      "pos": 1,  "metadata": {} },
            { "name": "SYNC_PATTERN_2",      "pos": 2,  "metadata": {} },
            { "name": "PACKET_ID",           "pos": 3,  "metadata": {} },
            { "name": "PACKET_LENGTH",       "pos": 4,  "metadata": {} },
            { "name": "MAQUI_AM_PVRXTIMENS", "pos": 5,  "metadata": {} },
            { "name": "MAQUI_AM_PVFLAGS",    "pos": 6,  "metadata": {} },
            { "name": "MAQUI_AM_PVDATUM",    "pos": 7,  "metadata": {} },
            { "name": "MAQUI_AM_PVXCOORD",   "pos": 8,  "metadata": {} },
            { "name": "MAQUI_AM_PVYCOORD",   "pos": 9,  "metadata": {} },
            { "name": "MAQUI_AM_PVZCOORD",   "pos": 10, "metadata": {} },
            { "name": "MAQUI_AM_PVXSTDCOORD","pos": 11, "metadata": {} },
            { "name": "MAQUI_AM_PVYSTDCOORD","pos": 12, "metadata": {} },
            { "name": "MAQUI_AM_PVZSTDCOORD","pos": 13, "metadata": {} },
            { "name": "MAQUI_AM_PVXVEL",     "pos": 14, "metadata": {} },
            { "name": "MAQUI_AM_PVYVEL",     "pos": 15, "metadata": {} },
            { "name": "MAQUI_AM_PVZVEL",     "pos": 16, "metadata": {} },
            { "name": "MAQUI_AM_PVXSTDVEL",  "pos": 17, "metadata": {} },
            { "name": "MAQUI_AM_PVYSTDVEL",  "pos": 18, "metadata": {} },
            { "name": "MAQUI_AM_PVZSTDVEL",  "pos": 19, "metadata": {} }
        ],
        "data": []
    }

    for wn, tow_ps, leap_sec, x, y, z in rows:
        # Filtrer si nécessaire (ex. wn < 2000)
        if wn < 2000: continue

        tai_ts = gnss_to_tai_timestamp(wn, tow_ps, leap_sec)

        # Champs fixes : SYNC_PATTERN_1 = "0xAA", SYNC_PATTERN_2 = "0x21",
        # PACKET_ID = 407, PACKET_LENGTH = 46, PVDATUM = 0
        json_data["data"].append([
            tai_ts,    # 0
            "0xAA",    # 1
            "0x21",    # 2
            407,       # 3
            46,        # 4
            tow_ps,    # 5 ⟵ ici on pourrait remplacer par timestamp interne si besoin
            0,         # 6 PVDATUM
            0,         # 7 reservado
            x,         # 8 PVXCOORD
            y,         # 9 PVYCOORD
            z,         # 10 PVZCOORD
            5, 5, 5,   # 11-13 STDs coord (exemple fixe)
            0.0,0.0,0.0,  # 14-16 velocity (exemple fixe)
            2,2,2      # 17-19 STD vel (exemple fixe)
        ])

    # Écriture du JSON avec paramètres sur ligne unique
    with open(output_path, 'w', encoding='utf-8') as f:
        # préambule
        head = {
            "mib_ref":      json_data["mib_ref"],
            "payload_name": json_data["payload_name"],
            "packet_name":  json_data["packet_name"],
            "metadata":     json_data["metadata"],
        }
        f.write(json.dumps(head, indent=2, ensure_ascii=False)[:-1])
        # parameters inline
        f.write(',\n  "parameters": [\n')
        for i, p in enumerate(json_data["parameters"]):
            comma = "," if i < len(json_data["parameters"]) - 1 else ""
            f.write(f'    {{ "name": "{p["name"]}", "pos": {p["pos"]}, "metadata": {{}} }}{comma}\n')
        f.write('  ],\n')

        # data ligne par ligne
        f.write('  "data": [\n')
        for i, row in enumerate(json_data["data"]):
            comma = "," if i < len(json_data["data"]) - 1 else ""
            f.write(f'    {json.dumps(row, separators=(",",":"), ensure_ascii=False)}{comma}\n')
        f.write('  ]\n}\n')

if __name__ == "__main__":
    db_path     = "frames_log_08.04.2025-09.12.19.db"
    output_path = "L0_TM_PV.json"
    convert_pv_to_json(db_path, output_path)
