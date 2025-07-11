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

def generate_stec_json(db_path, output_path):
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"Base introuvable : {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1) Charger GNSS_TIME
    cursor.execute("SELECT rx_time_nsec, wn, tow_ps, leap_sec FROM GNSS_TIME")
    time_map = {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}

    # 2) Charger OBSERVABLE_DATA pour iono
    cursor.execute("""
      SELECT rx_time_nsec, sat_id, const_id, iono_corr, iono_model
      FROM OBSERVABLE_DATA
    """)
    obs_map = {
        (r_ns, sat, cid): (corr, mod)
        for r_ns, sat, cid, corr, mod in cursor.fetchall()
    }

    # 3) Récupérer toutes les secondes distinctes
    cursor.execute("""
      SELECT DISTINCT rx_time_nsec
      FROM CHANNEL_TRACKING
      WHERE is_used_in_pvt = 1
      ORDER BY rx_time_nsec
    """)
    seconds = [r[0] for r in cursor.fetchall()]

    # 4) Initialiser JSON
    json_data = {
      "mib_ref":      "1.0.0",
      "payload_name": "/GENERIC/SWING/PAYLOAD/AQUILA",
      "packet_name":  "TAQUI_TM_211003STEC",
      "metadata":     {},
      "parameters": [
        { "name": "TM_packet_time_TAI",   "pos": 0, "metadata": {} },
        { "name": "DGENE_AM_SYHDSTECSB0", "pos": 1, "metadata": {} },
        { "name": "DGENE_AM_SYHDSTECSB1", "pos": 2, "metadata": {} },
        { "name": "DGENE_AM_SYHDSTECMSI", "pos": 3, "metadata": {} },
        { "name": "DGENE_AM_SYHDSTECPAS", "pos": 4, "metadata": {} },
        { "name": "PAQUI_AM_ERXTIMENS",   "pos": 5, "metadata": {} },
        { "name": "PAQUI_AM_ESIGNALID",   "pos": 6, "metadata": {} },
        { "name": "PAQUI_AM_ESATID",      "pos": 7, "metadata": {} },
        { "name": "PAQUI_AM_ESAMPLENB",   "pos": 8, "metadata": {} },
        {
          "name":      "PAQUI_CT_ZESAMPLEPKT",
          "pos":       9,
          "metadata":  {},
          "structure": [
            { "name": "PAQUI_AM_ERXTIMEMS",   "pos": 0, "metadata": {} },
            { "name": "PAQUI_AM_ETRKSTATUS",  "pos": 1, "metadata": {} },
            { "name": "PAQUI_AM_ECHNFLAGS",   "pos": 2, "metadata": {} },
            { "name": "PAQUI_AM_ELOCKTIME",   "pos": 3, "metadata": {} },
            { "name": "PAQUI_AM_ECARRNOISE",  "pos": 4, "metadata": {} },
            { "name": "PAQUI_AM_EERRIONO",    "pos": 5, "metadata": {} },
            { "name": "PAQUI_AM_ERESIDIONO",  "pos": 6, "metadata": {} },
            { "name": "PAQUI_AM_EIONOCORMDE", "pos": 7, "metadata": {} }
          ]
        }
      ],
      "data": []
    }

    # 5) Construire data : une entrée par seconde
    for rx_time_nsec in seconds:
        # a) filtre existence WN + seuil
        if rx_time_nsec not in time_map:
            continue
        wn, tow_ps, leap_sec = time_map[rx_time_nsec]
        if wn < 2000:
            continue

        # b) récupérer tous les canaux pour cette seconde
        cursor.execute("""
          SELECT sig_id, sat_id, trk_state, flag_val, locktime, cn0, const_id
          FROM CHANNEL_TRACKING
          WHERE rx_time_nsec = ? AND is_used_in_pvt = 1
          ORDER BY chan_id
        """, (rx_time_nsec,))
        rows = cursor.fetchall()
        if not rows:
            continue

        # c) timestamp TAI
        tm_tai = gnss_to_tai_timestamp(wn, tow_ps, leap_sec)

        # d) premier signal/sat pour positions 6 et 7
        first_sig, first_sat, *_ = rows[0]

        # e) construire le tableau imbriqué
        nested = []
        for sig_id, sat_id, status, flags, lock, cn0, const_id in rows:
            ms = int(rx_time_nsec // 1_000_000)
            # iono lookup
            iono_corr, iono_model = obs_map.get(
                (rx_time_nsec, sat_id, const_id),
                (0, 0)
            )
            nested.append([
                f"{ms:016X}",  # ERXTIMEMS en hex
                status,        # ETRKSTATUS
                flags,         # ECHNFLAGS
                lock,          # ELOCKTIME
                cn0,           # ECARRNOISE
                iono_corr,     # EERRIONO
                0,             # ERESIDIONO
                iono_model     # EIONOCORMDE
            ])

        # f) ajouter la ligne JSON
        json_data["data"].append([
            tm_tai,                          # 0
            "0xAA",                          # 1
            "0x21",                          # 2
            410,                             # 3
            68,                              # 4
            int(rx_time_nsec // 1_000_000),  # 5
            first_sig,                       # 6
            first_sat,                       # 7
            len(rows),                       # 8
            nested                           # 9
        ])

    conn.close()

    # 6) Écriture finale du JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        # préambule pretty-print
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
        # data ligne par ligne
        f.write('  ],\n  "data": [\n')
        for i, row in enumerate(json_data["data"]):
            comma = "," if i < len(json_data["data"]) - 1 else ""
            f.write(f'    {json.dumps(row, separators=(",",":"), ensure_ascii=False)}{comma}\n')
        f.write('  ]\n}\n')

    print(f"✅ JSON généré : {output_path} ({len(json_data['data'])} paquets)")

if __name__ == "__main__":
    db_path     = "frames_log_08.04.2025-09.12.19.db"
    output_path = "TM_L0_STEC.json"
    generate_stec_json(db_path, output_path)
