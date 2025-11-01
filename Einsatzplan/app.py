from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g
import sqlite3, os, uuid
from datetime import datetime

app = Flask(__name__)
# In Produktion besser über ENV setzen
app.secret_key = os.environ.get("SECRET_KEY", "geheimes_passwort")

# --- DB-Pfad: lokal = Projektordner, auf Render mit Disk = /var/data ---
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_DIR = os.environ.get("DATABASE_DIR", BASE_DIR)   # auf Render per Disk: /var/data
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "app.db")

# ---------------- SQLite (ohne externe Pakete) ----------------
def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        db = g._db = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10.0)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON;")
        db.execute("PRAGMA journal_mode = WAL;")
        db.execute("PRAGMA synchronous = NORMAL;")
    return db

@app.teardown_appcontext
def close_db(exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()

def init_db():
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS user (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            role TEXT DEFAULT 'mitarbeiter',
            vorname TEXT, nachname TEXT, email TEXT, handy TEXT,
            s34a TEXT, s34a_art TEXT, stelle TEXT, pschein TEXT,
            firma TEXT, stundensatz REAL
        );

        CREATE TABLE IF NOT EXISTS event (
            id TEXT PRIMARY KEY,
            title TEXT, ort TEXT, dienstkleidung TEXT, auftraggeber TEXT,
            start TEXT,
            status TEXT,        -- 'geplant' | 'offen'
            required_staff INTEGER DEFAULT 0,
            allowed_company TEXT
        );

        CREATE TABLE IF NOT EXISTS response (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            username TEXT NOT NULL REFERENCES user(username) ON DELETE CASCADE,
            status TEXT,        -- 'zugesagt' | 'bestätigt' | 'abgelehnt'
            remark TEXT,
            end_time TEXT,      -- 'HH:MM'
            UNIQUE(event_id, username)
        );

        CREATE INDEX IF NOT EXISTS idx_response_event ON response(event_id);
        CREATE INDEX IF NOT EXISTS idx_response_user  ON response(username);
        """
    )
    db.commit()

    # --------- Nur AdminTest anlegen (wird nicht in Mitarbeiterliste angezeigt) ---------
    exists = db.execute("SELECT 1 FROM user WHERE username=?", ("AdminTest",)).fetchone()
    if not exists:
        db.execute(
            """INSERT INTO user
               (username,password,role,vorname,nachname,email,handy,s34a,s34a_art,stelle,pschein,firma,stundensatz)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("AdminTest", "Test1234", "vorgesetzter",
             "Admin", "Test", "admin@example.com", "01500000000",
             "ja", "sachkunde", "Leitung", "ja", "HQ", None)
        )
        db.commit()

def row_to_dict(row):
    return {k: row[k] for k in row.keys()}

def to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default

# ---------------- Healthcheck ----------------
@app.route("/health")
def health():
    return "ok", 200

# ---------------- Views (Login / Dashboards) ----------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"]
        db = get_db()
        cur = db.execute("SELECT * FROM user WHERE username=?", (username,))
        u = cur.fetchone()
        if u and u["password"] == password:
            session["username"] = username
            session["role"] = u["role"] or "mitarbeiter"
            return redirect(url_for("dashboard"))
        return render_template("login.html", error="Login fehlgeschlagen")
    return render_template("login.html")

@app.route("/dashboard")
def dashboard():
    if "username" not in session:
        return redirect(url_for("login"))
    if session.get("role") in ["chef", "vorgesetzter"]:
        return render_template("dashboard_chef.html", user=session["username"])
    else:
        return render_template("dashboard_mitarbeiter.html", user=session["username"])

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ---------------- Users API ----------------
@app.route("/users", methods=["GET"])
def get_users():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    # AdminTest NICHT anzeigen
    cur = get_db().execute(
        "SELECT * FROM user WHERE username <> ? ORDER BY nachname, vorname",
        ("AdminTest",)
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    for u in users:
        if u["stundensatz"] is None:
            u["stundensatz"] = ""
    return jsonify(users)

@app.route("/users", methods=["POST"])
def add_user():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    username = (d.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username ist erforderlich"}), 400

    db = get_db()
    if db.execute("SELECT 1 FROM user WHERE username=?", (username,)).fetchone():
        return jsonify({"error": "Benutzername existiert schon"}), 400

    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in (None, "") else float(stundensatz)

    db.execute(
        """INSERT INTO user
           (username,password,role,vorname,nachname,email,handy,s34a,s34a_art,stelle,pschein,firma,stundensatz)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (username, d.get("password") or "", d.get("role") or "mitarbeiter",
         d.get("vorname"), d.get("nachname"), d.get("email"), d.get("handy"),
         d.get("s34a"), d.get("s34a_art"), d.get("stelle"), d.get("pschein"),
         d.get("firma"), stundensatz)
    )
    db.commit()
    return jsonify({"status": "ok"})

@app.route("/users/<username>", methods=["PUT"])
def edit_user(username):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    db = get_db()

    u = db.execute("SELECT * FROM user WHERE username=?", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    updates = dict(u)
    for k in ["vorname","nachname","email","handy","role","s34a","s34a_art","stelle","pschein","firma"]:
        if k in d: updates[k] = d[k]
    if "password" in d and d["password"] is not None:
        updates["password"] = d["password"]
    if "stundensatz" in d:
        updates["stundensatz"] = None if d["stundensatz"] in ("", None) else float(d["stundensatz"])

    db.execute(
        """UPDATE user SET password=?, role=?, vorname=?, nachname=?, email=?, handy=?,
           s34a=?, s34a_art=?, stelle=?, pschein=?, firma=?, stundensatz=? WHERE username=?""",
        (updates["password"], updates["role"], updates["vorname"], updates["nachname"],
         updates["email"], updates["handy"], updates["s34a"], updates["s34a_art"],
         updates["stelle"], updates["pschein"], updates["firma"], updates["stundensatz"], username)
    )
    db.commit()
    return jsonify({"status": "ok"})

@app.route("/users/<username>", methods=["DELETE"])
def delete_user(username):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM user WHERE username=?", (username,))
    db.commit()
    return jsonify({"status": "ok"})

# ---------------- Events API ----------------
@app.route("/events", methods=["GET"])
def events_list():
    db = get_db()
    role = session.get("role")

    if role in ["chef", "vorgesetzter"]:
        cur = db.execute("SELECT * FROM event")
        events = [row_to_dict(e) for e in cur.fetchall()]
    else:
        me = db.execute("SELECT * FROM user WHERE username=?", (session.get("username"),)).fetchone()
        if not me:
            return jsonify([])
        my_company = (me["firma"] or "").strip()
        cur = db.execute("SELECT * FROM event")
        events = []
        for e in cur.fetchall():
            allowed = (e["allowed_company"] or "").strip()
            if not allowed or allowed == my_company:
                events.append(row_to_dict(e))

    result = []
    for e in events:
        rcur = db.execute("SELECT username,status,remark,end_time FROM response WHERE event_id=?", (e["id"],))
        rmap = {r["username"]: {"status": r["status"] or "", "remark": r["remark"] or "", "end_time": r["end_time"] or ""} for r in rcur.fetchall()}
        e["responses"] = rmap
        result.append(e)
    return jsonify(result)

@app.route("/events", methods=["POST"])
def add_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    ev_id = str(uuid.uuid4())
    db = get_db()
    db.execute(
        """INSERT INTO event (id,title,ort,dienstkleidung,auftraggeber,start,status,required_staff,allowed_company)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (ev_id, d.get("title"), d.get("ort"), d.get("dienstkleidung"), d.get("auftraggeber"),
         d.get("start"), d.get("status","geplant"), to_int(d.get("required_staff",0),0),
         (d.get("allowed_company") or "").strip())
    )
    db.commit()
    return jsonify({"status":"ok"})

@app.route("/events/<event_id>", methods=["DELETE"])
def delete_event(event_id):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error":"Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM event WHERE id=?", (event_id,))
    db.commit()
    return jsonify({"status":"ok"})

@app.route("/events/release", methods=["POST"])
def release_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error":"Nicht erlaubt"}), 403
    d = request.json or {}
    event_id = d.get("event_id")
    db = get_db()
    cur = db.execute("UPDATE event SET status='offen' WHERE id=?", (event_id,))
    if cur.rowcount == 0:
        return jsonify({"error":"Event nicht gefunden"}), 404
    db.commit()
    return jsonify({"status":"ok"})

@app.route("/events/respond", methods=["POST"])
def respond_event():
    if session.get("role") != "mitarbeiter":
        return jsonify({"error":"Nicht erlaubt"}), 403
    d = request.json or {}
    event_id = d.get("event_id")
    response_val = d.get("response")
    remark = d.get("remark","")
    db = get_db()

    ev = db.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone()
    if not ev:
        return jsonify({"error":"Event nicht gefunden"}), 404

    me = db.execute("SELECT * FROM user WHERE username=?", (session["username"],)).fetchone()
    if not me:
        return jsonify({"error":"Nicht eingeloggt"}), 403

    allowed = (ev["allowed_company"] or "").strip()
    if allowed and allowed != (me["firma"] or "").strip():
        return jsonify({"error":"Dieser Einsatz ist nicht für Ihre Firma freigegeben"}), 403

    if db.execute("SELECT 1 FROM response WHERE event_id=? AND username=?", (event_id, me["username"])).fetchone():
        db.execute("UPDATE response SET status=?, remark=? WHERE event_id=? AND username=?",
                   (response_val, remark, event_id, me["username"]))
    else:
        db.execute("INSERT INTO response (event_id, username, status, remark) VALUES (?,?,?,?)",
                   (event_id, me["username"], response_val, remark))
    db.commit()
    return jsonify({"status":"ok"})

@app.route("/events/confirm", methods=["POST"])
def confirm_event():
    if session.get("role") not in ["chef","vorgesetzter"]:
        return jsonify({"error":"Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username, decision = d.get("event_id"), d.get("username"), d.get("decision")
    db = get_db()
    if db.execute("SELECT 1 FROM response WHERE event_id=? AND username=?", (event_id, username)).fetchone():
        db.execute("UPDATE response SET status=? WHERE event_id=? AND username=?", (decision, event_id, username))
    else:
        db.execute("INSERT INTO response (event_id, username, status) VALUES (?,?,?)", (event_id, username, decision))
    db.commit()
    return jsonify({"status":"ok"})

@app.route("/events/endtime", methods=["POST"])
def set_endtime():
    if session.get("role") != "mitarbeiter":
        return jsonify({"error":"Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, end_time = d.get("event_id"), d.get("end_time")
    db = get_db()

    r = db.execute("SELECT end_time FROM response WHERE event_id=? AND username=?",
                   (event_id, session["username"])).fetchone()
    if r and r["end_time"]:
        return jsonify({"error":"Endzeit bereits gespeichert"}), 400

    if r:
        db.execute("UPDATE response SET end_time=? WHERE event_id=? AND username=?",
                   (end_time, event_id, session["username"]))
    else:
        db.execute("INSERT INTO response (event_id, username, end_time) VALUES (?,?,?)",
                   (event_id, session["username"], end_time))
    db.commit()
    return jsonify({"success": True})

@app.route("/events/edit_entry", methods=["POST"])
def edit_entry():
    if session.get("role") not in ["chef","vorgesetzter"]:
        return jsonify({"error":"Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username = d.get("event_id"), d.get("username")
    start, end_time, remark = d.get("start"), d.get("end_time"), d.get("remark","")
    db = get_db()

    if start:
        db.execute("UPDATE event SET start=? WHERE id=?", (start, event_id))

    if db.execute("SELECT 1 FROM response WHERE event_id=? AND username=?", (event_id, username)).fetchone():
        db.execute("UPDATE response SET end_time=COALESCE(?, end_time), remark=? WHERE event_id=? AND username=?",
                   (end_time, remark, event_id, username))
    else:
        db.execute("INSERT INTO response (event_id, username, end_time, remark) VALUES (?,?,?,?)",
                   (event_id, username, end_time, remark))
    db.commit()
    return jsonify({"status":"ok"})

@app.route("/events/remove_user", methods=["POST"])
def remove_user_from_event():
    if session.get("role") not in ["chef","vorgesetzter"]:
        return jsonify({"error":"Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username = d.get("event_id"), d.get("username")
    db = get_db()
    cur = db.execute("DELETE FROM response WHERE event_id=? AND username=?", (event_id, username))
    if cur.rowcount == 0:
        return jsonify({"error":"Benutzer hat keine Zuordnung zu diesem Event"}), 404
    db.commit()
    return jsonify({"status":"ok", "message": f"{username} wurde aus dem Einsatz entfernt"})

@app.route("/events/assign_user", methods=["POST"])
def assign_user_to_event():
    if session.get("role") not in ["chef","vorgesetzter"]:
        return jsonify({"error":"Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username = d.get("event_id"), d.get("username")
    db = get_db()

    ev = db.execute("SELECT required_staff, allowed_company FROM event WHERE id=?", (event_id,)).fetchone()
    if not ev:
        return jsonify({"error":"Event nicht gefunden"}), 404

    u = db.execute("SELECT firma FROM user WHERE username=?", (username,)).fetchone()
    if not u:
        return jsonify({"error":"Benutzer existiert nicht"}), 404

    allowed = (ev["allowed_company"] or "").strip()
    if allowed and allowed != (u["firma"] or "").strip():
        return jsonify({"error":"Benutzer gehört nicht zur freigegebenen Firma"}), 400

    required = ev["required_staff"] or 0
    confirmed = db.execute("SELECT COUNT(*) c FROM response WHERE event_id=? AND status='bestätigt'", (event_id,)).fetchone()["c"]
    r = db.execute("SELECT status FROM response WHERE event_id=? AND username=?", (event_id, username)).fetchone()
    already_ok = r and r["status"] == "bestätigt"
    if not already_ok and required > 0 and confirmed >= required:
        return jsonify({"error":"Benötigte Anzahl bereits erreicht"}), 400

    if r:
        db.execute("UPDATE response SET status='bestätigt' WHERE event_id=? AND username=?", (event_id, username))
    else:
        db.execute("INSERT INTO response (event_id, username, status) VALUES (?,?, 'bestätigt')", (event_id, username))
    db.commit()
    return jsonify({"status":"ok", "message": f"{username} wurde dem Einsatz bestätigt zugewiesen"})

# ---------------- Report ----------------
@app.route("/events/report", methods=["GET"])
def report_events():
    if "username" not in session:
        return jsonify({"error":"Nicht eingeloggt"}), 403

    role = session.get("role", "mitarbeiter")
    month = request.args.get("month")
    db = get_db()

    def month_ok(start_str):
        if not month:
            return True
        try:
            s = datetime.fromisoformat(start_str)
            ym = datetime.strptime(month, "%Y-%m")
            return s.year == ym.year and s.month == ym.month
        except Exception:
            return False

    if role in ["chef","vorgesetzter"]:
        result = {}
        cur = db.execute("SELECT * FROM event")
        for e in cur.fetchall():
            if not month_ok(e["start"] or ""):
                continue
            rcur = db.execute("SELECT * FROM response WHERE event_id=? AND status='bestätigt' AND end_time IS NOT NULL", (e["id"],))
            for r in rcur.fetchall():
                try:
                    s = datetime.fromisoformat(e["start"])
                    eh, em = map(int, (r["end_time"] or "0:0").split(":"))
                    end = s.replace(hour=eh, minute=em)
                    hours = max((end - s).total_seconds()/3600, 0)
                except Exception:
                    continue
                pack = result.setdefault(r["username"], {"total": 0.0, "entries": []})
                pack["total"] += hours
                pack["entries"].append({
                    "date": s.strftime("%d.%m.%Y"),
                    "title": e["title"] or "",
                    "start": s.strftime("%H:%M"),
                    "end": r["end_time"],
                    "hours": round(hours, 2)
                })
        return jsonify(result)
    else:
        me = session["username"]
        total = 0.0
        entries = []
        ecur = db.execute("SELECT * FROM event")
        for e in ecur.fetchall():
            if not month_ok(e["start"] or ""):
                continue
            r = db.execute("SELECT * FROM response WHERE event_id=? AND username=? AND status='bestätigt' AND end_time IS NOT NULL",
                           (e["id"], me)).fetchone()
            if not r: 
                continue
            try:
                s = datetime.fromisoformat(e["start"])
                eh, em = map(int, (r["end_time"] or "0:0").split(":"))
                end = s.replace(hour=eh, minute=em)
                hours = max((end - s).total_seconds()/3600, 0)
            except Exception:
                continue
            total += hours
            entries.append({
                "date": s.strftime("%d.%m.%Y"),
                "title": e["title"] or "",
                "start": s.strftime("%H:%M"),
                "end": r["end_time"],
                "hours": round(hours, 2)
            })
        return jsonify({"total": round(total, 2), "entries": entries})

# ---------------- Start ----------------
if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
