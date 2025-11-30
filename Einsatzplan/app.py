from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g
import os, uuid
from datetime import datetime
import psycopg2
import psycopg2.extras

app = Flask(__name__)
app.secret_key = "geheimes_passwort"

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# PostgreSQL / Supabase – Verbindungs-String kommt über ENV-Variable
DATABASE_URL = os.environ.get("DATABASE_URL")


# ---------------- DB-Wrapper für psycopg2 ----------------
class DBWrapper:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        cur = self.conn.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL ist nicht gesetzt (Supabase-URL fehlt).")
        conn = psycopg2.connect(
            DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        db = g._db = DBWrapper(conn)
    return db


@app.teardown_appcontext
def close_db(exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()

    # users-Tabelle (früher: user – user ist in Postgres reserviertes Wort)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            role TEXT DEFAULT 'mitarbeiter',
            vorname TEXT,
            nachname TEXT,
            email TEXT,
            handy TEXT,
            s34a TEXT,
            s34a_art TEXT,
            stelle TEXT,
            pschein TEXT,
            firma TEXT,
            stundensatz REAL
        );
        """
    )

    # event-Tabelle
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS event (
            id TEXT PRIMARY KEY,
            title TEXT,
            ort TEXT,
            dienstkleidung TEXT,
            auftraggeber TEXT,
            start TEXT,
            status TEXT,
            required_staff INTEGER DEFAULT 0,
            allowed_company TEXT
        );
        """
    )

    # response-Tabelle
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS response (
            id SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            status TEXT,
            remark TEXT,
            end_time TEXT,
            UNIQUE(event_id, username)
        );
        """
    )

    # Indizes
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_response_event ON response(event_id);"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_response_user ON response(username);"
    )

    # Spalte 'stundensatz' in event ggf. nachrüsten
    cols = db.execute(
        """
        SELECT column_name AS name
        FROM information_schema.columns
        WHERE table_name = 'event'
        """
    ).fetchall()
    colnames = [c["name"] for c in cols]
    if "stundensatz" not in colnames:
        db.execute("ALTER TABLE event ADD COLUMN stundensatz REAL;")

    # AdminTest-Nutzer einmalig anlegen
    exists = db.execute(
        "SELECT 1 FROM users WHERE username=%s",
        ("AdminTest",)
    ).fetchone()

    if not exists:
        db.execute(
            """
            INSERT INTO users
               (username,password,role,vorname,nachname,email,handy,
                s34a,s34a_art,stelle,pschein,firma,stundensatz)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                "AdminTest", "Test1234", "vorgesetzter",
                "Admin", "Test", "admin@example.com", "01500000000",
                "ja", "sachkunde", "Leitung", "ja", "HQ", None
            )
        )

    db.commit()


# init_db wird auf Render / gunicorn einmalig beim ersten Request ausgeführt
@app.before_first_request
def _init_db_once():
    init_db()


def row_to_dict(row):
    return dict(row) if row is not None else None


def to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


# ---------------- Views (Login / Dashboards) ----------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"]
        db = get_db()
        cur = db.execute("SELECT * FROM users WHERE username=%s", (username,))
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
    cur = get_db().execute(
        "SELECT * FROM users WHERE username <> %s ORDER BY nachname, vorname",
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
    if db.execute(
        "SELECT 1 FROM users WHERE username=%s",
        (username,)
    ).fetchone():
        return jsonify({"error": "Benutzername existiert schon"}), 400

    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in (None, "") else float(stundensatz)

    db.execute(
        """
        INSERT INTO users
           (username,password,role,vorname,nachname,email,handy,
            s34a,s34a_art,stelle,pschein,firma,stundensatz)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            username, d.get("password") or "", d.get("role") or "mitarbeiter",
            d.get("vorname"), d.get("nachname"), d.get("email"), d.get("handy"),
            d.get("s34a"), d.get("s34a_art"), d.get("stelle"), d.get("pschein"),
            d.get("firma"), stundensatz
        )
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>", methods=["PUT"])
def edit_user(username):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    db = get_db()

    u = db.execute(
        "SELECT * FROM users WHERE username=%s",
        (username,)
    ).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    updates = dict(u)
    for k in ["vorname", "nachname", "email", "handy", "role",
              "s34a", "s34a_art", "stelle", "pschein", "firma"]:
        if k in d:
            updates[k] = d[k]
    if "password" in d and d["password"] is not None:
        updates["password"] = d["password"]
    if "stundensatz" in d:
        updates["stundensatz"] = None if d["stundensatz"] in ("", None) else float(d["stundensatz"])

    db.execute(
        """
        UPDATE users
        SET password=%s, role=%s, vorname=%s, nachname=%s, email=%s, handy=%s,
            s34a=%s, s34a_art=%s, stelle=%s, pschein=%s, firma=%s, stundensatz=%s
        WHERE username=%s
        """,
        (
            updates["password"], updates["role"], updates["vorname"], updates["nachname"],
            updates["email"], updates["handy"], updates["s34a"], updates["s34a_art"],
            updates["stelle"], updates["pschein"], updates["firma"], updates["stundensatz"],
            username
        )
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>", methods=["DELETE"])
def delete_user(username):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM users WHERE username=%s", (username,))
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
        me = db.execute(
            "SELECT * FROM users WHERE username=%s",
            (session.get("username"),)
        ).fetchone()
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
        rcur = db.execute(
            "SELECT username,status,remark,end_time FROM response WHERE event_id=%s",
            (e["id"],)
        )
        rmap = {
            r["username"]: {
                "status": r["status"] or "",
                "remark": r["remark"] or "",
                "end_time": r["end_time"] or ""
            }
            for r in rcur.fetchall()
        }
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

    stundensatz = d.get("stundensatz")
    try:
        stundensatz = float(stundensatz) if stundensatz not in (None, "") else None
    except Exception:
        stundensatz = None

    db.execute(
        """
        INSERT INTO event (
            id,title,ort,dienstkleidung,auftraggeber,start,
            status,required_staff,allowed_company,stundensatz
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            ev_id,
            d.get("title"),
            d.get("ort"),
            d.get("dienstkleidung"),
            d.get("auftraggeber"),
            d.get("start"),
            d.get("status", "geplant"),
            to_int(d.get("required_staff", 0), 0),
            (d.get("allowed_company") or "").strip(),
            stundensatz
        )
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/<event_id>", methods=["DELETE"])
def delete_event(event_id):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM event WHERE id=%s", (event_id,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/release", methods=["POST"])
def release_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id = d.get("event_id")
    db = get_db()
    cur = db.execute(
        "UPDATE event SET status='offen' WHERE id=%s",
        (event_id,)
    )
    if cur.rowcount == 0:
        return jsonify({"error": "Event nicht gefunden"}), 404
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/respond", methods=["POST"])
def respond_event():
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id = d.get("event_id")
    response_val = d.get("response")
    remark = d.get("remark", "")
    db = get_db()

    ev = db.execute(
        "SELECT * FROM event WHERE id=%s",
        (event_id,)
    ).fetchone()
    if not ev:
        return jsonify({"error": "Event nicht gefunden"}), 404

    me = db.execute(
        "SELECT * FROM users WHERE username=%s",
        (session["username"],)
    ).fetchone()
    if not me:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    allowed = (ev["allowed_company"] or "").strip()
    if allowed and allowed != (me["firma"] or "").strip():
        return jsonify({"error": "Dieser Einsatz ist nicht für Ihre Firma freigegeben"}), 403

    if db.execute(
        "SELECT 1 FROM response WHERE event_id=%s AND username=%s",
        (event_id, me["username"])
    ).fetchone():
        db.execute(
            "UPDATE response SET status=%s, remark=%s WHERE event_id=%s AND username=%s",
            (response_val, remark, event_id, me["username"])
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark) VALUES (%s,%s,%s,%s)",
            (event_id, me["username"], response_val, remark)
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/confirm", methods=["POST"])
def confirm_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username, decision = d.get("event_id"), d.get("username"), d.get("decision")
    db = get_db()
    if db.execute(
        "SELECT 1 FROM response WHERE event_id=%s AND username=%s",
        (event_id, username)
    ).fetchone():
        db.execute(
            "UPDATE response SET status=%s WHERE event_id=%s AND username=%s",
            (decision, event_id, username)
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status) VALUES (%s,%s,%s)",
            (event_id, username, decision)
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/endtime", methods=["POST"])
def set_endtime():
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, end_time = d.get("event_id"), d.get("end_time")
    db = get_db()

    r = db.execute(
        "SELECT end_time FROM response WHERE event_id=%s AND username=%s",
        (event_id, session["username"])
    ).fetchone()
    if r and r["end_time"]:
        return jsonify({"error": "Endzeit bereits gespeichert"}), 400

    if r:
        db.execute(
            "UPDATE response SET end_time=%s WHERE event_id=%s AND username=%s",
            (end_time, event_id, session["username"])
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, end_time) VALUES (%s,%s,%s)",
            (event_id, session["username"], end_time)
        )
    db.commit()
    return jsonify({"success": True})


@app.route("/events/edit_entry", methods=["POST"])
def edit_entry():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username = d.get("event_id"), d.get("username")
    start, end_time, remark = d.get("start"), d.get("end_time"), d.get("remark", "")
    stundensatz = d.get("stundensatz")
    db = get_db()

    # Event-Daten updaten
    if start:
        db.execute("UPDATE event SET start=%s WHERE id=%s", (start, event_id))

    if stundensatz is not None:
        if stundensatz in ("",):
            new_rate = None
        else:
            try:
                new_rate = float(stundensatz)
            except Exception:
                new_rate = None
        db.execute("UPDATE event SET stundensatz=%s WHERE id=%s", (new_rate, event_id))

    # Response / Einsatz-Eintrag updaten
    if db.execute(
        "SELECT 1 FROM response WHERE event_id=%s AND username=%s",
        (event_id, username)
    ).fetchone():
        db.execute(
            """
            UPDATE response
            SET end_time=COALESCE(%s, end_time), remark=%s
            WHERE event_id=%s AND username=%s
            """,
            (end_time, remark, event_id, username)
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, end_time, remark) VALUES (%s,%s,%s,%s)",
            (event_id, username, end_time, remark)
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/remove_user", methods=["POST"])
def remove_user_from_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username = d.get("event_id"), d.get("username")
    db = get_db()
    cur = db.execute(
        "DELETE FROM response WHERE event_id=%s AND username=%s",
        (event_id, username)
    )
    if cur.rowcount == 0:
        return jsonify({"error": "Benutzer hat keine Zuordnung zu diesem Event"}), 404
    db.commit()
    return jsonify({"status": "ok", "message": f"{username} wurde aus dem Einsatz entfernt"})


@app.route("/events/assign_user", methods=["POST"])
def assign_user_to_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id, username = d.get("event_id"), d.get("username")
    db = get_db()

    ev = db.execute(
        "SELECT required_staff, allowed_company FROM event WHERE id=%s",
        (event_id,)
    ).fetchone()
    if not ev:
        return jsonify({"error": "Event nicht gefunden"}), 404

    u = db.execute(
        "SELECT firma FROM users WHERE username=%s",
        (username,)
    ).fetchone()
    if not u:
        return jsonify({"error": "Benutzer existiert nicht"}), 404

    allowed = (ev["allowed_company"] or "").strip()
    if allowed and allowed != (u["firma"] or "").strip():
        return jsonify({"error": "Benutzer gehört nicht zur freigegebenen Firma"}), 400

    required = ev["required_staff"] or 0
    confirmed = db.execute(
        "SELECT COUNT(*) AS c FROM response WHERE event_id=%s AND status='bestätigt'",
        (event_id,)
    ).fetchone()["c"]
    r = db.execute(
        "SELECT status FROM response WHERE event_id=%s AND username=%s",
        (event_id, username)
    ).fetchone()
    already_ok = r and r["status"] == "bestätigt"
    if not already_ok and required > 0 and confirmed >= required:
        return jsonify({"error": "Benötigte Anzahl bereits erreicht"}), 400

    if r:
        db.execute(
            "UPDATE response SET status='bestätigt' WHERE event_id=%s AND username=%s",
            (event_id, username)
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status) VALUES (%s,%s,'bestätigt')",
            (event_id, username)
        )
    db.commit()
    return jsonify({"status": "ok", "message": f"{username} wurde dem Einsatz bestätigt zugewiesen"})


# ---------------- Report ----------------
@app.route("/events/report", methods=["GET"])
def report_events():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

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

    if role in ["chef", "vorgesetzter"]:
        result = {}
        cur = db.execute("SELECT * FROM event")
        for e in cur.fetchall():
            if not month_ok(e["start"] or ""):
                continue
            rcur = db.execute(
                """
                SELECT * FROM response
                WHERE event_id=%s AND status='bestätigt' AND end_time IS NOT NULL
                """,
                (e["id"],)
            )
            for r in rcur.fetchall():
                try:
                    s = datetime.fromisoformat(e["start"])
                    eh, em = map(int, (r["end_time"] or "0:0").split(":"))
                    end = s.replace(hour=eh, minute=em)
                    hours = max((end - s).total_seconds() / 3600, 0)
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
            r = db.execute(
                """
                SELECT * FROM response
                WHERE event_id=%s AND username=%s
                  AND status='bestätigt' AND end_time IS NOT NULL
                """,
                (e["id"], me)
            ).fetchone()
            if not r:
                continue
            try:
                s = datetime.fromisoformat(e["start"])
                eh, em = map(int, (r["end_time"] or "0:0").split(":"))
                end = s.replace(hour=eh, minute=em)
                hours = max((end - s).total_seconds() / 3600, 0)
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
    os.makedirs(BASE_DIR, exist_ok=True)
    with app.app_context():
        init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
