import { useEffect, useState } from "react";
import "./App.css";

function App() {
  const [drivers, setDrivers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [session, setSession] = useState(null)
  const [view, setView] = useState("live");
  const [ergastDrivers, setErgastDrivers] = useState([]);
  const [lapAnalysis, setLapAnalysis] = useState([]);
  const [constructorStandings, setConstructorStandings] = useState([]);


const fetchData = async () => {
  try {
    const res = await fetch("http://localhost:8001/f1/live");
    if (!res.ok) return;
    const data = await res.json();
    setDrivers(data.drivers);
    setSession(data.session);
    setLoading(false);
  } catch (e) {
    console.error("Fetch failed:", e);
  }
};
const fetchErgastDrivers = async () => {
  const res = await fetch("http://localhost:8001/ergast/drivers");
  const data = await res.json();
  setErgastDrivers(data.drivers);
};

const fetchLapAnalysis = async () => {
  const res = await fetch("http://localhost:8002/ergast/lap-analysis");
  const data = await res.json();
  setLapAnalysis(data.results);
};

const fetchConstructorStandings = async () => {
  const res = await fetch("http://localhost:8002/ergast/constructor-standings");
  const data = await res.json();
  setConstructorStandings(data.results);
};


  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  if (loading) return <p>Loading...</p>;

return (
  <div className="timing-board">
    <h1>F1 Live Timing</h1>
    <span className="live-badge">⬤ LIVE</span>

    <button
      onClick={() => {
        setView(view === "live" ? "drivers" : "live");
        fetchErgastDrivers();
      }}
    >
      Drivers
    </button>

    <button
      onClick={() => {
        setView("lap-analysis");
        fetchLapAnalysis();
      }}
    >
      Lap Analysis
    </button>

    <button
      onClick={() => {
        setView("constructor-standings");
        fetchConstructorStandings();
      }}
    >
      Constructor Standings
    </button>

    {session && (
      <div className="session-info">
        <span>
          {session.year} {session.circuit}
        </span>
        <span>{session.name}</span>
        <span>{session.country}</span>
      </div>
    )}

    {view === "live" && (
      <table>
        <thead>
          <tr>
            <th>POS</th>
            <th>Driver</th>
            <th>Team</th>
            <th>LAP</th>
            <th>Last Lap</th>
            <th>BEST LAP</th>
            <th>GAP</th>
            <th>PITS</th>
            <th>TYRE</th>
          </tr>
        </thead>
        <tbody>
          {drivers.map((d) => (
            <tr key={d.driver_number}>
              <td>
                <span className={`pos pos-${d.position}`}>{d.position}</span>
              </td>
              <td>
                <div className="driver-cell">
                  <span
                    className="team-bar"
                    style={{ backgroundColor: `#${d.team_colour}` }}
                  />
                  <img src={d.headshot_url} alt={d.name_acronym} />
                  <div className="driver-name">
                    <span className="acronym">{d.name_acronym}</span>
                    <span className="full-name">{d.full_name}</span>
                  </div>
                </div>
              </td>
              <td>{d.team_name}</td>
              <td>{d.lap_number ?? "—"}</td>
              <td>{d.last_lap ? d.last_lap.toFixed(3) + "s" : "—"}</td>
              <td style={{ color: d.best_lap ? "#00d2be" : "inherit" }}>
                {d.best_lap ? d.best_lap.toFixed(3) + "s" : "—"}
              </td>
              <td>
                {d.gap_to_leader != null
                  ? d.gap_to_leader === 0
                    ? "LEADER"
                    : `+${d.gap_to_leader}s`
                  : "—"}
              </td>
              <td>{d.pit_stops ?? "—"}</td>
              <td>
                <span
                  style={{
                    backgroundColor:
                      d.compound === "SOFT"
                        ? "#e10600"
                        : d.compound === "MEDIUM"
                          ? "#ffd700"
                          : d.compound === "HARD"
                            ? "#fff"
                            : "#444",
                    color:
                      d.compound === "HARD" || d.compound === "MEDIUM"
                        ? "#111"
                        : "#fff",
                    padding: "2px 8px",
                    borderRadius: "4px",
                    fontSize: "0.75rem",
                    fontWeight: "bold",
                    letterSpacing: "1px",
                  }}
                >
                  {d.compound ?? "—"}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    )}
    {view === "drivers" && (
      <table>
        <thead>
          <tr>
            <th>Code</th>
            <th>Name</th>
            <th>Nationality</th>
            <th>DOB</th>
          </tr>
        </thead>
        <tbody>
          {ergastDrivers.map((d) => (
            <tr key={d.driver_id}>
              <td>{d.code ?? "—"}</td>
              <td>
                {d.forename} {d.surname}
              </td>
              <td>{d.nationality}</td>
              <td>{d.dob}</td>
            </tr>
          ))}
        </tbody>
      </table>
    )}
    {view === "lap-analysis" && (
      <table>
        <thead>
          <tr>
            <th>Race ID</th>
            <th>Driver ID</th>
            <th>Fastest Lap (ms)</th>
            <th>Avg Lap (ms)</th>
          </tr>
        </thead>
        <tbody>
          {lapAnalysis.map((r) => (
            <tr key={`${r.race_id}-${r.driver_id}`}>
              <td>{r.race_id}</td>
              <td>{r.driver_id}</td>
              <td>{r.fastest_lap_ms}</td>
              <td>{r.avg_lap_ms}</td>
            </tr>
          ))}
        </tbody>
      </table>
    )}

    {view === "constructor-standings" && (
      <table>
        <thead>
          <tr>
            <th>Team</th>
            <th>Total Points</th>
            <th>Total Wins</th>
          </tr>
        </thead>
        <tbody>
          {constructorStandings.map((r) => (
            <tr key={r.constructor_id}>
              <td>{r.name}</td>
              <td>{r.total_points}</td>
              <td>{r.total_wins}</td>
            </tr>
          ))}
        </tbody>
      </table>
    )}
  </div>
);
}
export default App;

