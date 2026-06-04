import { useEffect, useState } from "react";
import "./App.css";

function App() {
  const [drivers, setDrivers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [session, setSession] = useState(null)

  const fetchData = async () => {
    const res = await fetch("http://localhost:8000/f1/live");
    const data = await res.json();
    setDrivers(data.drivers);
    setSession(data.session);
    setLoading(false);
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

    {session && (
      <div className="session-info">
        <span>
          {session.year} {session.circuit}
        </span>
        <span>{session.name}</span>
        <span>{session.country}</span>
      </div>
    )}

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
  </div>
);
}

export default App;

