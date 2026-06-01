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
          <th>GAP</th>
          <th>PITS</th>
        </tr>
      </thead>
      <tbody>
        {drivers.map((d) => (
          <tr key={d.driver_number}>
            <td>{d.position}</td>
            <td>
              <span
                style={{
                  borderLeft: `4px solid #${d.team_colour}`,
                  paddingLeft: "8px",
                }}
              >
                {d.name_acronym} — {d.full_name}
              </span>
            </td>
            <td>{d.team_name}</td>
            <td>{d.lap_number ?? "—"}</td>
            <td>{d.last_lap ? d.last_lap.toFixed(3) + "s" : "—"}</td>
            <td>
              {d.gap_to_leader != null
                ? d.gap_to_leader === 0
                  ? "LEADER"
                  : `+${d.gap_to_leader}s`
                : "—"}
            </td>
            <td>{d.pit_stops ?? "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);
}

export default App;
